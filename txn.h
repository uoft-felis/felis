#ifndef TXN_H
#define TXN_H

#include <cstdlib>
#include <cstdint>
#include <array>
#include <initializer_list>

#include "epoch.h"
#include "util.h"
#include "varstr.h"
#include "promise.h"
#include "slice.h"
#include "vhandle_batchappender.h"
#include "opts.h"

namespace felis {

class VHandle;
class Relation;
class EpochClient;

class BaseTxn {
 protected:
  friend class EpochClient;

  Epoch *epoch;
  uint64_t sid;

  using BrkType = std::array<mem::Brk *, NodeConfiguration::kMaxNrThreads / mem::kNrCorePerNode>;
  static BrkType g_brk;
  static int g_cur_numa_node;

 public:
  BaseTxn(uint64_t serial_id)
      : epoch(nullptr), sid(serial_id) {}

  static void *operator new(size_t nr_bytes) { return g_brk[g_cur_numa_node]->Alloc(nr_bytes); }
  static void operator delete(void *ptr) {}
  static void InitBrk(long nr_epochs);

  virtual void PrepareState() {}

  virtual ~BaseTxn() {}
  virtual void Prepare() = 0;
  virtual void PrepareInsert() = 0;
  virtual void Run() = 0;

  void RunAndAssignSchedulingKey() {
    Run();
    root_promise()->AssignSchedulingKey(serial_id());
  }

  virtual BasePromise *root_promise() = 0;
  virtual void ResetRoot() = 0;

  uint64_t serial_id() const { return sid; }
  uint64_t epoch_nr() const { return sid >> 32; }

  uint64_t AffinityFromRow(VHandle *row);
  uint64_t AffinityFromRows(uint64_t bitmap, VHandle *const *it);
  uint64_t AffinityFromRows(uint64_t bitmap, std::initializer_list<VHandle *> con) {
    return AffinityFromRows(bitmap, con.begin());
  }

  template <typename Api>
  class TxnApi {
   protected:
    uint64_t sid;
    uint64_t epoch_nr;
    Api *api;
   public:
    TxnApi(uint64_t sid, uint64_t epoch_nr, Api *api)
        : sid(sid), epoch_nr(epoch_nr), api(api) {}
    uint64_t serial_id() const { return sid; }
  };

  class TxnVHandle : public TxnApi<VHandle> {
   public:
    using TxnApi<VHandle>::TxnApi;
    void AppendNewVersion(bool is_ondemand_split = false);

    VarStr *ReadVarStr();
    template <typename T> T Read() {
      return ReadVarStr()->ToType<T>();
    }

    bool WriteVarStr(VarStr *obj);
    template <typename T> bool Write(const T &o) {
      return WriteVarStr(o.Encode());
    }

    template <typename T> bool WriteTryInline(const T &o) {
      return WriteVarStr(o.EncodeFromPtrOrDefault(api->AllocFromInline(o.EncodeSize())));
    }

    bool Delete() {
      return WriteVarStr(nullptr);
    }
  };

  class TxnHandle {
    uint64_t sid;
    uint64_t epoch_nr;
   public:
    TxnHandle(uint64_t sid, uint64_t epoch_nr) : sid(sid), epoch_nr(epoch_nr) {}
    TxnHandle() {}

    TxnVHandle operator()(VHandle *vhandle) const { return TxnVHandle(sid, epoch_nr, vhandle); }

    uint64_t serial_id() const { return sid; }
  };

  TxnHandle index_handle() { return TxnHandle{sid, epoch->id()}; }

  struct TxnIndexOpContext {
    static constexpr size_t kMaxPackedKeys = 15;
    TxnHandle handle;
    EpochObject state;

    // We can batch a lot of keys in the same context. We also should mark if
    // some keys are not used at all. Therefore, we need a bitmap.
    uint16_t keys_bitmap;
    uint16_t slices_bitmap;
    uint16_t rels_bitmap;

    uint16_t key_len[kMaxPackedKeys];
    const uint8_t *key_data[kMaxPackedKeys];
    int16_t slice_ids[kMaxPackedKeys];
    int16_t relation_ids[kMaxPackedKeys];

    template <typename Func>
    static void ForEachWithBitmap(uint16_t bitmap, Func f) {
      for (int i = 0, j = 0; i < kMaxPackedKeys; i++) {
        const uint16_t mask = (1 << i);
        if (bitmap & mask) {
          f(j, i);
          j++;
        }
      }
    }
   private:
    template <typename R>
    int _FromKeyParam(uint16_t bitmap, int bitshift, int shift, R param) {
      auto rel_id = R::kRelationId;
      for (int i = bitshift; i < kMaxPackedKeys && i < bitshift + param.size(); i++) {
        if (bitmap & (1 << i)) {
          auto varstr = param[i - bitshift].EncodeFromRoutine();
          key_len[shift] = varstr->len;
          key_data[shift] = varstr->data;
          relation_ids[shift] = rel_id;
          slice_ids[shift] = param.EncodeToSliceId(i - bitshift);

          shift++;
        }
      }
      return shift;
    }
    template <typename R, typename ...T>
    void _FromKeyParam(uint16_t bitmap, int bitshift, int shift, R param, T ...rest) {
      shift = _FromKeyParam(bitmap, bitshift, shift, param);
      _FromKeyParam(bitmap, bitshift + param.size(), shift, rest...);
    }
   public:

    // We don't need to worry about padding because TxnHandle is perfectly padded.
    // We also need to send three bitmaps.
    static constexpr size_t kHeaderSize =
        sizeof(TxnHandle) + sizeof(EpochObject)
        + sizeof(uint16_t) + sizeof(uint16_t) + sizeof(uint16_t);

    TxnIndexOpContext(TxnHandle handle, EpochObject state,
                      uint16_t keys_bitmap, VarStr **keys,
                      uint16_t slices_bitmap, int16_t *slice_ids,
                      uint16_t rels_bitmap, int16_t *rels);
    template <typename ...T>
    TxnIndexOpContext(TxnHandle handle, EpochObject state, uint16_t bitmap, T ...params)
        : handle(handle), state(state),
          keys_bitmap(bitmap), slices_bitmap(bitmap), rels_bitmap(bitmap) {
      _FromKeyParam(bitmap, 0, 0, params...);
    }

    TxnIndexOpContext() {}

    size_t EncodeSize() const;
    uint8_t *EncodeTo(uint8_t *buf) const;
    const uint8_t *DecodeFrom(const uint8_t *buf);
  };

  template <typename Extra>
  struct TxnIndexOpContextEx : public TxnIndexOpContext, public Extra {
    using TxnIndexOpContext::TxnIndexOpContext;

    void set_extra(const Extra &rhs) {
      (Extra &)(*this) = rhs;
    }

    size_t EncodeSize() const {
      return TxnIndexOpContext::EncodeSize() + Extra::EncodeSize();
    }
    uint8_t *EncodeTo(uint8_t *buf) const {
      return Extra::EncodeTo(TxnIndexOpContext::EncodeTo(buf));
    }
    const uint8_t *DecodeFrom(const uint8_t *buf) {
      return Extra::DecodeFrom(TxnIndexOpContext::DecodeFrom(buf));
    }
  };

  template <>
  struct TxnIndexOpContextEx<void> : public TxnIndexOpContext {
    using TxnIndexOpContext::TxnIndexOpContext;
  };

  static constexpr size_t kMaxRangeScanKeys = 32;
  using LookupRowResult = std::array<VHandle *, kMaxRangeScanKeys>;

 protected:
  struct TxnIndexLookupOpImpl {
    TxnIndexLookupOpImpl(const TxnIndexOpContext &ctx, int idx);
    LookupRowResult result;
  };
  struct TxnIndexInsertOpImpl {
    TxnIndexInsertOpImpl(const TxnIndexOpContext &ctx, int idx);
    VHandle *result;
  };
};

}

#endif /* TXN_H */
