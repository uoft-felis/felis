#ifndef TXN_H
#define TXN_H

#include <cstdlib>
#include <cstdint>
#include <array>
#include "epoch.h"
#include "util.h"
#include "sqltypes.h"
#include "promise.h"
#include "slice.h"

namespace felis {

class VHandle;
class Relation;
class EpochClient;

class BaseTxn {
 protected:
  friend class EpochClient;

  Epoch *epoch;
  uint64_t sid;

  PromiseProc proc;
 public:
  BaseTxn(uint64_t serial_id)
      : epoch(util::Instance<EpochManager>().current_epoch()), sid(serial_id) {}

  virtual ~BaseTxn() {}
  virtual void Prepare() = 0;
  virtual void PrepareInsert() = 0;
  virtual void Run() = 0;

  void RunAndAssignSchedulingKey() {
    Run();
    root_promise()->AssignSchedulingKey(serial_id());
  }

  template <typename TableType>
  VHandle *CreateNewRow(const typename TableType::Key &key) {
    auto slice_id = util::Instance<SliceLocator<TableType>>().Locate(key);
    auto row = new VHandle();
    util::Instance<felis::SliceManager>().OnNewRow(slice_id, TableType::kTable, key, row);

    VHandle::pool.Prefetch();
    RowEntity::pool.Prefetch();

    return row;
  }

  Promise<DummyValue> *root_promise() {
    return proc.promise();
  }

  void ResetRoot() {
    proc.Reset();
  }

  uint64_t serial_id() const { return sid; }
  uint64_t epoch_nr() const { return sid >> 32; }

  template <typename Api>
  class TxnApi {
   protected:
    uint64_t sid;
    uint64_t epoch_nr;
    Api *api;
   public:
    TxnApi(uint64_t sid, uint64_t epoch_nr, Api *api)
        : sid(sid), epoch_nr(epoch_nr), api(api) {}
  };

  class TxnVHandle : public TxnApi<VHandle> {
   public:
    using TxnApi<VHandle>::TxnApi;
    bool AppendNewVersion();

    VarStr *ReadVarStr();
    template <typename T> T Read() {
      return ReadVarStr()->ToType<T>();
    }

    bool WriteVarStr(VarStr *obj);
    template <typename T> bool Write(const T &o) {
      return WriteVarStr(o.Encode());
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

  struct TxnIndexOpContext {
    static constexpr size_t kMaxPackedKeys = 15;
    TxnHandle handle;
    int32_t rel_id;
    // We can batch a lot of keys in the same context. We also should mark if
    // some keys are not used at all. Therefore, we need a bitmap.
    uint16_t keys_bitmap;
    uint16_t values_bitmap;

    uint16_t key_len[kMaxPackedKeys];
    const uint8_t *key_data[kMaxPackedKeys];
    void *value_data[kMaxPackedKeys];

    // We don't need to worry about padding because TxnHandle is perfectly padded.
    // One Relation ID and two bitmaps.
    static constexpr size_t kHeaderSize =
        sizeof(TxnHandle) + sizeof(int32_t) + sizeof(uint16_t) + sizeof(uint16_t);

    TxnIndexOpContext(TxnHandle handle, int32_t rel_id,
                      uint16_t nr_keys, VarStr **keys,
                      uint16_t nr_values, void **values)
        : handle(handle), rel_id(rel_id), keys_bitmap(0), values_bitmap(0) {
      int j = 0;
      for (int i = 0; i < nr_keys && i < kMaxPackedKeys; i++) {
        if (keys[i] == nullptr) continue;
        keys_bitmap |= 1 << i;
        key_len[j] = keys[i]->len;
        key_data[j] = keys[i]->data;
        j++;
      }
      j = 0;
      for (int i = 0; i < nr_values && i < kMaxPackedKeys; i++) {
        if (values[i] == nullptr) continue;
        values_bitmap |= 1 << i;
        value_data[j++] = values[i];
      }
    }

    TxnIndexOpContext(TxnHandle handle, int32_t rel_id, VarStr *key)
        : TxnIndexOpContext(handle, rel_id, 1, &key, 0, nullptr) {}

    TxnIndexOpContext(TxnHandle handle, int32_t rel_id,
                      uint16_t nr_keys, VarStr **keys)
        : TxnIndexOpContext(handle, rel_id, nr_keys, keys, 0, nullptr) {}

    TxnIndexOpContext() {}

    size_t EncodeSize() const {
      size_t sum = 0;
      int nr_keys = __builtin_popcount(keys_bitmap);
      for (auto i = 0; i < nr_keys; i++) {
        sum += 2 + key_len[i];
      }
      sum += __builtin_popcount(values_bitmap) * sizeof(uint64_t);
      return kHeaderSize + sum;
    }
    uint8_t *EncodeTo(uint8_t *buf) const {
      memcpy(buf, this, kHeaderSize);
      uint8_t *p = buf + kHeaderSize;
      int nr_keys = __builtin_popcount(keys_bitmap);
      int nr_values = __builtin_popcount(values_bitmap);

      for (auto i = 0; i < nr_keys; i++) {
        memcpy(p, &key_len[i], 2);
        memcpy(p + 2, key_data[i], key_len[i]);
        p += 2 + key_len[i];
      }

      memcpy(p, value_data, nr_values * sizeof(uint64_t));
      p += nr_values * sizeof(uint64_t);

      return p;
    }
    const uint8_t *DecodeFrom(const uint8_t *buf) {
      memcpy(this, buf, kHeaderSize);

      const uint8_t *p = buf + kHeaderSize;
      int nr_keys = __builtin_popcount(keys_bitmap);
      int nr_values = __builtin_popcount(values_bitmap);

      for (auto i = 0; i < nr_keys; i++) {
        memcpy(&key_len[i], p, 2);
        key_data[i] = (uint8_t *) p + 2;
        p += 2 + key_len[i];
      }

      memcpy(value_data, p, nr_values * sizeof(uint64_t));
      p += nr_values * sizeof(uint64_t);

      return p;
    }
  };

  TxnHandle index_handle() { return TxnHandle{sid, epoch->id()}; }

  template <typename TableT>
  TxnIndexOpContext IndexContext(const typename TableT::Key &key) {
    return TxnIndexOpContext(index_handle(), static_cast<int>(TableT::kTable),
                             key.EncodeFromRoutine());
  }

  template <typename TableT, typename KeyIter>
  TxnIndexOpContext IndexContext(const KeyIter &begin, const KeyIter &end,
                                 uint16_t keys_bitmap = 0xFFFF) {
    VarStr *keys[end - begin];
    int i = 0;
    for (auto it = begin; it != end; ++it, i++) {
      if ((keys_bitmap & (1 << i)) == 0) {
        keys[i] = nullptr;
      } else {
        keys[i] = it->EncodeFromRoutine();
      }
    }
    return TxnIndexOpContext(index_handle(), static_cast<int>(TableT::kTable),
                             end - begin, keys);
  }

  template <typename TableT, typename KeyIter>
  TxnIndexOpContext IndexContext(const KeyIter &begin, const KeyIter &end,
                                 VHandle **values, uint16_t keys_bitmap = 0xFFFF) {
    VarStr *keys[end - begin];
    int i = 0;
    for (auto it = begin; it != end; ++it, i++) {
      if ((keys_bitmap & (1 << i)) == 0) {
        keys[i] = nullptr;
      } else {
        keys[i] = it->EncodeFromRoutine();
      }
    }
    return TxnIndexOpContext(index_handle(), static_cast<int>(TableT::kTable),
                             end - begin, keys, end - begin, (void **) values);
  }

  template <typename TableT>
  std::tuple<TxnIndexOpContext,
             int,
             Optional<Tuple<std::vector<VHandle *>, uint16_t>> (*)(const TxnIndexOpContext &, DummyValue)>
  TxnLookup(int node, const typename TableT::Key &key) {
    return std::make_tuple(
        IndexContext<TableT>(key), node,
        [](const TxnIndexOpContext &ctx,
           DummyValue _) -> Optional<Tuple<std::vector<VHandle *>, uint16_t>> {
          abort_if(ctx.keys_bitmap != 0x01, "ctx.keys_bitmap != 0x01 {}",
                   ctx.keys_bitmap);
          VHandle *handle = TxnIndexLookupOpImpl(0, ctx);
          std::vector<VHandle *> res;
          res.push_back(handle);
          return Tuple<std::vector<VHandle *>, uint16_t>(res, 0x01);
        });
  }

  template <typename TableT, typename KeyIter>
  std::tuple<TxnIndexOpContext,
             int,
             Optional<Tuple<std::vector<VHandle *>, uint16_t>> (*)(const TxnIndexOpContext &, DummyValue)>
  TxnLookupMany(int node, KeyIter begin, KeyIter end, uint16_t keys_bitmap = 0xFFFF) {
    return std::make_tuple(
        IndexContext<TableT>(begin, end, keys_bitmap),
        node,
        [](const TxnIndexOpContext &ctx, DummyValue _)
        -> Optional<Tuple<std::vector<VHandle *>, uint16_t>> {
          std::vector<VHandle *> rows;
          int j = 0;
          for (auto i = 0; i < ctx.kMaxPackedKeys; i++) {
            if ((ctx.keys_bitmap & (1 << i)) == 0) continue;
            rows.push_back(TxnIndexLookupOpImpl(j, ctx));
            j++;
          }
          return Tuple<std::vector<VHandle *>, uint16_t>(rows, ctx.keys_bitmap);
        });
  }

  template <typename TableT, typename KeyIter>
  std::tuple<TxnIndexOpContext,
             int,
             Optional<VoidValue> (*)(const TxnIndexOpContext &, DummyValue)>
  TxnInsert(int node, KeyIter begin, KeyIter end, VHandle **values, uint16_t bitmap = 0xFFFF) {
    return std::make_tuple(
        IndexContext<TableT>(begin, end, values, bitmap),
        node,
        [](const TxnIndexOpContext &ctx, DummyValue _) -> Optional<VoidValue> {
          TxnIndexInsertOpImpl(ctx);
          return nullopt;
        });
  }

  template <typename TableT, typename Key>
  std::tuple<TxnIndexOpContext,
             int,
             Optional<VoidValue> (*)(const TxnIndexOpContext &, DummyValue)>
  TxnInsertOne(int node, Key k, VHandle *v) {
    Key *begin = &k, *end = begin + 1;
    VHandle **values = &v;
    return TxnInsert<TableT>(node, begin, end, values);
  }

 protected:
  static VHandle *TxnIndexLookupOpImpl(int i, const TxnIndexOpContext &) __attribute__((noinline));
  static void TxnIndexInsertOpImpl(const TxnIndexOpContext &);
};

template <typename TxnState>
class Txn : public BaseTxn {
 public:
  typedef EpochObject<TxnState> State;

 protected:
  State state;
 public:
  Txn(uint64_t serial_id) : BaseTxn(serial_id) {
    state = epoch->AllocateEpochObjectOnCurrentNode<TxnState>();
  }

  template <typename ...Types> using ContextType = sql::Tuple<State, TxnHandle, Types...>;

  template <typename Func, typename ...Types>
  std::tuple<ContextType<void *, Types...>,
             int,
             Optional<VoidValue> (*)(const ContextType<void *, Types...>&, Tuple<std::vector<VHandle *>, uint16_t>)>
  TxnAppendVersion(int node, Func func, Types... params) {
    void * p = (void *) (void (*)(const ContextType<void *, Types...> &, VHandle *, int)) func;
    return std::make_tuple(
        ContextType<void *, Types...>(state, index_handle(), p, params...),
        node,
        [](const ContextType<void *, Types...> &ctx, Tuple<std::vector<VHandle *>, uint16_t> args) -> Optional<VoidValue> {
          void *p = ctx.template _<2>();
          auto state = ctx.template _<0>();
          auto index_handle = ctx.template _<1>();
          auto [handles, bitmap] = args;
          auto fp = (void (*)(const ContextType<void *, Types...> &, VHandle *, int)) p;

          int j = 0;
          for (auto i = 0; i < TxnIndexOpContext::kMaxPackedKeys; i++) {
            if ((bitmap & (1 << i)) == 0) continue;
            __builtin_prefetch(handles[j]);
            fp(ctx, handles[j++], i);
          }

          for (auto &handle: handles) {
            while (!index_handle(handle).AppendNewVersion());
          }

          return nullopt;
        });
  }

  template <typename Func, typename ...Types>
  std::tuple<ContextType<Types...>,
             int,
             Func>
  TxnProc(int node, Func func, Types... params) {
    return std::make_tuple(ContextType<Types...>(state, index_handle(), params...),
                           node,
                           func);
  }


  template <typename Func, typename ...Types>
  std::tuple<PipelineClosure<ContextType<Types...>>,
             int,
             Func>
  TxnPipelineProc(int node, Func func, int nr_pipelines, Types... params) {
    return std::make_tuple(PipelineClosure<ContextType<Types...>>(nr_pipelines,
                                                                  state,
                                                                  index_handle(),
                                                                  params...),
                           node,
                           func);
  }

};

}

#endif /* TXN_H */
