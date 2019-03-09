#ifndef TXN_H
#define TXN_H

#include <cstdlib>
#include <cstdint>
#include <array>
#include "epoch.h"
#include "util.h"
#include "sqltypes.h"
#include "promise.h"

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
    uint16_t nr_keys; // we can batch a lot of keys in the same context.
    uint16_t nr_values;

    uint16_t key_len[kMaxPackedKeys];
    const uint8_t *key_data[kMaxPackedKeys];
    uint64_t value_data[kMaxPackedKeys];

    // We don't need to worry about padding because TxnHandle is perfectly padded.
    static constexpr size_t kHeaderSize =
        sizeof(TxnHandle) + sizeof(int32_t) + sizeof(uint16_t) + sizeof(uint16_t);

    TxnIndexOpContext(TxnHandle handle, int32_t rel_id,
                      uint16_t nr_keys, VarStr **keys,
                      uint16_t nr_values, uint64_t *values)
        : handle(handle), rel_id(rel_id), nr_keys(nr_keys), nr_values(nr_values) {
      for (uint32_t i = 0; i < nr_keys; i++) {
        key_len[i] = keys[i]->len;
        key_data[i] = keys[i]->data;
      }
      memcpy(value_data, values, nr_values * sizeof(uint64_t));
    }

    TxnIndexOpContext(TxnHandle handle, int32_t rel_id, VarStr *key)
        : TxnIndexOpContext(handle, rel_id, 1, &key, 0, nullptr) {}

    TxnIndexOpContext(TxnHandle handle, int32_t rel_id,
                      uint16_t nr_keys, VarStr **keys)
        : TxnIndexOpContext(handle, rel_id, nr_keys, keys, 0, nullptr) {}

    TxnIndexOpContext() {}

    size_t EncodeSize() const {
      size_t sum = 0;
      for (uint32_t i = 0; i < nr_keys; i++) sum += 2 + key_len[i];
      sum += nr_values * sizeof(uint64_t);
      return kHeaderSize + sum;
    }
    uint8_t *EncodeTo(uint8_t *buf) const {
      memcpy(buf, this, kHeaderSize);
      uint8_t *p = buf + kHeaderSize;

      for (uint16_t i = 0; i < nr_keys; i++) {
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
      for (uint16_t i = 0; i < nr_keys; i++) {
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
  TxnIndexOpContext IndexContext(const KeyIter &begin, const KeyIter &end) {
    VarStr *keys[end - begin];
    int i = 0;
    for (auto it = begin; it != end; ++it, i++) {
      keys[i] = it->EncodeFromRoutine();
    }
    return TxnIndexOpContext(index_handle(), static_cast<int>(TableT::kTable),
                             end - begin, keys);
  }

  template <typename TableT, typename KeyIter>
  TxnIndexOpContext IndexContext(const KeyIter &begin, const KeyIter &end,
                                 VHandle **values) {
    VarStr *keys[end - begin];
    int i = 0;
    for (auto it = begin; it != end; ++it, i++) {
      keys[i] = it->EncodeFromRoutine();
    }
    return TxnIndexOpContext(index_handle(), static_cast<int>(TableT::kTable),
                             end - begin, keys, end - begin, (uint64_t *) values);
  }

  template <typename TableT>
  std::tuple<TxnIndexOpContext,
             int,
             Optional<Tuple<VHandle *, int>> (*)(const TxnIndexOpContext &, DummyValue)>
  TxnLookup(int node, const typename TableT::Key &key) {
    return std::make_tuple(
        IndexContext<TableT>(key),
        node,
        [](const TxnIndexOpContext &ctx, DummyValue _) -> Optional<Tuple<VHandle *, int>> {
          return TxnIndexLookupOpImpl(0, ctx);
        });
  }

  template <typename TableT, typename KeyIter>
  std::tuple<TxnIndexOpContext,
             int,
             Optional<Tuple<VHandle *, int>> (*)(const TxnIndexOpContext &, Tuple<int>)>
  TxnLookupMany(int node, KeyIter begin, KeyIter end) {
    return std::make_tuple(
        IndexContext<TableT>(begin, end),
        node,
        [](const TxnIndexOpContext &ctx, Tuple<int> selector) -> Optional<Tuple<VHandle *, int>> {
          return TxnIndexLookupOpImpl(selector._<0>(), ctx);
        });
  }

  template <typename TableT, typename KeyIter>
  std::tuple<TxnIndexOpContext,
             int,
             Optional<VoidValue> (*)(const TxnIndexOpContext &, DummyValue)>
  TxnInsert(int node, KeyIter begin, KeyIter end, VHandle **values) {
    return std::make_tuple(
        IndexContext<TableT>(begin, end, values),
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
  static Optional<Tuple<VHandle *, int>> TxnIndexLookupOpImpl(int i, const TxnIndexOpContext &);
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
             Optional<VoidValue> (*)(const ContextType<void *, Types...>&, Tuple<VHandle *, int>)>
  TxnAppendVersion(int node, Func func, Types... params) {
    void * p = (void *) (void (*)(const ContextType<void *, Types...> &, VHandle *, int)) func;
    return std::make_tuple(
        ContextType<void *, Types...>(state, index_handle(), p, params...),
        node,
        [](const ContextType<void *, Types...> &ctx, Tuple<VHandle *, int> args) -> Optional<VoidValue> {
          void *p = ctx.template _<2>();
          auto state = ctx.template _<0>();
          auto index_handle = ctx.template _<1>();
          auto [handle, selector] = args;

          if (handle) {
            while (!index_handle(handle).AppendNewVersion());
          }

          auto fp = (void (*)(const ContextType<void *, Types...> &, VHandle *, int)) p;
          fp(ctx, handle, selector);
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
