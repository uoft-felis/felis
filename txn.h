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

  CompletionObject<util::Ref<CompletionObject<util::Ref<EpochCallback>>>> completion;
 public:
  BaseTxn()
      : epoch(util::Instance<EpochManager>().current_epoch()),
        completion(0, epoch->epoch_client()->completion) {
    completion.Increment(1);
  }

  virtual ~BaseTxn() {}
  virtual void Run() = 0;

  uint64_t serial_id() const { return sid; }
  void set_serial_id(uint64_t s) { sid = s; }

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
  };

  class TxnHandle {
    uint64_t sid;
    uint64_t epoch_nr;
   public:
    TxnHandle(uint64_t sid, uint64_t epoch_nr) : sid(sid), epoch_nr(epoch_nr) {}
    TxnHandle() {}

    TxnVHandle operator()(VHandle *vhandle) { return TxnVHandle(sid, epoch_nr, vhandle); }
  };

  struct TxnIndexOpContext {
    TxnHandle handle;
    int32_t rel_id;
    uint32_t key_len;
    const uint8_t *key_data;

    // We don't need to worry about padding because TxnHandle is perfectly padded.
    static constexpr size_t kHeaderSize =
        sizeof(TxnHandle) + sizeof(int32_t) + sizeof(uint32_t);

    TxnIndexOpContext(TxnHandle handle, int32_t rel_id, VarStr *key)
        : handle(handle), rel_id(rel_id), key_len(key->len), key_data(key->data) {}

    TxnIndexOpContext() {}

    size_t EncodeSize() const {
      return sizeof(TxnHandle) + sizeof(int32_t) + sizeof(uint32_t) + key_len;
    }
    uint8_t *EncodeTo(uint8_t *buf) const {
      memcpy(buf, this, kHeaderSize);
      memcpy(buf + kHeaderSize, key_data, key_len);
      return buf + kHeaderSize + key_len;
    }
    const uint8_t *DecodeFrom(const uint8_t *buf) {
      memcpy(this, buf, kHeaderSize);
      key_data = (uint8_t *) buf + kHeaderSize; // key_data ptr is always borrowed.
      return buf + kHeaderSize + key_len;
    }
  };

  TxnHandle index_handle() { return TxnHandle{sid, epoch->id()}; }
  TxnIndexOpContext IndexContextByStr(int32_t rel_id, VarStr *key) {
    return TxnIndexOpContext(index_handle(), rel_id, key);
  }

  template <typename TableT>
  TxnIndexOpContext IndexContext(const typename TableT::Key &key) {
    return IndexContextByStr(static_cast<int>(TableT::kTable), key.EncodeFromRoutine());
  }

  template <typename TableT>
  std::tuple<TxnIndexOpContext,
             int,
             Optional<Tuple<VHandle *>> (*)(const TxnIndexOpContext &, DummyValue)>
  TxnLookup(int node, const typename TableT::Key &key) {
    return std::make_tuple(
        IndexContext<TableT>(key),
        node,
        [](const TxnIndexOpContext &ctx, DummyValue _) -> Optional<Tuple<VHandle*>> {
          return TxnIndexLookupOpImpl(ctx);
        });
  }

 protected:
  static Optional<Tuple<VHandle *>> TxnIndexLookupOpImpl(const TxnIndexOpContext &);
  static void TxnFuncUnref(BaseTxn *txn, int origin_node_id);
  void TxnFuncRef(int origin_node_id);
};

struct BaseTxnState {
  BaseTxn *txn;
};

template <typename TxnState>
class Txn : public BaseTxn {
 public:
  typedef EpochObject<TxnState> State;

 protected:
  State state;
 public:
  Txn() {
    state = epoch->AllocateEpochObjectOnCurrentNode<TxnState>();
    state->txn = this;
  }

  template <typename ...Types> using ContextType = sql::Tuple<State, TxnHandle, void *, Types...>;

  template <typename Func, typename ...Types>
  std::tuple<ContextType<Types...>,
             int,
             Optional<VoidValue> (*)(const ContextType<Types...>&, Tuple<VHandle *>)>
  TxnSetupVersion(int node, Func func, Types... params) {
    void * p = (void *) (void (*)(const ContextType<Types...> &, VHandle *)) func;
    TxnFuncRef(node);

    return std::make_tuple(
        ContextType<Types...>(state, index_handle(), p, params...),
        node,
        [](const ContextType<Types...> &ctx, Tuple<VHandle *> args) -> Optional<VoidValue> {
          void *p = ctx.template _<2>();
          auto state = ctx.template _<0>();
          auto index_handle = ctx.template _<1>();
          auto [handle] = args;

          // TODO: insert if not there...
          if (handle) {
            index_handle(handle).AppendNewVersion();
          }

          auto fp = (void (*)(const ContextType<Types...> &, VHandle *)) p;
          fp(ctx, handle);

          TxnFuncUnref(state->txn, state.origin_node_id());
          return nullopt;
        });
  }

};

}

#endif /* TXN_H */
