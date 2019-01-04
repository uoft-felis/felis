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
  virtual void Run() = 0;

  Promise<DummyValue> *root_promise() {
    return proc.promise();
  }

  void ResetRoot() {
    proc.Reset();
  }

  uint64_t serial_id() const { return sid; }

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
      return kHeaderSize + key_len;
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
             Optional<VoidValue> (*)(const ContextType<void *, Types...>&, Tuple<VHandle *>)>
  TxnSetupVersion(int node, Func func, Types... params) {
    void * p = (void *) (void (*)(const ContextType<void *, Types...> &, VHandle *)) func;
    return std::make_tuple(
        ContextType<void *, Types...>(state, index_handle(), p, params...),
        node,
        [](const ContextType<void *, Types...> &ctx, Tuple<VHandle *> args) -> Optional<VoidValue> {
          void *p = ctx.template _<2>();
          auto state = ctx.template _<0>();
          auto index_handle = ctx.template _<1>();
          auto [handle] = args;

          // TODO: insert if not there...
          if (handle) {
            while (!index_handle(handle).AppendNewVersion());
          }

          auto fp = (void (*)(const ContextType<void *, Types...> &, VHandle *)) p;
          fp(ctx, handle);
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

};

}

#endif /* TXN_H */
