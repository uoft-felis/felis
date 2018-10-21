#ifndef TXN_H
#define TXN_H

#include <cstdlib>
#include <cstdint>
#include <array>
#include "epoch.h"
#include "util.h"

namespace felis {

class VHandle;
class Relation;

class BaseTxn {
 protected:
  Epoch *epoch;
  uint64_t sid;
 public:
  BaseTxn() {
    epoch = util::Instance<EpochManager>().current_epoch();
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

  class TxnIndex : public TxnApi<Relation> {
   public:
    using TxnApi<Relation>::TxnApi;
    VHandle *Lookup(const VarStr *k);
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

    TxnIndex operator()(Relation &rel) { return TxnIndex(sid, epoch_nr, &rel); }
    TxnVHandle operator()(VHandle *vhandle) { return TxnVHandle(sid, epoch_nr, vhandle); }
  };

  TxnHandle index_handle() { return TxnHandle{sid, epoch->id()}; }
};

template <typename TxnState>
class Txn : public BaseTxn {
 protected:
  typedef EpochObject<TxnState> State;
  State state;
 public:
  Txn() {
    state = epoch->AllocateEpochObjectOnCurrentNode<TxnState>();
    // The state is only initialized on the current node, which is the coordinator.
    new ((TxnState *) state) TxnState();
  }

  template <typename ...Types>
  struct ContextStruct {
    State state;
    TxnHandle handle;
    std::tuple<Types...> params;
  };

  template <typename ...Types>
  ContextStruct<Types...> Context(Types... args) {
    return ContextStruct<Types...>{state, index_handle(), std::make_tuple(args...)};
  }

};

}

#endif /* TXN_H */
