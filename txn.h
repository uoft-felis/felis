#ifndef TXN_H
#define TXN_H

#include <cstdlib>
#include <cstdint>
#include <array>
#include "epoch.h"
#include "util.h"

namespace dolly {

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

  class TxnIndex {
    uint64_t sid;
    uint64_t epoch_nr;
    Relation *rel;
   public:
    TxnIndex(uint64_t sid, uint64_t epoch_nr, Relation &rel)
        : sid(sid), epoch_nr(epoch_nr), rel(&rel) {}
    VHandle *Lookup(const VarStr *k);
  };

  class TxnIndexHandle {
    uint64_t sid;
    uint64_t epoch_nr;
   public:
    TxnIndexHandle(uint64_t sid, uint64_t epoch_nr) : sid(sid), epoch_nr(epoch_nr) {}
    TxnIndexHandle() {}

    TxnIndex operator()(Relation &rel) { return TxnIndex(sid, epoch_nr, rel); }
  };

  TxnIndexHandle index_handle() { return TxnIndexHandle{sid, epoch->id()}; }
};

template <typename TxnState>
class Txn : public BaseTxn {
 protected:
  typedef EpochObject<TxnState> State;
  EpochObject<TxnState> state;
 public:
  Txn() {
    state = epoch->AllocateEpochObjectOnCurrentNode<TxnState>();
    // The state is only initialized on the current node, which is the coordinator.
    new ((TxnState *) state) TxnState();
  }

  template <typename ...Types>
  struct CaptureStruct {
    State state;
    TxnIndexHandle indexer;
    std::tuple<Types...> params;
  };

  template <typename ...Types>
  CaptureStruct<Types...> Capture(Types... args) {
    return CaptureStruct<Types...>{state, index_handle(), std::make_tuple(args...)};
  }

};

}

#endif /* TXN_H */
