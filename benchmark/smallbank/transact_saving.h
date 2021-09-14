#ifndef SMALLBANK_TRANSACT_SAVING_H
#define SMALLBANK_TRANSACT_SAVING_H

#include "smallbank.h"
#include "txn_cc.h"

namespace smallbank {

using namespace felis;

struct TransactSavingStruct {
  static constexpr int percent_abort = 10;
  uint64_t account_id;
  int64_t transact_v;
};

struct TransactSavingState {
  IndexInfo *saving;
  bool aborted = false;
  struct Completion : public TxnStateCompletion<TransactSavingState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->saving = rows[0];
      handle(rows[0]).AppendNewVersion();
    }
  };
};

class TransactSavingTxn : public Txn<TransactSavingState>, public TransactSavingStruct {
  Client *client;

public:
  TransactSavingTxn(Client *client, uint64_t serial_id);
  TransactSavingTxn(Client *client, uint64_t serial_id, TransactSavingStruct *input);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
  void RecoverInputStruct(TransactSavingStruct *input) {
    this->account_id = input->account_id;
    this->transact_v = input->transact_v;
  }
};

} // namespace smallbank

#endif
