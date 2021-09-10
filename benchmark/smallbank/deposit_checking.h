#ifndef SMALLBANK_DEPOSIT_CHECKING_H
#define SMALLBANK_DEPOSIT_CHECKING_H

#include "smallbank.h"
#include "txn_cc.h"

namespace smallbank {

using namespace felis;

struct DepositCheckingStruct {
  uint64_t account_id;
  int64_t deposit_v;
};

struct DepositCheckingState {
  IndexInfo *checking;
  struct Completion : public TxnStateCompletion<DepositCheckingState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->checking = rows[0];
      handle(rows[0]).AppendNewVersion();
    }
  };
};

class DepositCheckingTxn : public Txn<DepositCheckingState>, public DepositCheckingStruct {
  Client *client;

public:
  DepositCheckingTxn(Client *client, uint64_t serial_id);
  DepositCheckingTxn(Client *client, uint64_t serial_id, DepositCheckingStruct *input);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
  void RecoverInputStruct(DepositCheckingStruct *input) {
    this->account_id = input->account_id;
    this->deposit_v = input->deposit_v;
  }
};

} // namespace smallbank

#endif
