#ifndef SMALLBANK_BALANCE_H
#define SMALLBANK_BALANCE_H

#include "smallbank.h"
#include "txn_cc.h"

namespace smallbank {

using namespace felis;

struct BalanceStruct {
  uint64_t account_id;
};

struct BalanceState {
  IndexInfo *saving;
  IndexInfo *checking;
};

class BalanceTxn : public Txn<BalanceState>, public BalanceStruct {
  Client *client;

public:
  BalanceTxn(Client *client, uint64_t serial_id);
  static uint64_t ReadSavingBalance(TxnRow vhandle);
  static uint64_t ReadCheckingBalance(TxnRow vhandle);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
};

} // namespace smallbank

#endif
