#ifndef SMALLBANK_AMALGAMATE_H
#define SMALLBANK_AMALGAMATE_H

#include "smallbank.h"
#include "txn_cc.h"

namespace smallbank {

using namespace felis;

struct AmalgamateStruct {
  uint64_t account_id_1;
  uint64_t account_id_2;
};

struct AmalgamateState {
  IndexInfo *saving_1;
  IndexInfo *checking_1;
  IndexInfo *checking_2;

  struct Completion : public TxnStateCompletion<AmalgamateState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      if (id == 0){
        state->saving_1 = rows[0];
        handle(rows[0]).AppendNewVersion();
      }
      else if (id == 1) {
        state->checking_1 = rows[0];
        handle(rows[0]).AppendNewVersion();
      }
      else if (id == 2) {
        state->checking_2 = rows[0];
        handle(rows[0]).AppendNewVersion();
      }
    }
  };
};

class AmalgamateTxn : public Txn<AmalgamateState>, public AmalgamateStruct {
  Client *client;

public:
  AmalgamateTxn(Client *client, uint64_t serial_id);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
};

} // namespace smallbank

#endif
