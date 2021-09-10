#ifndef SMALLBANK_WRITE_CHECK_H
#define SMALLBANK_WRITE_CHECK_H

#include "smallbank.h"
#include "txn_cc.h"

namespace smallbank {

using namespace felis;

struct WriteCheckStruct {
  uint64_t account_id;
  int64_t check_v;
};

struct WriteCheckState {
  IndexInfo *saving;
  IndexInfo *checking;

  struct Completion : public TxnStateCompletion<WriteCheckState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      if (id == 0) {
        state->saving = rows[0];
      } else if (id == 1) {
        state->checking = rows[0];
        handle(rows[0]).AppendNewVersion();
      } 
    }
  };
};

class WriteCheckTxn : public Txn<WriteCheckState>, public WriteCheckStruct {
  Client *client;

public:
  WriteCheckTxn(Client *client, uint64_t serial_id);
  WriteCheckTxn(Client *client, uint64_t serial_id, WriteCheckStruct *input);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
  void RecoverInputStruct(WriteCheckStruct *input) {
    this->account_id = input->account_id;
    this->check_v = input->check_v;
  }
};

} // namespace smallbank

#endif
