#ifndef MCBM_DELETE_H
#define MCBM_DELETE_H

#include "mcbm.h"
#include "txn_cc.h"

namespace mcbm {

using namespace felis;

struct DeleteStruct {
  uint64_t row_id;
};

struct DeleteState {
  IndexInfo *row;
};

class DeleteTxn : public Txn<DeleteState>, public DeleteStruct {
  Client *client;

public:
  DeleteTxn(Client *client, uint64_t serial_id);
  DeleteTxn(Client *client, uint64_t serial_id, DeleteStruct *input);

  static void Dummy(TxnRow vhandle);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
  void RecoverInputStruct(DeleteStruct *input) {
    this->row_id = input->row_id;
  }
};

} // namespace mcbm

#endif
