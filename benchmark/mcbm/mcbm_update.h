#ifndef MCBM_UPDATE_H
#define MCBM_UPDATE_H

#include "mcbm.h"
#include "txn_cc.h"

namespace mcbm {

using namespace felis;

struct UpdateStruct {
  uint64_t row_id;
};

struct UpdateState {
  IndexInfo *row;
};

class UpdateTxn : public Txn<UpdateState>, public UpdateStruct {
  Client *client;

public:
  UpdateTxn(Client *client, uint64_t serial_id);
  UpdateTxn(Client *client, uint64_t serial_id, UpdateStruct *input);

  static void Dummy(TxnRow vhandle);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
  void RecoverInputStruct(UpdateStruct *input) {
    this->row_id = input->row_id;
  }
};

} // namespace mcbm

#endif
