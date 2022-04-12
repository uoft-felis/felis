#ifndef MCBM_LOOKUP_H
#define MCBM_LOOKUP_H

#include "mcbm.h"
#include "txn_cc.h"

namespace mcbm {

using namespace felis;

struct LookupStruct {
  uint64_t row_id;
};

struct LookupState {
  IndexInfo *row;
};

class LookupTxn : public Txn<LookupState>, public LookupStruct {
  Client *client;

public:
  LookupTxn(Client *client, uint64_t serial_id);
  LookupTxn(Client *client, uint64_t serial_id, LookupStruct *input);

  static void ReadRowLookup(TxnRow vhandle);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
  void RecoverInputStruct(LookupStruct *input) {
    this->row_id = input->row_id;
  }
};

} // namespace mcbm

#endif
