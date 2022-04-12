#ifndef MCBM_INSERT_H
#define MCBM_INSERT_H

#include "mcbm.h"
#include "txn_cc.h"
#include "piece_cc.h"
#include <tuple>

namespace mcbm {

using namespace felis;

struct InsertStruct {
  uint64_t row_id;
};


struct InsertState {

  // shirley: add these fields so we can set the keys on insert completion
  uint64_t rowid = -1;

  IndexInfo *newrow; // insert
  struct InsertCompletion : public TxnStateCompletion<InsertState> {

    // shirley TODO: confirm what is this args and how does it work.
    Tuple<uint64_t> args;
    void operator()(int id, IndexInfo *row) {
      state->newrow = row;

      // shirley: setting keys in vhandle
      state->rowid = args.value;
      row->vhandle_ptr()->set_table_keys(args.value,
                                         -1, 
                                         -1,
                                         -1,
                                         (int)mcbm::TableType::MBTable);

      MBTable::Value v;
      v.v = args.value;

      // shirley: use WriteInitialInline bc writing initial version after row insert
      handle(row).WriteInitialInline(v);
    }
  };
};

class InsertTxn : public Txn<InsertState>, public InsertStruct {
  Client *client;
 public:
   InsertTxn(Client *client, uint64_t serial_id);
   InsertTxn(Client *client, uint64_t serial_id, InsertStruct *input);

   void Run() override final;
   void Prepare() override final;
   void PrepareInsert() override final;
   void RecoverInputStruct(InsertStruct *input) {
     this->row_id = input->row_id;
  }
};

}

#endif
