#ifndef TPCC_PAYMENT_H
#define TPCC_PAYMENT_H

#include "tpcc.h"
#include "txn.h"

namespace tpcc {

using namespace felis;

struct PaymentStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_warehouse_id;
  uint customer_district_id;
  int payment_amount;
  uint32_t ts;
  uint customer_id;
};

struct PaymentState {
  VHandle *warehouse;
  VHandle *district;
  VHandle *customer;
  NodeBitmap nodes;
  struct Completion : public TxnStateCompletion<PaymentState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      if (id == 0) {
        state->warehouse = rows[0];
      } else if (id == 1) {
        state->district = rows[0];
      } else if (id == 2) {
        state->customer = rows[0];
      }
      handle(rows[0]).AppendNewVersion();
    }
  };
};

}

#endif
