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
  struct {
    VHandle *warehouse;
    VHandle *district;
    VHandle *customer;
  } rows;
};

}

#endif
