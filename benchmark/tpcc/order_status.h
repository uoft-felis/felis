#ifndef TPCC_ORDER_STATUS_H
#define TPCC_ORDER_STATUS_H

#include "tpcc.h"
#include "txn_cc.h"
#include "pwv_graph.h"

namespace tpcc {

using namespace felis;

struct OrderStatusStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_id;
};

struct OrderStatusState {
  VHandle *customer;
  VHandle *order_line[15];
  int oid;
};

}

#endif
