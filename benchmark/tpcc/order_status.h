#ifndef TPCC_ORDER_STATUS_H
#define TPCC_ORDER_STATUS_H

#include "tpcc.h"
#include "txn_cc.h"

namespace tpcc {

using namespace felis;

struct OrderStatusStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_id;
};

struct OrderStatusState {
  // All query done insde Run()
};

}

#endif
