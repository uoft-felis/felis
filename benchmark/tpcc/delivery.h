#ifndef TPCC_DELIVERY_H
#define TPCC_DELIVERY_H

#include "tpcc.h"
#include "txn.h"
#include "promise.h"
#include <tuple>

namespace tpcc {

using namespace felis;

struct DeliveryStruct {
  uint warehouse_id;
  uint o_carrier_id;
  uint32_t ts;

  int32_t last_no_o_ids[10];
};

struct DeliveryState {
  struct {
    VHandle *new_orders[10]; // NewOrder per-district
    VHandle *order_lines[10][15]; // OrderLines per NewOrder
    VHandle *oorders[10];
    VHandle *customers[10];
  } rows;
};

}

#endif
