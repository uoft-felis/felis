#ifndef TPCC_NEW_ORDER_H
#define TPCC_NEW_ORDER_H

#include "tpcc.h"
#include "txn.h"
#include "promise.h"
#include <tuple>

namespace tpcc {

using namespace felis;

struct NewOrderStruct {
  static constexpr int kNewOrderMaxItems = 15;

  uint warehouse_id;
  uint district_id;
  uint customer_id;
  uint nr_items;

  ulong new_order_id;

  uint ts_now;

  uint item_id[kNewOrderMaxItems];
  uint supplier_warehouse_id[kNewOrderMaxItems];
  uint order_quantities[kNewOrderMaxItems];
};


struct NewOrderState {
  struct {
    VHandle *warehouse;
    VHandle *district;
    VHandle *stocks[15];
  } rows;
};

}

#endif
