#include "tpcc.h"
#include "txn.h"
#include "promise.h"
#include <tuple>

namespace tpcc {

using namespace felis;

struct NewOrderStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_id;
  uint nr_items;

  ulong new_order_id;

  uint ts_now;

  uint item_id[15];
  uint supplier_warehouse_id[15];
  uint order_quantities[15];
};


struct NewOrderState {
  struct {
    VHandle *district;
    VHandle *stocks[15];
  } rows;
};

}