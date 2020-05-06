#ifndef TPCC_STOCK_LEVEL_H
#define TPCC_STOCK_LEVEL_H

#include "txn_cc.h"
#include "tpcc.h"

namespace tpcc {

struct StockLevelStruct {
  uint warehouse_id;
  uint district_id;
  int threshold;
};

struct StockLevelState {
  int current_oid;
};

}

#endif
