#ifndef TPCC_PRI_STOCK_H
#define TPCC_PRI_STOCK_H

#include "tpcc.h"
#include "txn.h"
#include "promise.h"
#include <tuple>

namespace tpcc {

using namespace felis;

struct PriStockStruct {
  static constexpr int kStockMaxItems = 10;
  uint warehouse_id;
  uint nr_items;

  struct StockDetail {
    uint item_id[kStockMaxItems];
    uint stock_quantities[kStockMaxItems];
  } detail;
};

struct PriStockState {
  VHandle *stocks[10]; // update
  struct StocksLookupCompletion : public TxnStateCompletion<PriStockState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      debug(DBG_WORKLOAD "AppendNewVersion {} sid {}", (void *) rows[0], handle.serial_id());
      state->stocks[id] = rows[0];
      handle(rows[0]).AppendNewVersion();
    }
  };
  NodeBitmap stocks_nodes;
};

}

#endif
