#ifndef TPCC_PRI_STOCK_H
#define TPCC_PRI_STOCK_H

#include "tpcc.h"
#include "txn_cc.h"
#include "piece_cc.h"
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
  InvokeHandle<PriStockState, unsigned int, int> stock_futures[10];
  struct StocksLookupCompletion : public TxnStateCompletion<PriStockState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      debug(DBG_WORKLOAD "AppendNewVersion {} sid {}", (void *) rows[0], handle.serial_id());
      state->stocks[id] = rows[0];
      handle(rows[0]).AppendNewVersion();
    }
  };
  NodeBitmap stocks_nodes;
};

class PriStockTxn : public Txn<PriStockState>, public PriStockStruct {
  Client *client;
 public:
  PriStockTxn(Client *client, uint64_t serial_id);

  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final {}
};

}

#endif
