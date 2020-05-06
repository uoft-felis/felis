#ifndef TPCC_NEW_ORDER_H
#define TPCC_NEW_ORDER_H

#include "tpcc.h"
#include "txn_cc.h"
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

  uint ts_now;

  struct OrderDetail {
    uint item_id[kNewOrderMaxItems];
    uint supplier_warehouse_id[kNewOrderMaxItems];
    uint order_quantities[kNewOrderMaxItems];
  } detail;
};


struct NewOrderState {

  /*
  VHandle *items[15]; // read-only
  struct ItemsLookupCompletion : public TxnStateCompletion<NewOrderState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->items[id] = rows[0];
    }
  };
  NodeBitmap items_nodes;
  */

  VHandle *orderlines[15]; // insert
  struct OrderLinesInsertCompletion : public TxnStateCompletion<NewOrderState> {
    void operator()(int id, VHandle *row) {
      state->orderlines[id] = row;
      handle(row).AppendNewVersion();
    }
  };
  NodeBitmap orderlines_nodes;

  VHandle *oorder; // insert
  VHandle *neworder; // insert
  VHandle *cididx; // insert

  FutureValue<VHandle *> oorder_future, neworder_future;

  struct OtherInsertCompletion : public TxnStateCompletion<NewOrderState> {
    void operator()(int id, VHandle *row) {
      if (id == 0) {
        state->oorder = row;
      } else if (id == 1) {
        state->neworder = row;
      } else if (id == 2) {
        state->cididx = row;
      }
      handle(row).AppendNewVersion(id < 2);
    }
  };
  NodeBitmap other_inserts_nodes;

  VHandle *stocks[15]; // update
  FutureValue<VHandle *> stock_futures[15];
  struct StocksLookupCompletion : public TxnStateCompletion<NewOrderState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      debug(DBG_WORKLOAD "AppendNewVersion {} sid {}", (void *) rows[0], handle.serial_id());
      state->stocks[id] = rows[0];
      handle(rows[0]).AppendNewVersion(true);
    }
  };
  NodeBitmap stocks_nodes;
};

}

#endif
