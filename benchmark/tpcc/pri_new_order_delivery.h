#ifndef TPCC_PRI_NEW_ORDER_DELIVERY_H
#define TPCC_PRI_NEW_ORDER_DELIVERY_H

#include "new_order.h"

namespace tpcc {

using namespace felis;

struct PriNewOrderDeliveryState {
  VHandle *orderlines[15]; // insert
  struct OrderLinesInsertCompletion : public TxnStateCompletion<PriNewOrderDeliveryState> {
    void operator()(int id, VHandle *row) {
      state->orderlines[id] = row;
      handle(row).AppendNewVersion();
    }
  };
  NodeBitmap orderlines_nodes;

  VHandle *oorder; // insert
  VHandle *neworder; // insert
  struct OtherInsertCompletion : public TxnStateCompletion<PriNewOrderDeliveryState> {
    void operator()(int id, VHandle *row) {
      if (id == 0) {
        state->oorder = row;
      } else if (id == 1) {
        state->neworder = row;
      }
      handle(row).AppendNewVersion();
    }
  };
  NodeBitmap other_inserts_nodes;

  VHandle *stocks[15]; // update
  struct StocksLookupCompletion : public TxnStateCompletion<PriNewOrderDeliveryState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      debug(DBG_WORKLOAD "AppendNewVersion {} sid {}", (void *) rows[0], handle.serial_id());
      state->stocks[id] = rows[0];
      handle(rows[0]).AppendNewVersion();
    }
  };
  NodeBitmap stocks_nodes;

  VHandle *customer; // update
  struct CustomerLookupCompletion : public TxnStateCompletion<PriNewOrderDeliveryState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->customer = rows[0];
      handle(rows[0]).AppendNewVersion();
    }
  };
  NodeBitmap customer_nodes; //  actually unused
};

}

#endif
