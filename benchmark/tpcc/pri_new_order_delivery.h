#ifndef TPCC_PRI_NEW_ORDER_DELIVERY_H
#define TPCC_PRI_NEW_ORDER_DELIVERY_H

#include "new_order.h"

namespace tpcc {

using namespace felis;

struct PriNewOrderDeliveryState {
  VHandle *orderlines[15]; // insert
  struct OrderLinesInsertCompletion : public TxnStateCompletion<PriNewOrderDeliveryState> {
    Tuple<NewOrderStruct::OrderDetail> args;
    void operator()(int id, VHandle *row) {
      state->orderlines[id] = row;
      handle(row).AppendNewVersion();

      auto &[detail] = args;
      auto amount = detail.unit_price[id] * detail.order_quantities[id];

      handle(row).WriteTryInline(
          OrderLine::Value::New(detail.item_id[id], 0, amount,
                                detail.supplier_warehouse_id[id],
                                detail.order_quantities[id]));
    }
  };
  NodeBitmap orderlines_nodes;

  VHandle *oorder; // insert
  VHandle *neworder; // insert
  struct OtherInsertCompletion : public TxnStateCompletion<PriNewOrderDeliveryState> {
    OOrder::Value args;
    void operator()(int id, VHandle *row) {
      handle(row).AppendNewVersion();
      if (id == 0) {
        state->oorder = row;
        handle(row).WriteTryInline(args);
      } else if (id == 1) {
        state->neworder = row;
        handle(row).WriteTryInline(NewOrder::Value());
      }
    }
  };
  NodeBitmap other_inserts_nodes;

  VHandle *stocks[15]; // update
  InvokeHandle<PriNewOrderDeliveryState, unsigned int, bool, int> stock_futures[15];
  struct StocksLookupCompletion : public TxnStateCompletion<PriNewOrderDeliveryState> {
    Tuple<int> args = Tuple<int>(-1);
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      debug(DBG_WORKLOAD "AppendNewVersion {} sid {}", (void *) rows[0], handle.serial_id());
      auto [bitmap]= args;
      if (bitmap == -1) {
        state->stocks[id] = rows[0];
        handle(rows[0]).AppendNewVersion(1);
      } else { // Bohm partitioning
        int idx = 0, oldid = id;
        do {
          idx = __builtin_ctz(bitmap);
          bitmap &= ~(1 << idx);
        } while (id-- > 0);

        state->stocks[idx] = rows[0];
        handle(rows[0]).AppendNewVersion();
      }
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
  uint64_t issue_tsc;
  uint64_t exec_tsc;
  uint64_t sid;
  std::atomic_int piece_exec_cnt;
  std::atomic_int piece_issue_cnt;
};

class PriNewOrderDeliveryTxn : public Txn<PriNewOrderDeliveryState>, public NewOrderStruct {
  Client *client;
  int delivery_sum;
 public:
  PriNewOrderDeliveryTxn(Client *client, uint64_t serial_id)
      : Txn<PriNewOrderDeliveryState>(serial_id),
        NewOrderStruct(client->GenerateTransactionInput<NewOrderStruct>()),
        client(client)
  {}
  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final;
};

}

#endif
