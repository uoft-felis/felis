#ifndef TPCC_NEW_ORDER_H
#define TPCC_NEW_ORDER_H

#include "tpcc.h"
#include "txn_cc.h"
#include "piece_cc.h"
#include <tuple>

namespace tpcc {

using namespace felis;

struct NewOrderStruct {
  static constexpr int kNewOrderMaxItems = 15;

  uint warehouse_id;
  uint district_id;
  uint customer_id;

  uint ts_now;

  struct OrderDetail {
    // shirley: add these fields so we can set the keys on insert completion
    int warehouse_id;
    int district_id;
    int customer_id;
    int oorder_id;
    uint nr_items;
    uint item_id[kNewOrderMaxItems];
    uint supplier_warehouse_id[kNewOrderMaxItems];
    uint order_quantities[kNewOrderMaxItems];

    uint unit_price[kNewOrderMaxItems];
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

  // shirley: add these fields so we can set the keys on insert completion
  int warehouse_id = -1;
  int district_id = -1;
  int customer_id = -1;
  int oorder_id = -1;

  IndexInfo *orderlines[15]; // insert
  struct OrderLinesInsertCompletion : public TxnStateCompletion<NewOrderState> {
    Tuple<NewOrderStruct::OrderDetail> args;
    void operator()(int id, IndexInfo *row) {
      state->orderlines[id] = row;
      // shirley: Don't call append new version bc don't need version array yet.
      // handle(row).AppendNewVersion();

      auto &[detail] = args;
      auto amount = detail.unit_price[id] * detail.order_quantities[id];

      // shirley: setting keys in vhandle
      // shirley: id can be 0-14. the ID used in keys should be 1-15 so need to add 1.
      if (id == 0){
        state->warehouse_id = detail.warehouse_id;
        state->district_id = detail.district_id;
        state->customer_id = detail.customer_id;
        state->oorder_id = detail.oorder_id;
      }
      row->vhandle_ptr()->set_table_keys(detail.warehouse_id,
                                         detail.district_id, 
                                         detail.oorder_id,
                                         id + 1,
                                         (int)tpcc::TableType::OrderLine);

      // shirley: use WriteInitialInline bc writing initial version after row insert
      handle(row).WriteInitialInline(
          OrderLine::Value::New(detail.item_id[id], 0, amount,
                                detail.supplier_warehouse_id[id],
                                detail.order_quantities[id]));
    }
  };
  NodeBitmap orderlines_nodes;

  IndexInfo *oorder; // insert
  IndexInfo *neworder; // insert
  IndexInfo *cididx; // insert

  struct OtherInsertCompletion : public TxnStateCompletion<NewOrderState> {
    OOrder::Value args;
    void operator()(int id, IndexInfo *row) {
      // shirley: don't call append new version bc don't need version array yet.
      // handle(row).AppendNewVersion();
      if (id == 0) {
        state->oorder = row;
        // shirley: setting keys in vhandle
        row->vhandle_ptr()->set_table_keys(state->warehouse_id,
                                           state->district_id, 
                                           state->oorder_id,
                                           -1,
                                           (int)tpcc::TableType::OOrder);
        // shirley: use WriteInitialInline bc writing initial version after row insert
        handle(row).WriteInitialInline(args);
      } else if (id == 1) {
        state->neworder = row;
        // shirley: setting keys in vhandle
        row->vhandle_ptr()->set_table_keys(state->warehouse_id,
                                           state->district_id, 
                                           state->oorder_id,
                                           state->customer_id,
                                           (int)tpcc::TableType::NewOrder);
        // shirley: use WriteInitialInline bc writing initial version after row insert
        handle(row).WriteInitialInline(NewOrder::Value());
      } else if (id == 2) {
        state->cididx = row;
        // shirley: setting keys in vhandle
        row->vhandle_ptr()->set_table_keys(state->warehouse_id,
                                           state->district_id, 
                                           state->customer_id,
                                           state->oorder_id,
                                           (int)tpcc::TableType::OOrderCIdIdx);
        // shirley: use WriteInitialInline bc writing initial version after row insert
        handle(row).WriteInitialInline(OOrderCIdIdx::Value());
      }
      // handle(row).AppendNewVersion(id < 2);
    }
  };
  NodeBitmap other_inserts_nodes;

  IndexInfo *stocks[15]; // update
  InvokeHandle<NewOrderState, unsigned int, bool, int> stock_futures[15];
  struct StocksLookupCompletion : public TxnStateCompletion<NewOrderState> {
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
};

class NewOrderTxn : public Txn<NewOrderState>, public NewOrderStruct {
  Client *client;
 public:
   NewOrderTxn(Client *client, uint64_t serial_id);
   NewOrderTxn(Client *client, uint64_t serial_id, NewOrderStruct *input);

   void Run() override final;
   void Prepare() override final;
   void PrepareInsert() override final;
   void RecoverInputStruct(NewOrderStruct *input) {
     this->warehouse_id = input->warehouse_id;
     this->district_id = input->district_id;
     this->customer_id = input->customer_id;
     this->ts_now = input->ts_now;

     this->detail.warehouse_id = input->detail.warehouse_id;
     this->detail.district_id = input->detail.district_id;
     this->detail.customer_id = input->detail.customer_id;
     this->detail.oorder_id = input->detail.oorder_id;
     this->detail.nr_items = input->detail.nr_items;

     for (int i = 0; i < kNewOrderMaxItems; i++) {
       this->detail.item_id[i] = input->detail.item_id[i];
       this->detail.supplier_warehouse_id[i] =
           input->detail.supplier_warehouse_id[i];
       this->detail.order_quantities[i] = input->detail.order_quantities[i];
       this->detail.unit_price[i] = input->detail.unit_price[i];
     }
  }
};

}

#endif
