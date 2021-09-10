#ifndef TPCC_ORDER_STATUS_H
#define TPCC_ORDER_STATUS_H

#include "tpcc.h"
#include "txn_cc.h"
#include "pwv_graph.h"

namespace tpcc {

using namespace felis;

struct OrderStatusStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_id;
};

struct OrderStatusState {
  IndexInfo *customer;
  IndexInfo *order_line[15];
  int oid;
};

class OrderStatusTxn : public Txn<OrderStatusState>, public OrderStatusStruct {
  Client *client;
 public:
   OrderStatusTxn(Client *client, uint64_t serial_id);
   OrderStatusTxn(Client *client, uint64_t serial_id, OrderStatusStruct *input);
   void Run() override final;
   void PrepareInsert() override final {}
   void Prepare() override final;
   void RecoverInputStruct(OrderStatusStruct *input) {
     this->warehouse_id = input->warehouse_id;
     this->district_id = input->district_id;
     this->customer_id = input->customer_id;
  }
};

}

#endif
