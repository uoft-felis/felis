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
  VHandle *customer;
  VHandle *order_line[15];
  int oid;
};

class OrderStatusTxn : public Txn<OrderStatusState>, public OrderStatusStruct {
  Client *client;
 public:
  OrderStatusTxn(Client *client, uint64_t serial_id);
  void Run() override final;
  void PrepareInsert() override final {}
  void Prepare() override final;
};

}

#endif
