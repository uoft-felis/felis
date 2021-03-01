#ifndef TPCC_PRIORITY_H
#define TPCC_PRIORITY_H

#include "priority.h"
#include "benchmark/tpcc/tpcc.h"

namespace tpcc {

// Pre-generate priority txns for the benchmark before the experiment starts
void GeneratePriorityTxn();

// STOCK transaction: a priority txn, add stock to certain items in a warehouse
struct StockTxnInput {
  static constexpr int kStockMaxItems = 10;
  uint warehouse_id;
  uint nr_items;

  struct StockDetail {
    uint item_id[kStockMaxItems];
    uint stock_quantities[kStockMaxItems];
  } detail;
};

struct DeliveryTxnInput {
  uint warehouse_id;
  uint district_id;
  uint64_t oorder_id;
  uint customer_id;
  uint32_t ts;
  uint nr_items; // order_line's primary key: (w_id, d_id, o_id, i), i in [1, nr_items]
};

template <>
StockTxnInput ClientBase::GenerateTransactionInput<StockTxnInput>();

bool StockTxn_Run(felis::PriorityTxn *txn);
bool NewOrderTxn_Run(felis::PriorityTxn *txn);
bool DeliveryTxn_Run(felis::PriorityTxn *txn);

}

#endif /* TPCC_PRIORITY_H */
