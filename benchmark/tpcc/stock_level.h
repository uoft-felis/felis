#ifndef TPCC_STOCK_LEVEL_H
#define TPCC_STOCK_LEVEL_H

#include "txn_cc.h"
#include "tpcc.h"
#include "pwv_graph.h"

namespace tpcc {

struct StockLevelStruct {
  uint warehouse_id;
  uint district_id;
  int threshold;
};

struct StockLevelState {
  int current_oid;
  std::array<felis::VHandle *, 300> items;
  std::array<uint, 300> item_ids;
  int n;
  felis::FutureValue<void> barrier;
  felis::PieceRoutine *last;

  felis::PWVGraph::Resource *res;
  int nr_res;
};

class StockLevelTxn : public felis::Txn<StockLevelState>, public StockLevelStruct {
  Client *client;
 public:
  StockLevelTxn(Client *client, uint64_t serial_id);

  void PrepareInsert() override final;
  void Prepare() override final;
  void Run() override final;
};

}

#endif
