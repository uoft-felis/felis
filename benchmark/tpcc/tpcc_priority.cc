#include "epoch.h"
#include "benchmark/tpcc/tpcc_priority.h"

namespace tpcc {

template <>
StockTxnInput ClientBase::GenerateTransactionInput<StockTxnInput>()
{
  StockTxnInput in;
  in.warehouse_id = PickWarehouse();
  in.nr_items = RandomNumber(1, StockTxnInput::kStockMaxItems);

  for (int i = 0; i < in.nr_items; i++) {
 again:
    auto id = GetItemId();
    // Check duplicates. Got this from NewOrder.
    for (int j = 0; j < i; j++)
      if (in.detail.item_id[j] == id) goto again;
    in.detail.item_id[i] = id;
    in.detail.stock_quantities[i] = RandomNumber(50, 100);
  }
  return in;
}

bool StockTxn_Run(felis::PriorityTxn *txn)
{
  StockTxnInput input = dynamic_cast<tpcc::Client*>
      (felis::EpochClient::g_workload_client)->GenerateTransactionInput<StockTxnInput>();
  std::vector<Stock::Key> stock_keys;
  for (int i = 0; i < input.nr_items; ++i) {
    stock_keys.push_back(Stock::Key::New(input.warehouse_id,
                                         input.detail.item_id[i]));
  }

  logger->info("[Pri] Priority Txn {} Running!", txn->serial_id());
  std::vector<felis::VHandle*> stock_rows;
  if (!(txn->InitRegisterUpdate<tpcc::Stock>(stock_keys, stock_rows))) {
    logger->info("[Pri] init register failed!");
    return false;
  }
  if (!txn->Init()) {
    logger->info("[Pri] Init() failed!");
    return false;
  }
  logger->info("[Pri] Init() succuess!");

  // TODO: issue PromiseRoutine
  // for (int i = 0; i < input.nr_items; ++i) {
  //   logger->info("[Pri] Priority Txn {} updating its {} row {}",
  //         txn->serial_id(), i, (void *) stock_rows[i]);

  //   auto stock = txn->Read<Stock::Value>(stock_rows[i]);
  //   stock.s_quantity += input.detail.stock_quantities[i];
  //   txn->Write(stock_rows[i], stock);
  //   ClientBase::OnUpdateRow(stock_rows[i]);

  //   logger->info("[Pri] Priority Txn {} updated its {} row {}",
  //         txn->serial_id(), i, (void *)stock_rows[i]);
  // }
  return txn->Commit();
}

}
