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
  // generate txn input
  StockTxnInput txnInput = dynamic_cast<tpcc::Client*>
      (felis::EpochClient::g_workload_client)->GenerateTransactionInput<StockTxnInput>();
  std::vector<Stock::Key> stock_keys;
  for (int i = 0; i < txnInput.nr_items; ++i) {
    stock_keys.push_back(Stock::Key::New(txnInput.warehouse_id,
                                         txnInput.detail.item_id[i]));
  }

  // init
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

  struct Context {
    uint warehouse_id;
    uint item_id;
    uint quantity;
    felis::VHandle* stock_row;
    felis::PriorityTxn *txn;
  };

  // issue promise
  for (int i = 0; i < txnInput.nr_items; ++i) {
    auto lambda =
        [](std::tuple<Context> capture) {
          auto [ctx] = capture;
            auto stock = ctx.txn->Read<Stock::Value>(ctx.stock_row);
            printf("LAMBDA updates: w_id %u, i_id %u, %u -> %u, %p\n",
                   ctx.warehouse_id, ctx.item_id, stock.s_quantity,
                   stock.s_quantity + ctx.quantity, ctx.stock_row);
            stock.s_quantity += ctx.quantity;
            ctx.txn->Write(ctx.stock_row, stock);
            ClientBase::OnUpdateRow(ctx.stock_row);

        };
    Context ctx{txnInput.warehouse_id,
                txnInput.detail.item_id[i],
                txnInput.detail.stock_quantities[i],
                stock_rows[i],
                txn};
    txn->IssuePromise(ctx, lambda);
  }

  return txn->Commit();
}

}
