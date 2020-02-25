#include "benchmark/tpcc/tpcc_priority.h"

namespace tpcc {

static StockTxn txn();

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

bool StockTxn::Run()
{
  std::vector<Stock::Key> stock_keys;
  for (int i = 0; i < this->nr_items; ++i) {
    stock_keys.push_back(Stock::Key::New(this->warehouse_id,
                                         this->detail.item_id[i]));
  }

  std::vector<felis::VHandle*> stock_rows;
  if (!InitRegisterUpdate<Stock>(stock_keys, stock_rows)) return false;
  if (!Init()) return false;

  for (int i = 0; i < this->nr_items; ++i) {
    debug(DBG_WORKLOAD "Priority Txn {} updating its {} row {}",
          serial_id(), i, (void *) stock_rows[i]);

    auto stock = Read<Stock::Value>(stock_rows[i]);
    stock.s_quantity += this->detail.stock_quantities[i];
    Write(stock_rows[i], stock);
    ClientBase::OnUpdateRow(stock_rows[i]);

    debug(DBG_WORKLOAD "Priority Txn {} updated its {} row {}",
          serial_id(), i, (void *)stock_rows[i]);
  }
  return Commit();
}

}
