#include <numeric>
#include "pri_stock.h"

namespace tpcc {

template <>
PriStockStruct ClientBase::GenerateTransactionInput<PriStockStruct>()
{
  PriStockStruct in;
  in.warehouse_id = PickWarehouse();
  in.nr_items = RandomNumber(1, PriStockStruct::kStockMaxItems);

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

PriStockTxn::PriStockTxn(Client *client, uint64_t serial_id)
    : Txn<PriStockState>(serial_id),
      PriStockStruct(client->GenerateTransactionInput<PriStockStruct>()),
      client(client)
{}

void PriStockTxn::Prepare()
{
  Stock::Key stock_keys[kStockMaxItems];
  INIT_ROUTINE_BRK(8192);

  for (int i = 0; i < nr_items; i++) {
    stock_keys[i] =
        Stock::Key::New(warehouse_id, detail.item_id[i]);
  }

  state->stocks_nodes =
      TxnIndexLookup<TpccSliceRouter, PriStockState::StocksLookupCompletion, void>(
          nullptr,
          KeyParam<Stock>(stock_keys, nr_items));

  if (g_tpcc_config.IsWarehousePinnable()) {
    root->AssignAffinity(g_tpcc_config.WarehouseToCoreId(warehouse_id));
  }
}

void PriStockTxn::Run()
{
  struct {
    unsigned int quantities[PriStockStruct::kStockMaxItems];
    int warehouse;
  } params;

  params.warehouse = warehouse_id;
  for (auto i = 0; i < nr_items; i++) {
    params.quantities[i] = detail.stock_quantities[i];
  }

  for (auto &p: state->stocks_nodes) {
    auto [node, bitmap] = p;

    auto &conf = util::Instance<NodeConfiguration>();
    if (node == conf.node_id()) {
      for (int i = 0; i < PriStockStruct::kStockMaxItems; i++) {
        if ((bitmap & (1 << i)) == 0) continue;

        state->stock_futures[i] = UpdateForKey(
            node, state->stocks[i],
            [](const auto &ctx, VHandle *row) {
              auto &[state, index_handle, quantity, i] = ctx;
              debug(DBG_WORKLOAD "Txn {} updating its {} row {}",
                    index_handle.serial_id(), i, (void *) row);

              TxnRow vhandle = index_handle(row);
              auto stock = vhandle.Read<Stock::Value>();
              stock.s_quantity += quantity;

              vhandle.Write(stock);
              ClientBase::OnUpdateRow(row);
              debug(DBG_WORKLOAD "Txn {} updated its {} row {}",
                    index_handle.serial_id(), i,
                    (void *) row);
            },
            params.quantities[i],
            i);
      }
      auto aff = std::numeric_limits<uint64_t>::max();

      if (g_tpcc_config.IsWarehousePinnable())
        aff = g_tpcc_config.WarehouseToCoreId(warehouse_id);

      root->AttachRoutine(
          MakeContext(bitmap, params), node,
          [](const auto &ctx) {
            auto &[state, index_handle, bitmap, params] = ctx;
            for (int i = 0; i < PriStockStruct::kStockMaxItems; i++) {
              if ((bitmap & (1 << i)) == 0) continue;

              state->stock_futures[i].Invoke(
                  state, index_handle,
                  params.quantities[i],
                  i);
            }
          },
          aff);
    }
  }
}

}
