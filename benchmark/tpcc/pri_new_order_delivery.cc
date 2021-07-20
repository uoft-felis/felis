#include <numeric>
#include "pri_new_order_delivery.h"

namespace tpcc {

void PriNewOrderDeliveryTxn::PrepareInsert()
{
  auto &mgr = util::Instance<TableManager>();
  auto auto_inc_zone = warehouse_id * 10 + district_id;
  auto oorder_id = mgr.Get<tpcc::OOrder>().AutoIncrement(auto_inc_zone);

  auto oorder_key = OOrder::Key::New(warehouse_id, district_id, oorder_id);
  auto neworder_key = NewOrder::Key::New(warehouse_id, district_id, oorder_id, customer_id);
  OrderLine::Key orderline_keys[kNewOrderMaxItems];
  auto nr_items = detail.nr_items;
  bool all_local = true;

  for (int i = 0; i < nr_items; i++) {
    orderline_keys[i] = OrderLine::Key::New(warehouse_id, district_id, oorder_id, i + 1);
    if (detail.supplier_warehouse_id[i] != warehouse_id)
      all_local = false;
  }

  INIT_ROUTINE_BRK(8192);
  void *buf = mem::AllocFromRoutine(16);

  auto handle = index_handle();

  for (int i = 0; i < nr_items; i++) {
    auto item = mgr.Get<Item>().Search(Item::Key::New(detail.item_id[i]).EncodeView(buf));
    auto item_value = handle(item).Read<Item::Value>();
    detail.unit_price[i] = item_value.i_price;
  }

  auto o_carrier_id = __rdtsc() % 10 + 1; // delivery; random between 1 and 10
  auto args = OOrder::Value::New(customer_id, o_carrier_id, nr_items,
                                  all_local, ts_now);

  state->orderlines_nodes =
      TxnIndexInsert<TpccSliceRouter, PriNewOrderDeliveryState::OrderLinesInsertCompletion, void>(
          nullptr,
          KeyParam<OrderLine>(orderline_keys, nr_items));

  state->other_inserts_nodes =
      TxnIndexInsert<TpccSliceRouter, PriNewOrderDeliveryState::OtherInsertCompletion, OOrder::Value>(
          &args,
          KeyParam<OOrder>(oorder_key),
          KeyParam<NewOrder>(neworder_key));
}

void PriNewOrderDeliveryTxn::Prepare()
{
  Stock::Key stock_keys[kNewOrderMaxItems];

  auto nr_items = detail.nr_items;
  for (int i = 0; i < nr_items; i++) {
    stock_keys[i] =
        Stock::Key::New(detail.supplier_warehouse_id[i], detail.item_id[i]);
  }
  auto customer_key = Customer::Key::New(warehouse_id, district_id, customer_id);

  INIT_ROUTINE_BRK(8192);

  state->stocks_nodes =
      TxnIndexLookup<TpccSliceRouter, PriNewOrderDeliveryState::StocksLookupCompletion, void>(
          nullptr,
          KeyParam<Stock>(stock_keys, nr_items));
  state->customer_nodes =
      TxnIndexLookup<TpccSliceRouter, PriNewOrderDeliveryState::CustomerLookupCompletion, void>(
          nullptr,
          KeyParam<Customer>(customer_key));

  if (g_tpcc_config.IsWarehousePinnable())
    root->AssignAffinity(g_tpcc_config.WarehouseToCoreId(warehouse_id));
}

void PriNewOrderDeliveryTxn::Run()
{
  struct {
    unsigned int quantities[NewOrderStruct::kNewOrderMaxItems];
    unsigned int supplier_warehouses[NewOrderStruct::kNewOrderMaxItems];
    int warehouse;
  } params;

  params.warehouse = warehouse_id;
  auto nr_items = detail.nr_items;

  bool all_local = true;
  for (auto i = 0; i < nr_items; i++) {
    params.quantities[i] = detail.order_quantities[i];
    params.supplier_warehouses[i] = detail.supplier_warehouse_id[i];
    if (detail.supplier_warehouse_id[i] != warehouse_id)
      all_local = false;
  }

  for (auto &p: state->stocks_nodes) {
    auto [node, bitmap] = p;
    auto &conf = util::Instance<NodeConfiguration>();
    if (node == conf.node_id()) {
      for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
        if ((bitmap & (1 << i)) == 0) continue;

        state->stock_futures[i] = UpdateForKey(
            node, state->stocks[i],
            [](const auto &ctx, VHandle *row) {
              auto &[state, index_handle, quantity, remote, i] = ctx;
              debug(DBG_WORKLOAD "Txn {} updating its {} row {}",
                    index_handle.serial_id(), i, (void *) row);

              TxnRow vhandle = index_handle(row);
              auto stock = vhandle.Read<Stock::Value>();

              probes::TpccNewOrder{2, 1}();

              if (stock.s_quantity - quantity < 10) {
                stock.s_quantity += 91;
              }
              stock.s_quantity -= quantity;
              stock.s_ytd += quantity;
              stock.s_remote_cnt += remote ? 1 : 0;

              vhandle.Write(stock);
              ClientBase::OnUpdateRow(row);
              debug(DBG_WORKLOAD "Txn {} updated its {} row {}",
                    index_handle.serial_id(), i,
                    (void *) row);
            },
            params.quantities[i],
            (bool) (params.supplier_warehouses[i] != warehouse_id),
            i);
      }

      auto aff = std::numeric_limits<uint64_t>::max();
      if (g_tpcc_config.IsWarehousePinnable())
        aff = g_tpcc_config.WarehouseToCoreId(warehouse_id);

      root->AttachRoutine(
          MakeContext(bitmap, warehouse_id, params), node,
          [](const auto &ctx) {
            auto &[state, index_handle, bitmap, warehouse_id, params] = ctx;
            for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
              if ((bitmap & (1 << i)) == 0) continue;

              state->stock_futures[i].Invoke(
                  state, index_handle,
                  params.quantities[i],
                  params.supplier_warehouses[i] != warehouse_id,
                  i);
            }
            void *buf = alloca(8);
            auto warehouse = util::Instance<TableManager>().Get<tpcc::Warehouse>().Search(
                Warehouse::Key::New(warehouse_id).EncodeView(buf));
            TxnRow row = index_handle(warehouse);
            row.Read<Warehouse::Value>();
          },
          aff);
    }
  }

  for (auto &p: state->orderlines_nodes) {
    auto [node, bitmap] = p;
    auto &conf = util::Instance<NodeConfiguration>();
    if (node == conf.node_id()) {
      auto aff = std::numeric_limits<uint64_t>::max();
      if (g_tpcc_config.IsWarehousePinnable())
        aff = g_tpcc_config.WarehouseToCoreId(warehouse_id);

      root->AttachRoutine(
          MakeContext(bitmap, detail), node,
          [](const auto &ctx) {
            auto &[state, index_handle, bitmap, detail] = ctx;
            int sum = 0;

            INIT_ROUTINE_BRK(4096);

            for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
              if ((bitmap & (1 << i)) == 0) continue;

              auto item = ClientBase::tables().Get<Item>().Search(Item::Key::New(detail.item_id[i]).EncodeViewRoutine());
              auto item_value = index_handle(item).template Read<Item::Value>();
              auto amount = item_value.i_price * detail.order_quantities[i];

              auto handle = index_handle(state->orderlines[i]);
              auto orderline = OrderLine::Value::New(detail.item_id[i], 0, amount,
                                                    detail.supplier_warehouse_id[i],
                                                    detail.order_quantities[i]);
              sum += orderline.ol_amount;
              orderline.ol_delivery_d = 234567;
              handle.Write(orderline);
              ClientBase::OnUpdateRow(state->orderlines[i]);
            }

            // well, orderline and customer should be in the same warehouse,
            // so I just use the same node. may not work for random sharding or stuff
            auto customer = index_handle(state->customer).template Read<Customer::Value>();
            customer.c_balance = sum;
            index_handle(state->customer).Write(customer);
            ClientBase::OnUpdateRow(state->customer);
          });
    }
  }
}

} // namespace tpcc
