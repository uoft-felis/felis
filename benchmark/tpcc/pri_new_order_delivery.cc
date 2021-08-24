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

  int sum = 0;
  for (int i = 0; i < nr_items; i++) {
    auto item = mgr.Get<Item>().Search(Item::Key::New(detail.item_id[i]).EncodeView(buf));
    auto item_value = handle(item).Read<Item::Value>();
    detail.unit_price[i] = item_value.i_price;
    sum += item_value.i_price * detail.order_quantities[i];
  }
  this->delivery_sum = sum;

  auto o_carrier_id = __rdtsc() % 10 + 1; // delivery; random between 1 and 10
  auto args0 = Tuple<OrderDetail>(detail);
  auto args1 = OOrder::Value::New(customer_id, o_carrier_id, nr_items,
                                  all_local, ts_now);

  if (g_tpcc_config.IsWarehousePinnable() || !VHandleSyncService::g_lock_elision) {
    if (VHandleSyncService::g_lock_elision) {
      txn_indexop_affinity = std::numeric_limits<uint32_t>::max(); // magic to flatten out indexops
    }

    state->orderlines_nodes =
        TxnIndexInsert<TpccSliceRouter, PriNewOrderDeliveryState::OrderLinesInsertCompletion, Tuple<OrderDetail>>(
            &args0,
            KeyParam<OrderLine>(orderline_keys, nr_items));

    state->other_inserts_nodes =
        TxnIndexInsert<TpccSliceRouter, PriNewOrderDeliveryState::OtherInsertCompletion, OOrder::Value>(
            &args1,
            KeyParam<OOrder>(oorder_key),
            KeyParam<NewOrder>(neworder_key));

  } else {
    ASSERT_PWV_CONT;

    txn_indexop_affinity = kIndexOpFlatten;
    state->orderlines_nodes =
        TxnIndexInsert<TpccSliceRouter, PriNewOrderDeliveryState::OrderLinesInsertCompletion, Tuple<OrderDetail>>(
            &args0,
            KeyParam<OrderLine>(orderline_keys, nr_items));

    state->other_inserts_nodes =
        TxnIndexInsert<TpccSliceRouter, PriNewOrderDeliveryState::OtherInsertCompletion, OOrder::Value>(
            &args1,
            KeyParam<OOrder>(oorder_key),
            PlaceholderParam());

    state->other_inserts_nodes +=
        TxnIndexInsert<TpccSliceRouter, PriNewOrderDeliveryState::OtherInsertCompletion, void>(
            nullptr,
            PlaceholderParam(),
            KeyParam<NewOrder>(neworder_key));

  }
}

void PriNewOrderDeliveryTxn::Prepare()
{
  state->issue_tsc = __rdtsc();
  state->sid = this->serial_id();
  state->piece_exec_cnt.store(0);
  state->piece_issue_cnt.store(0);

  Stock::Key stock_keys[kNewOrderMaxItems];
  INIT_ROUTINE_BRK(8192);

  if (!VHandleSyncService::g_lock_elision) {
    auto nr_items = detail.nr_items;
    for (int i = 0; i < nr_items; i++) {
      stock_keys[i] =
          Stock::Key::New(detail.supplier_warehouse_id[i], detail.item_id[i]);
    }
    state->stocks_nodes =
        TxnIndexLookup<TpccSliceRouter, PriNewOrderDeliveryState::StocksLookupCompletion, void>(
            nullptr,
            KeyParam<Stock>(stock_keys, nr_items));

    if (g_tpcc_config.IsWarehousePinnable()) {
      root->AssignAffinity(g_tpcc_config.WarehouseToCoreId(warehouse_id));
    }
  } else {
    unsigned int w = 0;
    int istart = 0, iend = 0;
    uint32_t picked = 0;
    int nr_unique_warehouses = 0;

    state->stocks_nodes = NodeBitmap();

    while (true) {
      uint32_t old_bitmap = picked;
      for (int i = 0; i < detail.nr_items; i++) {
        if ((picked & (1 << i)) == 0) {
          if (w == 0) w = detail.supplier_warehouse_id[i];
          else if (w != detail.supplier_warehouse_id[i]) continue;

          stock_keys[iend++] =
              Stock::Key::New(detail.supplier_warehouse_id[i], detail.item_id[i]);
          picked |= (1 << i);
        }
      }
      if (istart == iend) break;

      if (!g_tpcc_config.IsWarehousePinnable()) {
        ASSERT_PWV_CONT;
        // In this situation, w - 1 means the Stock(0) partition anyway.
      }

      txn_indexop_affinity = w - 1;
      nr_unique_warehouses++;

      auto args = Tuple<int>(picked ^ old_bitmap);
      TxnIndexLookup<TpccSliceRouter, PriNewOrderDeliveryState::StocksLookupCompletion, Tuple<int>>(
          &args,
          KeyParam<Stock>(stock_keys + istart, iend - istart));

      w = 0;
      istart = iend;
    }
    abort_if(iend != detail.nr_items, "Bug in NewOrder Prepare() Bohm partitioning");
    state->stocks_nodes.MergeOrAdd(1, (1 << detail.nr_items) - 1);
  }

  auto customer_key = Customer::Key::New(warehouse_id, district_id, customer_id);
  if (VHandleSyncService::g_lock_elision) { // partition
    if (g_tpcc_config.IsWarehousePinnable()) {
      txn_indexop_affinity = warehouse_id - 1;
    } else {
      txn_indexop_affinity = g_tpcc_config.PWVDistrictToCoreId(district_id, 20);
    }
  }
  state->customer_nodes =
      TxnIndexLookup<TpccSliceRouter, PriNewOrderDeliveryState::CustomerLookupCompletion, void>(
          nullptr,
          KeyParam<Customer>(customer_key));
  if (g_tpcc_config.IsWarehousePinnable()) {
    root->AssignAffinity(g_tpcc_config.WarehouseToCoreId(warehouse_id));
  }
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

    if (!Options::kEnablePartition) {
      auto &conf = util::Instance<NodeConfiguration>();
      if (node != conf.node_id())
        continue;
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
    } else { // kEnablePartition
      std::array<int, NewOrderStruct::kNewOrderMaxItems> unique_warehouses;
      int nr_unique_warehouses = 0;
      unique_warehouses.fill(0);

      for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
        if ((bitmap & (1 << i)) == 0) continue;

        auto supp_warehouse = detail.supplier_warehouse_id[i];
        if (std::find(unique_warehouses.begin(), unique_warehouses.begin() + nr_unique_warehouses,
                      supp_warehouse) == unique_warehouses.begin() + nr_unique_warehouses) {
          unique_warehouses[nr_unique_warehouses++] = supp_warehouse;
        }
      }
      for (int f = 0; f < nr_unique_warehouses; f++) {
        auto w = unique_warehouses[f];
        auto aff = std::numeric_limits<uint64_t>::max();

        aff = w - 1;

        state->piece_issue_cnt++;
        root->AttachRoutine(
            MakeContext(bitmap, params, w), node,
            [](const auto &ctx) {
              auto &[state, index_handle, bitmap, params, w] = ctx;
              auto piece_exec_cnt = state->piece_exec_cnt.fetch_add(1);
              if (piece_exec_cnt == 0)
                state->exec_tsc = __rdtsc();

              for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
                if ((bitmap & (1 << i)) == 0) continue;
                if (w > 0 && params.supplier_warehouses[i] != w) continue;

                debug(DBG_WORKLOAD "Txn {} updating its {} row {}",
                      index_handle.serial_id(), i, (void *) state->stocks[i]);

                TxnRow vhandle = index_handle(state->stocks[i]);
                auto stock = vhandle.Read<Stock::Value>();

                if (stock.s_quantity - params.quantities[i] < 10) {
                  stock.s_quantity += 91;
                }
                stock.s_quantity -= params.quantities[i];
                stock.s_ytd += params.quantities[i];
                stock.s_remote_cnt += (params.supplier_warehouses[i] != params.warehouse);

                vhandle.Write(stock);
                ClientBase::OnUpdateRow(state->stocks[i]);
                debug(DBG_WORKLOAD "Txn {} updated its {} row {}",
                      index_handle.serial_id(), i,
                      (void *)state->stocks[i]);
              }

              if (piece_exec_cnt == state->piece_issue_cnt.load() - 1) {
                auto tsc = __rdtsc();
                auto exec = (tsc > state->exec_tsc) ? tsc - state->exec_tsc : 0;
                auto total = exec + state->issue_tsc;
                probes::PriExecTime{exec / 2200, total / 2200, state->sid}();
              }
            },
            aff);
      }
    } // kEnablePartition
  }

  // update customer
  for (auto &p: state->customer_nodes) {
    auto [node, bitmap] = p;

    auto &conf = util::Instance<NodeConfiguration>();
    if (node != conf.node_id())
      continue;

    auto aff = std::numeric_limits<uint64_t>::max();
    if (g_tpcc_config.IsWarehousePinnable()) {
      aff = g_tpcc_config.WarehouseToCoreId(warehouse_id);
    } else if (VHandleSyncService::g_lock_elision) {
      // partition, single warehouse
      aff = g_tpcc_config.PWVDistrictToCoreId(district_id, 20);
    }

    state->piece_issue_cnt++;
    root->AttachRoutine(
        MakeContext(this->delivery_sum), node,
        [](const auto &ctx) {
          auto &[state, index_handle, sum] = ctx;
          auto piece_exec_cnt = state->piece_exec_cnt.fetch_add(1);
          if (piece_exec_cnt == 0)
            state->exec_tsc = __rdtsc();

          INIT_ROUTINE_BRK(4096);
          auto customer = index_handle(state->customer).template Read<Customer::Value>();
          customer.c_balance += sum;
          customer.c_delivery_cnt++;
          index_handle(state->customer).Write(customer);
          ClientBase::OnUpdateRow(state->customer);

          if (piece_exec_cnt == state->piece_issue_cnt.load() - 1) {
            auto tsc = __rdtsc();
            auto exec = tsc - state->exec_tsc;
            auto total = exec + state->issue_tsc;
            probes::PriExecTime{exec / 2200, total / 2200, state->sid}();
          }
        }, aff);
  }
  auto tsc = __rdtsc();
  state->issue_tsc = tsc - state->issue_tsc;
  probes::PriInitTime{state->issue_tsc / 2200, 0, 0, state->sid}();
}

} // namespace tpcc
