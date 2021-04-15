#include <numeric>
#include "new_order.h"
#include "pwv_graph.h"

namespace tpcc {

template <>
NewOrderStruct ClientBase::GenerateTransactionInput<NewOrderStruct>()
{
  NewOrderStruct s;
  s.warehouse_id = PickWarehouse();
  s.district_id = PickDistrict();
  s.customer_id = GetCustomerId();
  s.detail.nr_items = RandomNumber(5, NewOrderStruct::kNewOrderMaxItems);

  for (int i = 0; i < s.detail.nr_items; i++) {
 again:
    auto id = GetItemId();
    // Check duplicates. This is our customization to TPC-C because we cannot
    // handle duplicate keys.
    //
    // In practice, this should be handle by the client application.
    for (int j = 0; j < i; j++)
      if (s.detail.item_id[j] == id) goto again;

    s.detail.item_id[i] = id;
    s.detail.order_quantities[i] = RandomNumber(1, 10);
    if (nr_warehouses() == 1
        || RandomNumber(1, 100) > int(kNewOrderRemoteItem * 100)) {
      s.detail.supplier_warehouse_id[i] = s.warehouse_id;
    } else {
      s.detail.supplier_warehouse_id[i] =
          RandomNumberExcept(1, nr_warehouses(), s.warehouse_id);
    }
  }
  s.ts_now = GetCurrentTime();
  return s;
}

class NewOrderTxn : public Txn<NewOrderState>, public NewOrderStruct {
  Client *client;
 public:
  NewOrderTxn(Client *client, uint64_t serial_id)
      : Txn<NewOrderState>(serial_id),
        NewOrderStruct(client->GenerateTransactionInput<NewOrderStruct>()),
        client(client)
  {}
  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final;
};

void NewOrderTxn::PrepareInsert()
{
  auto &mgr = util::Instance<TableManager>();
  auto auto_inc_zone = warehouse_id * 10 + district_id;
  auto oorder_id = mgr.Get<tpcc::OOrder>().AutoIncrement(auto_inc_zone);

  auto oorder_key = OOrder::Key::New(warehouse_id, district_id, oorder_id);
  auto neworder_key = NewOrder::Key::New(warehouse_id, district_id, oorder_id, customer_id);
  auto cididx_key = OOrderCIdIdx::Key::New(warehouse_id, district_id, customer_id, oorder_id);
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

  auto args0 = Tuple<OrderDetail>(detail);
  auto args1 = OOrder::Value::New(customer_id, 0, nr_items,
                                  all_local, ts_now);



  if (g_tpcc_config.IsWarehousePinnable() || !VHandleSyncService::g_lock_elision) {
    if (VHandleSyncService::g_lock_elision) {
      // txn_indexop_affinity = g_tpcc_config.WarehouseToCoreId(warehouse_id);
      txn_indexop_affinity = std::numeric_limits<uint32_t>::max(); // magic to flatten out indexops
    }

    state->orderlines_nodes =
        TxnIndexInsert<TpccSliceRouter, NewOrderState::OrderLinesInsertCompletion, Tuple<OrderDetail>>(
            &args0,
            KeyParam<OrderLine>(orderline_keys, nr_items));

    state->other_inserts_nodes =
        TxnIndexInsert<TpccSliceRouter, NewOrderState::OtherInsertCompletion, OOrder::Value>(
            &args1,
            KeyParam<OOrder>(oorder_key),
            KeyParam<NewOrder>(neworder_key),
            KeyParam<OOrderCIdIdx>(cididx_key));

#if 0 // Hmm...I don't think we need to keep track of the inserts
    if (VHandleSyncService::g_lock_elision && Client::g_enable_pwv) {
      auto &gm = util::Instance<PWVGraphManager>();
      gm[txn_indexop_affinity]->ReserveEdge(serial_id(), 3);
      for (auto row: {state->orderlines[0], state->oorder, state->neworder}) {
        gm[txn_indexop_affinity]->AddResource(serial_id(), PWVGraph::VHandleToResource(row));
      }
    }
#endif

  } else {
    ASSERT_PWV_CONT;
    // int parts[3] = {
    //       g_tpcc_config.PWVDistrictToCoreId(district_id, 10),
    //       g_tpcc_config.PWVDistrictToCoreId(district_id, 40),
    //       g_tpcc_config.PWVDistrictToCoreId(district_id, 30),
    // };

    // txn_indexop_affinity = parts[0];

    txn_indexop_affinity = kIndexOpFlatten;
    state->orderlines_nodes =
        TxnIndexInsert<TpccSliceRouter, NewOrderState::OrderLinesInsertCompletion, Tuple<OrderDetail>>(
            &args0,
            KeyParam<OrderLine>(orderline_keys, nr_items));

    // txn_indexop_affinity = parts[1];
    state->other_inserts_nodes =
        TxnIndexInsert<TpccSliceRouter, NewOrderState::OtherInsertCompletion, OOrder::Value>(
            &args1,
            KeyParam<OOrder>(oorder_key),
            PlaceholderParam(),
            KeyParam<OOrderCIdIdx>(cididx_key));

    // txn_indexop_affinity = parts[2];
    state->other_inserts_nodes +=
        TxnIndexInsert<TpccSliceRouter, NewOrderState::OtherInsertCompletion, void>(
            nullptr,
            PlaceholderParam(),
            KeyParam<NewOrder>(neworder_key));

#if 0
    if (Client::g_enable_pwv) {
      auto &gm = util::Instance<PWVGraphManager>();
      for (auto part_id: parts) {
        gm[part_id]->ReserveEdge(serial_id());
      }
      gm[parts[0]]->AddResource(handle.serial_id(),
                                PWVGraph::VHandleToResource(state->orderlines[0]));
      gm[parts[1]]->AddResource(handle.serial_id(),
                                PWVGraph::VHandleToResource(state->oorder));
      gm[parts[2]]->AddResource(handle.serial_id(),
                                PWVGraph::VHandleToResource(state->neworder));
    }
#endif

  }
}

void NewOrderTxn::Prepare()
{
  Stock::Key stock_keys[kNewOrderMaxItems];
  INIT_ROUTINE_BRK(8192);

  if (!VHandleSyncService::g_lock_elision) {
    auto nr_items = detail.nr_items;
    for (int i = 0; i < nr_items; i++) {
      stock_keys[i] =
          Stock::Key::New(detail.supplier_warehouse_id[i], detail.item_id[i]);
    }

    // abort_if(nr_items < 5, "WTF {}", nr_items);
    state->stocks_nodes =
        TxnIndexLookup<TpccSliceRouter, NewOrderState::StocksLookupCompletion, void>(
            nullptr,
            KeyParam<Stock>(stock_keys, nr_items));

    if (g_tpcc_config.IsWarehousePinnable()) {
      root->AssignAffinity(g_tpcc_config.WarehouseToCoreId(warehouse_id));
    }
  } else {
    // PWV partitions the initialization phase! We don't support multiple-nodes
    // under PWV, because we don't want to keep a per-core partitioning result
    // in NodeBitmap.
    unsigned int w = 0;
    int istart = 0, iend = 0;
    uint32_t picked = 0;
    // int *unique_warehouses;
    int nr_unique_warehouses = 0;

    // if (Client::g_enable_pwv) unique_warehouses = alloca(sizeof(int) * 16);

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

      if (Client::g_enable_pwv) {
        auto &gm = util::Instance<PWVGraphManager>();
        gm[w - 1]->ReserveEdge(serial_id());
        gm[w - 1]->AddResource(serial_id(), &ClientBase::g_pwv_stock_resources[w - 1]);
      }
      nr_unique_warehouses++;

      auto args = Tuple<int>(picked ^ old_bitmap);
      TxnIndexLookup<TpccSliceRouter, NewOrderState::StocksLookupCompletion, Tuple<int>>(
          &args,
          KeyParam<Stock>(stock_keys + istart, iend - istart));

      w = 0;
      istart = iend;
    }
    abort_if(iend != detail.nr_items, "Bug in NewOrder Prepare() Bohm partitioning");
    state->stocks_nodes.MergeOrAdd(1, (1 << detail.nr_items) - 1);
  }
}

void NewOrderTxn::Run()
{
  int nr_nodes = util::Instance<NodeConfiguration>().nr_nodes();

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

#if 0
  for (auto &p: state->other_inserts_nodes) {
    auto [node, bitmap] = p;

    if (bitmap & (1 << 0)) {
      UpdateForKey(
          &state->oorder_future, node, state->oorder,
          [](const auto &ctx, VHandle *row) -> VHandle * {
            auto &[state, index_handle, customer_id, nr_items, ts_now,
                   all_local] = ctx;
            index_handle(row).WriteTryInline(
                OOrder::Value::New(customer_id, 0, nr_items,
                                   all_local, ts_now));
            ClientBase::OnUpdateRow(row);
            return row;
          },
          customer_id, nr_items, ts_now, all_local);
    }

    if (bitmap & (1 << 1)) {
      UpdateForKey(
          &state->neworder_future, node, state->neworder,
          [](const auto &ctx, VHandle *row) -> VHandle * {
            auto &[state, index_handle] = ctx;
            index_handle(row).WriteTryInline(NewOrder::Value());
            ClientBase::OnUpdateRow(row);
            return row;
          });
    }

    root->AttachRoutine(
        MakeContext(bitmap, customer_id, nr_items, ts_now, all_local), node,
        [](const auto &ctx) {
          auto &[state, index_handle, bitmap, customer_id, nr_items, ts_now,
                 all_local] = ctx;
          probes::TpccNewOrder{0, __builtin_popcount(bitmap)}();

          if (bitmap & 0x01) {
            state->oorder_future.Invoke(&state, index_handle);
          }

          if (bitmap & 0x02) {
            state->neworder_future.Invoke(&state, index_handle);
          }

          if (bitmap & 0x04) {
            index_handle(state->cididx).WriteTryInline(OOrderCIdIdx::Value());
            ClientBase::OnUpdateRow(state->cididx);
          }
        });
  }

  for (auto &p: state->orderlines_nodes) {
    auto [node, bitmap] = p;

    auto aff = std::numeric_limits<uint64_t>::max();
    if (!Client::g_enable_granola) {
      aff = AffinityFromRows(bitmap, state->orderlines);
    }

    root->AttachRoutine(
        MakeContext(bitmap, detail), node,
        [](const auto &ctx) {
          auto &[state, index_handle, bitmap, detail] = ctx;
          auto &mgr = util::Instance<TableManager>();

          void *buf = alloca(16);
          for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
            if ((bitmap & (1 << i)) == 0) continue;

            auto item = mgr.Get<Item>().Search(Item::Key::New(detail.item_id[i]).EncodeView(buf));
            auto item_value = index_handle(item).template Read<Item::Value>();
            auto amount = item_value.i_price * detail.order_quantities[i];

            probes::TpccNewOrder{1, 1}();

            index_handle(state->orderlines[i])
                .WriteTryInline(OrderLine::Value::New(detail.item_id[i], 0, amount,
                                                      detail.supplier_warehouse_id[i],
                                                      detail.order_quantities[i]));
            ClientBase::OnUpdateRow(state->orderlines[i]);
          }
        },
        aff);
  }
#endif

  for (auto &p: state->stocks_nodes) {
    auto [node, bitmap] = p;

    if (!Options::kEnablePartition) {
      auto &conf = util::Instance<NodeConfiguration>();
      if (node == conf.node_id()) {
        // Local
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
      } else {
        // Remote piece
        root->AttachRoutine(
            MakeContext(bitmap, params), node,
            [](const auto &ctx) {
              auto &[state, index_handle, bitmap, params] = ctx;
              for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
                if ((bitmap & (1 << i)) == 0) continue;

                TxnRow vhandle = index_handle(state->stocks[i]);
                auto stock = vhandle.Read<Stock::Value>();

                probes::TpccNewOrder{2, 1}();

                if (stock.s_quantity - params.quantities[i] < 10) {
                  stock.s_quantity += 91;
                }
                stock.s_quantity -= params.quantities[i];
                stock.s_ytd += params.quantities[i];
                stock.s_remote_cnt += (params.supplier_warehouses[i] != params.warehouse) ? 1 : 0;

                vhandle.Write(stock);
              }
            });
      }

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

        root->AttachRoutine(
            MakeContext(bitmap, params, w), node,
            [](const auto &ctx) {
              auto &[state, index_handle, bitmap, params, w] = ctx;

              for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
                if ((bitmap & (1 << i)) == 0) continue;
                if (w > 0 && params.supplier_warehouses[i] != w) continue;

                debug(DBG_WORKLOAD "Txn {} updating its {} row {}",
                      index_handle.serial_id(), i, (void *) state->stocks[i]);

                TxnRow vhandle = index_handle(state->stocks[i]);
                auto stock = vhandle.Read<Stock::Value>();

                probes::TpccNewOrder{2, 1}();

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

              if (Client::g_enable_pwv) {
                auto g = util::Instance<PWVGraphManager>().local_graph();
                g->ActivateResource(
                    index_handle.serial_id(), &ClientBase::g_pwv_stock_resources[w - 1]);
              }
            },
            aff);
      }
    }
  }
}

}

namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::NewOrder), Client *, uint64_t>::Construct(tpcc::Client * client, uint64_t serial_id)
{
  return new tpcc::NewOrderTxn(client, serial_id);
}

}


#if 0
struct NodePathAggregator {
  struct NodePath {
    short nr;
    short index[NewOrderStruct::kNewOrderMaxItems];
    int code;
  } *paths;
  NodePath **htable;
  int nr_paths;
  int htable_size;

  struct Path {
    NodePath paths[NewOrderStruct::kNewOrderMaxItems];
    NodePath *htable[];

    static size_t StructSize(int htable_size) {
      return sizeof(Path) + sizeof(NodePath *) * htable_size;
    }
  };

  NodePathAggregator(Path *p, int htable_size)
      : paths(p->paths), htable(p->htable), nr_paths(0), htable_size(htable_size) {
    memset(htable, 0, htable_size * sizeof(NodePath *));
  }

  NodePathAggregator& operator+=(const std::tuple<int, int> &rhs) {
    auto [index, code] = rhs;
    NodePath *p = htable[code];
    if (!p) {
      p = &paths[nr_paths++];
      p->nr = 0;
      p->code = code;
      htable[code] = p;
    }
    p->index[p->nr++] = index;
    return *this;
  }

  NodePath *begin() {
    return paths;
  }

  NodePath *end() {
    return paths + nr_paths;
  }
};
#endif
