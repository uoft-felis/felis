#include <numeric>
#include "new_order.h"

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
  void PrepareState() override final {
    Txn<NewOrderState>::PrepareState();
    client->get_insert_locality_manager().PlanLoad(warehouse_id - 1, detail.nr_items);
    client->get_initialization_locality_manager().PlanLoad(warehouse_id - 1, 5);
  }
  void Run() override final;
  void Prepare() override final {
    if (!Client::g_enable_granola)
      PrepareImpl();
  }
  void PrepareInsert() override final {
    if (!Client::g_enable_granola)
      PrepareInsertImpl();
  }

  void PrepareImpl();
  void PrepareInsertImpl();
};

void NewOrderTxn::PrepareInsertImpl()
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

  auto handle = index_handle();

  for (int i = 0; i < nr_items; i++) {
    auto item = mgr.Get<Item>().Search(Item::Key::New(detail.item_id[i]).EncodeFromRoutine());
    auto item_value = handle(item).Read<Item::Value>();
    detail.unit_price[i] = item_value.i_price;
  }

  auto args0 = Tuple<OrderDetail>(detail);
  state->orderlines_nodes =
      TxnIndexInsert<TpccSliceRouter, NewOrderState::OrderLinesInsertCompletion, Tuple<OrderDetail>>(
          &args0,
          KeyParam<OrderLine>(orderline_keys, nr_items));

  auto args1 = OOrder::Value::New(customer_id, 0, nr_items,
                                  all_local, ts_now);

  state->other_inserts_nodes =
      TxnIndexInsert<TpccSliceRouter, NewOrderState::OtherInsertCompletion, OOrder::Value>(
          &args1,
          KeyParam<OOrder>(oorder_key),
          KeyParam<NewOrder>(neworder_key),
          KeyParam<OOrderCIdIdx>(cididx_key));

  if (g_tpcc_config.nr_warehouses != 1) {
    state->insert_aff = client->get_insert_locality_manager().GetScheduleCore(
        Config::WarehouseToCoreId(warehouse_id));
    root->AssignAffinity(state->insert_aff);
  }
}

void NewOrderTxn::PrepareImpl()
{
  Stock::Key stock_keys[kNewOrderMaxItems];
  auto nr_items = detail.nr_items;
  for (int i = 0; i < nr_items; i++) {
    stock_keys[i] =
        Stock::Key::New(detail.supplier_warehouse_id[i], detail.item_id[i]);
  }

  INIT_ROUTINE_BRK(8192);

  // abort_if(nr_items < 5, "WTF {}", nr_items);
  state->stocks_nodes =
      TxnIndexLookup<TpccSliceRouter, NewOrderState::StocksLookupCompletion, void>(
          nullptr,
          KeyParam<Stock>(stock_keys, nr_items));

  if (g_tpcc_config.nr_warehouses != 1) {
    state->initialize_aff = client->get_initialization_locality_manager().GetScheduleCore(
        Config::WarehouseToCoreId(warehouse_id));
    root->AssignAffinity(state->initialize_aff);
  }
}

void NewOrderTxn::Run()
{
  if (Client::g_enable_granola) {
    // Search the index. AppendNewVersion() is automatically Nop when
    // g_enable_granola is on.
    PrepareInsertImpl();
    PrepareImpl();
  }

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

    auto aff = std::numeric_limits<uint64_t>::max();
    if (!Client::g_enable_granola && g_tpcc_config.nr_warehouses != 1) {
      aff = AffinityFromRows(bitmap, {state->oorder, state->neworder, state->cididx});
    }

    root->Then(
        MakeContext(bitmap, customer_id, nr_items, ts_now, all_local), node,
        [](const auto &ctx, auto args) -> Optional<VoidValue> {
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

          return nullopt;
        },
        aff);
  }

  for (auto &p: state->orderlines_nodes) {
    auto [node, bitmap] = p;

    auto aff = std::numeric_limits<uint64_t>::max();
    if (!Client::g_enable_granola) {
      aff = AffinityFromRows(bitmap, state->orderlines);
    }

    root->Then(
        MakeContext(bitmap, detail), node,
        [](const auto &ctx, auto args) -> Optional<VoidValue> {
          auto &[state, index_handle, bitmap, detail] = ctx;
          auto &mgr = util::Instance<TableManager>();

          INIT_ROUTINE_BRK(4096);

          for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
            if ((bitmap & (1 << i)) == 0) continue;

            auto item = mgr.Get<Item>().Search(Item::Key::New(detail.item_id[i]).EncodeFromRoutine());
            auto item_value = index_handle(item).template Read<Item::Value>();
            auto amount = item_value.i_price * detail.order_quantities[i];

            probes::TpccNewOrder{1, 1}();

            index_handle(state->orderlines[i])
                .WriteTryInline(OrderLine::Value::New(detail.item_id[i], 0, amount,
                                                      detail.supplier_warehouse_id[i],
                                                      detail.order_quantities[i]));
            ClientBase::OnUpdateRow(state->orderlines[i]);
          }

          return nullopt;
        },
        aff);
  }
#endif

  for (auto &p: state->stocks_nodes) {
    auto [node, bitmap] = p;

    if (!Client::g_enable_granola) {
      auto &conf = util::Instance<NodeConfiguration>();
      if (node == conf.node_id()) {
        // Local
        for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
          if ((bitmap & (1 << i)) == 0) continue;

          UpdateForKey(
              &state->stock_futures[i], node, state->stocks[i],
              [](const auto &ctx, VHandle *row) -> VHandle * {
                auto &[state, index_handle, quantity, remote, i] = ctx;
                debug(DBG_WORKLOAD "Txn {} updating its {} row {}",
                      index_handle.serial_id(), i, (void *) row);

                TxnVHandle vhandle = index_handle(row);
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

                return row;
              },
              params.quantities[i],
              (bool) (params.supplier_warehouses[i] != warehouse_id),
              i);
        }

        auto aff = std::numeric_limits<uint64_t>::max();

        if (g_tpcc_config.nr_warehouses != 1) {
          aff = state->initialize_aff;
        }

        root->Then(
            MakeContext(bitmap), node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, bitmap] = ctx;
              for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
                if ((bitmap & (1 << i)) == 0) continue;

                state->stock_futures[i].Invoke(&state, index_handle);
              }
              return nullopt;
            }, aff);
      } else {
        // Remote piece
        root->Then(
            MakeContext(bitmap, params), node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, bitmap, params] = ctx;
              for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
                if ((bitmap & (1 << i)) == 0) continue;

                TxnVHandle vhandle = index_handle(state->stocks[i]);
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
              return nullopt;
            });
      }

    } else {
      // Granola needs partitioning, that's why we need to split this into
      // multiple pieces even if this piece could totally be executed on the same
      // node.
      std::array<int, NewOrderStruct::kNewOrderMaxItems> warehouse_filters;
      int nr_warehouse_filters = 0;
      warehouse_filters.fill(0);

      for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
        if ((bitmap & (1 << i)) == 0) continue;

        auto supp_warehouse = detail.supplier_warehouse_id[i];
        if (std::find(warehouse_filters.begin(), warehouse_filters.begin() + nr_warehouse_filters,
                      supp_warehouse) == warehouse_filters.begin() + nr_warehouse_filters) {
          warehouse_filters[nr_warehouse_filters++] = supp_warehouse;
        }
      }
      for (int f = 0; f < nr_warehouse_filters; f++) {
        auto filter = warehouse_filters[f];
        auto aff = std::numeric_limits<uint64_t>::max();

        aff = filter - 1;

        root->Then(
            MakeContext(bitmap, params, filter), node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, bitmap, params, filter] = ctx;

              for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
                if ((bitmap & (1 << i)) == 0) continue;
                if (filter > 0 && params.supplier_warehouses[i] != filter) continue;

                debug(DBG_WORKLOAD "Txn {} updating its {} row {}",
                      index_handle.serial_id(), i, (void *) state->stocks[i]);

                TxnVHandle vhandle = index_handle(state->stocks[i]);
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
              return nullopt;
            },
            aff);
      }
    }
  }

  if (Client::g_enable_granola)
    root->AssignAffinity(warehouse_id - 1);
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
