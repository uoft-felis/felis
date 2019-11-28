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
  s.nr_items = RandomNumber(5, NewOrderStruct::kNewOrderMaxItems);

  for (int i = 0; i < s.nr_items; i++) {
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
  auto auto_inc_zone = warehouse_id * 10 + district_id;
  auto oorder_id = client->relation(OOrder::kTable).AutoIncrement(auto_inc_zone);

  auto oorder_key = OOrder::Key::New(warehouse_id, district_id, oorder_id);
  auto neworder_key = NewOrder::Key::New(warehouse_id, district_id, oorder_id, customer_id);
  OrderLine::Key orderline_keys[kNewOrderMaxItems];
  for (int i = 0; i < nr_items; i++) {
    orderline_keys[i] = OrderLine::Key::New(warehouse_id, district_id, oorder_id, i + 1);
  }

  INIT_ROUTINE_BRK(8192);

  state->orderlines_nodes =
      TxnIndexInsert<TpccSliceRouter, NewOrderState::OrderLinesInsertCompletion, void>(
          nullptr,
          KeyParam<OrderLine>(orderline_keys, nr_items));

  state->other_inserts_nodes =
      TxnIndexInsert<TpccSliceRouter, NewOrderState::OtherInsertCompletion, void>(
          nullptr,
          KeyParam<OOrder>(oorder_key),
          KeyParam<NewOrder>(neworder_key));
}

void NewOrderTxn::PrepareImpl()
{
  Stock::Key stock_keys[kNewOrderMaxItems];
  Item::Key item_keys[kNewOrderMaxItems];
  for (int i = 0; i < nr_items; i++) {
    stock_keys[i] =
        Stock::Key::New(detail.supplier_warehouse_id[i], detail.item_id[i]);
    item_keys[i] = Item::Key::New(detail.item_id[i]);
  }

  INIT_ROUTINE_BRK(8192);

  // abort_if(nr_items < 5, "WTF {}", nr_items);

  state->stocks_nodes =
      TxnIndexLookup<TpccSliceRouter, NewOrderState::StocksLookupCompletion, void>(
          nullptr,
          KeyParam<Stock>(stock_keys, nr_items));

  state->items_nodes =
      TxnIndexLookup<TpccSliceRouter, NewOrderState::ItemsLookupCompletion, void>(
          nullptr,
          KeyParam<Item>(item_keys, nr_items));
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
  bool all_local = true;
  for (auto i = 0; i < nr_items; i++) {
    params.quantities[i] = detail.order_quantities[i];
    params.supplier_warehouses[i] = detail.supplier_warehouse_id[i];
    if (detail.supplier_warehouse_id[i] != warehouse_id)
      all_local = false;
  }

  for (auto &p: state->stocks_nodes) {
    auto [node, bitmap] = p;

    // Granola needs partitioning, that's why we need to split this into
    // multiple pieces even if this piece could totally be executed on the same
    // node.
    std::array<int, NewOrderStruct::kNewOrderMaxItems> warehouse_filters;
    int nr_warehouse_filters = 0;
    warehouse_filters.fill(0);

    if (!Client::g_enable_granola) {
      warehouse_filters[nr_warehouse_filters++] = -1; // Don't filter
    } else {
      for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
        if ((bitmap & (1 << i)) == 0) continue;

        auto supp_warehouse = detail.supplier_warehouse_id[i];
        if (std::find(warehouse_filters.begin(), warehouse_filters.begin() + nr_warehouse_filters,
                      supp_warehouse) == warehouse_filters.begin() + nr_warehouse_filters) {
          warehouse_filters[nr_warehouse_filters++] = supp_warehouse;
        }
      }
    }

    auto root = proc.promise();
    for (int f = 0; f < nr_warehouse_filters; f++) {
      auto filter = warehouse_filters[f];
      auto aff = std::numeric_limits<uint64_t>::max();

      if (filter > 0) aff = filter - 1;

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

  for (auto &p: state->other_inserts_nodes) {
    auto [node, bitmap] = p;

    proc |
        TxnProc(node,
                [](const auto &ctx, auto args) -> Optional<VoidValue> {
                  auto &[state, index_handle, bitmap, customer_id, nr_items, ts_now,
                         all_local] = ctx;
                  if (bitmap & 0x01) {
                    index_handle(state->oorder)
                        .Write(OOrder::Value::New(customer_id, 0, nr_items,
                                                  all_local, ts_now));
                    ClientBase::OnUpdateRow(state->oorder);
                  }

                  if (bitmap & 0x02) {
                    index_handle(state->neworder).Write(NewOrder::Value());
                    ClientBase::OnUpdateRow(state->neworder);
                  }

                  return nullopt;
                },
                bitmap, customer_id, nr_items, ts_now, all_local);
  }

  for (auto &p: state->orderlines_nodes) {
    auto [node, bitmap] = p;
    proc
        | TxnProc(
            node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, bitmap, detail] = ctx;

              for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
                if ((bitmap & (1 << i)) == 0) continue;

                auto item_value = index_handle(state->items[i])
                                  .template Read<Item::Value>();
                auto amount = item_value.i_price * detail.order_quantities[i];

                index_handle(state->orderlines[i])
                    .Write(OrderLine::Value::New(detail.item_id[i], 0, amount,
                                                 detail.supplier_warehouse_id[i],
                                                 detail.order_quantities[i]));
                ClientBase::OnUpdateRow(state->orderlines[i]);
              }

              return nullopt;
            },
            bitmap, detail);
  }
  if (Client::g_enable_granola)
    root_promise()->AssignAffinity(warehouse_id - 1);
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
