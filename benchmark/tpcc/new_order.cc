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
  s.new_order_id = PickNewOrderId(s.warehouse_id, s.district_id);

  for (int i = 0; i < s.nr_items; i++) {
 again:
    auto id = GetItemId();
    // Check duplicates. This is our customization to TPC-C because we cannot
    // handle duplicate keys.
    //
    // In practice, this should be handle by the client application.
    for (int j = 0; j < i; j++)
      if (s.item_id[j] == id) goto again;

    s.item_id[i] = id;
    s.order_quantities[i] = RandomNumber(1, 10);
    if (nr_warehouses() == 1
        || RandomNumber(1, 100) > int(kNewOrderRemoteItem * 100)) {
      s.supplier_warehouse_id[i] = s.warehouse_id;
    } else {
      s.supplier_warehouse_id[i] =
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

void NewOrderTxn::PrepareInsert()
{
  auto node = Client::warehouse_to_node_id(warehouse_id);
  auto auto_inc_zone = warehouse_id * 10 + district_id;

  auto oorder_id = client->relation(OOrder::kTable).AutoIncrement(auto_inc_zone);
  auto oorder_key = OOrder::Key::New(warehouse_id, district_id, oorder_id);
  auto oorder_value = CreateNewRow<OOrder>(oorder_key);
  auto neworder_key = NewOrder::Key::New(warehouse_id, district_id, oorder_id, customer_id);
  auto neworder_value = CreateNewRow<NewOrder>(neworder_key);
  state->rows.oorder = oorder_value;
  state->rows.neworder = neworder_value;

  oorder_value->AppendNewVersion(serial_id(), epoch_nr());
  neworder_value->AppendNewVersion(serial_id(), epoch_nr());

  OrderLine::Key orderline_keys[kNewOrderMaxItems];
  VHandle *orderline_values[kNewOrderMaxItems];
  for (int i = 0; i < nr_items; i++) {
    orderline_keys[i] = OrderLine::Key::New(warehouse_id, district_id, oorder_id, i + 1);
    orderline_values[i] = CreateNewRow<OrderLine>(orderline_keys[i]);
    orderline_values[i]->AppendNewVersion(serial_id(), epoch_nr());
    state->rows.orderlines[i] = orderline_values[i];
  }

  INIT_ROUTINE_BRK(4096);

  proc
      | TxnInsert<OrderLine>(
          node,
          orderline_keys,
          orderline_keys + nr_items,
          orderline_values);

  proc
      | TxnInsertOne<OOrder>(
          node,
          oorder_key,
          oorder_value);

  proc
      | TxnInsertOne<NewOrder>(
          node,
          neworder_key,
          neworder_value);

}

void NewOrderTxn::Prepare()
{
  INIT_ROUTINE_BRK(4096);

  auto district_key = District::Key::New(warehouse_id, district_id);
  auto warehouse_key = Warehouse::Key::New(warehouse_id);

  int nr_nodes = util::Instance<NodeConfiguration>().nr_nodes();
  int lookup_node = client->warehouse_to_lookup_node_id(warehouse_id);
  int node = Client::warehouse_to_node_id(warehouse_id);

  NodePathAggregator agg(
      new (alloca(NodePathAggregator::Path::StructSize(nr_nodes * nr_nodes))) NodePathAggregator::Path,
      nr_nodes * nr_nodes);

  for (int i = 0; i < nr_items; i++) {
    int s_wh = supplier_warehouse_id[i];
    int node = Client::warehouse_to_node_id(s_wh);
    int lookup_node = client->warehouse_to_lookup_node_id(s_wh);
    int code = node - 1 + (lookup_node - 1) * nr_nodes;
    agg += { i, code };
  }

  Stock::Key stock_keys[kNewOrderMaxItems];
  Item::Key item_keys[kNewOrderMaxItems];
  for (int i = 0; i < nr_items; i++) {
    stock_keys[i] = Stock::Key::New(supplier_warehouse_id[i], item_id[i]);
    item_keys[i] = Item::Key::New(item_id[i]);
  }

  for (auto &p: agg) {
    int lookup_node = p.code % nr_nodes + 1;
    int node = p.code / nr_nodes + 1;
    uint16_t bitmap = 0;

    for (auto i = 0; i < p.nr; i++) {
      bitmap |= (1 << p.index[i]);
    }

    proc
        | TxnLookupMany<Stock>(
            lookup_node,
            stock_keys, stock_keys + nr_items,
            bitmap)
        | TxnAppendVersion(
            node,
            [](const auto &ctx, auto *handle, int i) {
              auto &[state, _1, _2] = ctx;
              state->rows.stocks[i] = handle;
            });
  }

  proc
      | TxnLookup<Warehouse>(
          lookup_node,
          warehouse_key);

  proc
      | TxnLookup<District>(
          lookup_node,
          district_key);

  proc
      | TxnLookupMany<Item>(
          lookup_node,
          item_keys, item_keys + nr_items)
      | TxnProc(
          node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle] = ctx;
            auto [handles, bitmap] = args;
            for (auto i = 0; i < TxnIndexOpContext::kMaxPackedKeys; i++) {
              if ((bitmap & (1 << i)) == 0) continue;
              state->rows.items[i] = handles[i];
            }
            return nullopt;
          });
}

void NewOrderTxn::Run()
{
  int nr_nodes = util::Instance<NodeConfiguration>().nr_nodes();
  NodePathAggregator agg(
      new (alloca(NodePathAggregator::Path::StructSize(nr_nodes))) NodePathAggregator::Path,
      nr_nodes);

  struct {
    uint quantities[NewOrderStruct::kNewOrderMaxItems];
    uint remote_bitmap;
  } params;

  bool all_local = true;

  for (auto i = 0; i < nr_items; i++) {
    int node = Client::warehouse_to_node_id(supplier_warehouse_id[i]);
    agg += { i, node - 1 };
    params.quantities[i] = order_quantities[i];
    int remote = (supplier_warehouse_id[i] == warehouse_id) ? 0 : 1;
    params.remote_bitmap |= (remote << i);
    if (remote) all_local = false;
  }

  for (auto &p: agg) {
    int node = p.code + 1;
    proc
        | TxnProc(
            node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle,
                     p, params] = ctx;

              for (int t = 0; t < p.nr; t++) {
                util::Prefetch({state->rows.stocks[p.index[t]]});
              }
              for (int t = 0; t < p.nr; t++) {
                int i = p.index[t];

                logger->debug("Txn {} updating its {} row {}",
                              index_handle.serial_id(), i, (void *) state->rows.stocks[i]);

                TxnVHandle vhandle = index_handle(state->rows.stocks[i]);
                auto stock = vhandle.Read<Stock::Value>();
                if (stock.s_quantity - params.quantities[i] < 10) {
                  stock.s_quantity += 91;
                }
                stock.s_quantity -= params.quantities[i];
                stock.s_ytd += params.quantities[i];
                stock.s_remote_cnt += (params.remote_bitmap & (1 << i)) ? 1 : 0;

                vhandle.Write(stock);
                ClientBase::OnUpdateRow(state->rows.stocks[i]);
                logger->debug("Txn {} updated its {} row {}",
                              index_handle.serial_id(), i,
                              (void *)state->rows.stocks[i]);
              }
              return nullopt;
            }, p, params);
  }

  auto node = Client::warehouse_to_node_id(warehouse_id);

  proc
      | TxnProc(
          node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle, customer_id, nr_items, ts_now, all_local] = ctx;
            index_handle(state->rows.neworder)
                .Write(NewOrder::Value());
            ClientBase::OnUpdateRow(state->rows.neworder);

            index_handle(state->rows.oorder)
                .Write(OOrder::Value::New(
                    customer_id, 0, nr_items, all_local, ts_now));
            ClientBase::OnUpdateRow(state->rows.oorder);
            return nullopt;
          },
          customer_id, nr_items, ts_now, all_local);

  proc
      | TxnProc(
          node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle, nr_items, supplier_warehouses, quantities, items] = ctx;
            util::Prefetch(state->rows.items, state->rows.items + nr_items);

            for (int i = 0; i < nr_items; i++) {
              auto item_value = index_handle(state->rows.items[i])
                                    .template Read<Item::Value>();
              auto amount = item_value.i_price * quantities[i];

              index_handle(state->rows.orderlines[i])
                  .Write(OrderLine::Value::New(items[i], 0, amount,
                                               supplier_warehouses[i],
                                               quantities[i]));
              ClientBase::OnUpdateRow(state->rows.orderlines[i]);
            }

            return nullopt;
          },
          nr_items, supplier_warehouse_id, order_quantities, item_id);
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
