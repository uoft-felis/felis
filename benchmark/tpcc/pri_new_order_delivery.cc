#include <numeric>
#include "pri_new_order_delivery.h"

namespace tpcc {

class PriNewOrderDeliveryTxn : public Txn<PriNewOrderDeliveryState>, public NewOrderStruct {
  Client *client;
 public:
  PriNewOrderDeliveryTxn(Client *client, uint64_t serial_id)
      : Txn<PriNewOrderDeliveryState>(serial_id),
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

void PriNewOrderDeliveryTxn::PrepareInsertImpl()
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
      TxnIndexInsert<TpccSliceRouter, PriNewOrderDeliveryState::OrderLinesInsertCompletion, void>(
          nullptr,
          KeyParam<OrderLine>(orderline_keys, nr_items));

  state->other_inserts_nodes =
      TxnIndexInsert<TpccSliceRouter, PriNewOrderDeliveryState::OtherInsertCompletion, void>(
          nullptr,
          KeyParam<OOrder>(oorder_key),
          KeyParam<NewOrder>(neworder_key));
}

void PriNewOrderDeliveryTxn::PrepareImpl()
{
  Stock::Key stock_keys[kNewOrderMaxItems];
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
}

void PriNewOrderDeliveryTxn::Run()
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
    root->Then(
        MakeContext(bitmap, customer_id, nr_items, ts_now, all_local), node,
        [](const auto &ctx, auto args) -> Optional<VoidValue> {
          auto &[state, index_handle, bitmap, customer_id, nr_items, ts_now,
                 all_local] = ctx;
          if (bitmap & 0x01) {
            auto oorder = OOrder::Value::New(customer_id, 0, nr_items,
                                            all_local, ts_now); // new_order
            oorder.o_carrier_id = __rdtsc() % 10 + 1; // delivery; random between 1 and 10
            index_handle(state->oorder).Write(oorder);
            ClientBase::OnUpdateRow(state->oorder);
          }

          if (bitmap & 0x02) {
            auto neworder = NewOrder::Value(); // new_order; generate, but hold the write
            index_handle(state->neworder).Delete(); // delivery
            ClientBase::OnUpdateRow(state->neworder);
          }

          return nullopt;
        });
  }

  for (auto &p: state->orderlines_nodes) {
    auto [node, bitmap] = p;
    root->Then(
        MakeContext(bitmap, detail), node,
        [](const auto &ctx, auto args) -> Optional<VoidValue> {
          auto &[state, index_handle, bitmap, detail] = ctx;
          auto &mgr = util::Instance<RelationManager>();
          int sum = 0;

          INIT_ROUTINE_BRK(4096);

          for (int i = 0; i < NewOrderStruct::kNewOrderMaxItems; i++) {
            if ((bitmap & (1 << i)) == 0) continue;

            auto item = mgr.Get<Item>().Search(Item::Key::New(detail.item_id[i]).EncodeFromRoutine());
            auto item_value = index_handle(item).template Read<Item::Value>();
            auto amount = item_value.i_price * detail.order_quantities[i];

            auto orderline = OrderLine::Value::New(detail.item_id[i], 0, amount,
                                                   detail.supplier_warehouse_id[i],
                                                   detail.order_quantities[i]);
            sum += orderline.ol_amount;
            orderline.ol_delivery_d = 234567;
            index_handle(state->orderlines[i]).Write(orderline);
            ClientBase::OnUpdateRow(state->orderlines[i]);
          }

          // well, orderline and customer should be in the same warehouse,
          // so I just use the same node. may not work for random sharding or stuff
          auto customer = index_handle(state->customer).template Read<Customer::Value>();
          customer.c_balance = sum;
          index_handle(state->customer).Write(customer);
          ClientBase::OnUpdateRow(state->customer);
          return nullopt;
        });
  }

  if (Client::g_enable_granola)
    root_promise()->AssignAffinity(warehouse_id - 1);
}

} // namespace tpcc

namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::PriNewOrderDelivery), Client *, uint64_t>::Construct(tpcc::Client * client, uint64_t serial_id)
{
  return new tpcc::PriNewOrderDeliveryTxn(client, serial_id);
}

}
