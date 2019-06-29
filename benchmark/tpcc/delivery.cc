#include "delivery.h"

namespace tpcc {

static int g_last_no_o_ids[10] = {
  2100, 2100, 2100, 2100, 2100, 2100, 2100, 2100, 2100, 2100,
};

template <>
DeliveryStruct ClientBase::GenerateTransactionInput<DeliveryStruct>()
{
  DeliveryStruct s;
  s.warehouse_id = PickWarehouse();
  s.o_carrier_id = PickDistrict();
  s.ts = GetCurrentTime();
  auto &conf = util::Instance<NodeConfiguration>();

  for (int i = 0; i < kTPCCConfig.districts_per_warehouse; i++) {
    s.last_no_o_ids[i] = (g_last_no_o_ids[i] << 8) + (conf.node_id() & 0x00FF);
    g_last_no_o_ids[i]++;
  }
  return s;
}

class DeliveryTxn : public Txn<DeliveryState>, public DeliveryStruct {
  Client *client;
 public:
  DeliveryTxn(Client *client, uint64_t serial_id)
      : Txn<DeliveryState>(serial_id),
        DeliveryStruct(client->GenerateTransactionInput<DeliveryStruct>()),
        client(client)
  {}
  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final {}
};

void DeliveryTxn::Prepare()
{
  INIT_ROUTINE_BRK(16384);
  for (int i = 0; i < kTPCCConfig.districts_per_warehouse; i++) {
    auto district_id = i + 1;
    auto oid = last_no_o_ids[i];
    auto &mgr = util::Instance<RelationManager>();

    auto neworder_start = NewOrder::Key::New(
        warehouse_id, district_id, oid, 0);
    auto neworder_end = NewOrder::Key::New(
        warehouse_id, district_id, oid, std::numeric_limits<int>::max());
    auto &neworder_table = mgr.Get<NewOrder>();
    auto no_it = neworder_table.IndexSearchIterator(
        neworder_start.EncodeFromRoutine(), neworder_end.EncodeFromRoutine());
    if (!no_it.IsValid()) {
      state->nodes[i] = NodeBitmap();
      continue;
    }

    auto customer_id = no_it.key().template ToType<NewOrder::Key>().no_c_id;
    auto orderline_start = OrderLine::Key::New(
        warehouse_id, district_id, oid, 0);
    auto orderline_end = OrderLine::Key::New(
        warehouse_id, district_id, oid, 16);
    auto oorder_key = OOrder::Key::New(warehouse_id, district_id, oid);
    auto customer_key = Customer::Key::New(
        warehouse_id, district_id, customer_id);
    auto dest_neworder_key = NewOrder::Key::New(
        warehouse_id, district_id, oid, customer_id);

    auto args = Tuple<int>(i);
    state->nodes[i] =
        TxnIndexLookup<DeliveryState::Completion, Tuple<int>>(
            tpcc::SliceRouter,
            &args,
            RangeParam<OrderLine>(orderline_start, orderline_end),
            KeyParam<OOrder>(oorder_key),
            KeyParam<Customer>(customer_key),
            KeyParam<NewOrder>(dest_neworder_key));
  }
}

void DeliveryTxn::Run()
{
  for (int i = 0; i < 10; i++) {
    int16_t sum_node = -1, customer_node = -1;
    for (auto &p: state->nodes[i]) {
      auto [node, bitmap] = p;
      if (bitmap & 0x01) sum_node = node;
      if (bitmap & 0x08) customer_node = node;
    }
    for (auto &p: state->nodes[i]) {
      auto [node, bitmap] = p;
      // if (bitmap == 0x08) continue;

      auto next =
          proc
          | TxnProc(
              node,
              [](const auto &ctx, auto args) -> Optional<VoidValue> {
                auto &[state, index_handle, bitmap, district_id, carrier_id, ts] = ctx;
                int i = district_id - 1;

                if (bitmap & 0x10) {
                  index_handle(state->new_orders[i]).Delete();
                  ClientBase::OnUpdateRow(state->new_orders[i]);
                }

                if (bitmap & 0x04) {
                  auto oorder = index_handle(state->oorders[i]).template Read<OOrder::Value>();
                  oorder.o_carrier_id = carrier_id;
                  index_handle(state->oorders[i]).Write(oorder);
                  ClientBase::OnUpdateRow(state->oorders[i]);
                }

                if (bitmap & 0x01) {
                  int sum = 0;
                  for (int j = 0; j < 15; j++) {
                    if (state->order_lines[i][j] == nullptr) break;
                    auto handle = index_handle(state->order_lines[i][j]);
                    auto ol = handle.template Read<OrderLine::Value>();
                    sum += ol.ol_amount;
                    ol.ol_delivery_d = ts;
                    handle.Write(ol);
                    ClientBase::OnUpdateRow(state->order_lines[i][j]);
                  }

                  auto customer = index_handle(state->customers[i]).template Read<Customer::Value>();
                  customer.c_balance = sum;
                  index_handle(state->customers[i]).Write(customer);
                  ClientBase::OnUpdateRow(state->customers[i]);
                }

                return nullopt;
              }, bitmap, i + 1, o_carrier_id, ts);
#if 0
      if (node == sum_node) {
        next
            | TxnProc(
                customer_node,
                [](const auto &ctx, auto args) -> Optional<VoidValue> {
                  auto [sum] = args;
                  auto &[state, index_handle, i] = ctx;

                  auto customer = index_handle(state->customers[i]).template Read<Customer::Value>();
                  customer.c_balance = sum;
                  index_handle(state->customers[i]).Write(customer);
                  ClientBase::OnUpdateRow(state->customers[i]);

                  return nullopt;
                }, i);
      }
#endif
    }
  }
  root_promise()->AssignAffinity(NodeConfiguration::g_nr_threads);
}

} // namespace tpcc


namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::Delivery), Client *, uint64_t>::Construct(tpcc::Client * client, uint64_t serial_id)
{
  return new DeliveryTxn(client, serial_id);
}

}
