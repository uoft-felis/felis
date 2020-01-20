#include "delivery.h"

namespace tpcc {

static std::atomic_long g_last_no_o_ids[10] = {
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
  auto &mgr = util::Instance<RelationManager>();
  for (int i = 0; i < g_tpcc_config.districts_per_warehouse; i++) {
     auto district_id = i + 1;
    auto oidhi = g_last_no_o_ids[i].load();
    auto oid_min = oidhi << 8;

    auto neworder_start = NewOrder::Key::New(
        warehouse_id, district_id, oid_min, 0);
    auto neworder_end = NewOrder::Key::New(
        warehouse_id, district_id, std::numeric_limits<uint32_t>::max(), 0);

    auto no_key = NewOrder::Key::New(0, 0, 0, 0);

    for (auto no_it = mgr.Get<NewOrder>().IndexSearchIterator(
             neworder_start.EncodeFromRoutine(), neworder_end.EncodeFromRoutine());
         no_it.IsValid(); no_it.Next()) {
      if (no_it.row()->ShouldScanSkip(serial_id())) continue;

      no_key = no_it.key().template ToType<NewOrder::Key>();

      // logger->info("district {} oid {} ver {} < sid {}", district_id, no_key.no_o_id, no_it.row()->first_version(), serial_id());
      goto found;
    }
    state->nodes[i] = NodeBitmap();
    continue;
 found:

    auto oid = no_key.no_o_id;
    long newoidhi = oid >> 8;
    while (newoidhi > oidhi) {
      g_last_no_o_ids[i].compare_exchange_strong(oidhi, newoidhi);
    }

    auto customer_id = no_key.no_c_id;
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
        TxnIndexLookup<TpccSliceRouter, DeliveryState::Completion, Tuple<int>>(
            &args,
            RangeParam<OrderLine>(orderline_start, orderline_end),
            KeyParam<OOrder>(oorder_key),
            KeyParam<Customer>(customer_key),
            KeyParam<NewOrder>(dest_neworder_key));
  }
}

#define SPLIT_CUSTOMER_PIECE

void DeliveryTxn::Run()
{
  for (int i = 0; i < 10; i++) {
    int16_t sum_node = -1, customer_node = -1;
    for (auto &p: state->nodes[i]) {
      auto [node, bitmap] = p;
      if (bitmap & (1 << 0)) sum_node = node;
      if (bitmap & (1 << 3)) customer_node = node;
    }
    for (auto &p: state->nodes[i]) {
      auto [node, bitmap] = p;
      if (bitmap == (1 << 3)) continue;

      auto next = root->Then(
          MakeContext(bitmap, i + 1, o_carrier_id, ts), node,
          [](const auto &ctx, auto args) -> Optional<Tuple<int>> {
            auto &[state, index_handle, bitmap, district_id, carrier_id, ts] = ctx;
            int i = district_id - 1;

            if (bitmap & (1 << 4)) {
              index_handle(state->new_orders[i]).Delete();
              ClientBase::OnUpdateRow(state->new_orders[i]);
            }

            if (bitmap & (1 << 2)) {
              // logger->info("row {} district {} sid {}", (void *) state->oorders[i], district_id, index_handle.serial_id());
              auto oorder = index_handle(state->oorders[i]).template Read<OOrder::Value>();
              oorder.o_carrier_id = carrier_id;
              index_handle(state->oorders[i]).Write(oorder);
              ClientBase::OnUpdateRow(state->oorders[i]);
            }

            if (bitmap & (1 << 0)) {
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

#ifdef SPLIT_CUSTOMER_PIECE
              return Tuple<int>(sum);
#else
              auto customer = index_handle(state->customers[i]).template Read<Customer::Value>();
              customer.c_balance = sum;
              index_handle(state->customers[i]).Write(customer);
              ClientBase::OnUpdateRow(state->customers[i]);
#endif
            }

            return nullopt;
          });

#ifdef SPLIT_CUSTOMER_PIECE
      if (node == sum_node) {
        next->Then(
            MakeContext(i), customer_node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto [sum] = args;
              auto &[state, index_handle, i] = ctx;

              auto customer = index_handle(state->customers[i]).template Read<Customer::Value>();
              customer.c_balance = sum;
              index_handle(state->customers[i]).Write(customer);
              ClientBase::OnUpdateRow(state->customers[i]);

              return nullopt;
            });
      }
#endif
    }
  }
  if (Client::g_enable_granola) {
    root_promise()->AssignAffinity(warehouse_id - 1);
  } else {
    root_promise()->AssignAffinity(NodeConfiguration::g_nr_threads);
  }
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
