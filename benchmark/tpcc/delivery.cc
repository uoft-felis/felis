#include "delivery.h"

namespace tpcc {

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
  void Prepare() override final {
    if (!EpochClient::g_enable_granola)
      PrepareImpl();
  }
  void PrepareInsert() override final {}
  void PrepareImpl();
};

void DeliveryTxn::PrepareImpl()
{
  INIT_ROUTINE_BRK(16384);
  auto &mgr = util::Instance<TableManager>();
  for (int i = 0; i < g_tpcc_config.districts_per_warehouse; i++) {
    auto district_id = i + 1;
    auto oid_min = ClientBase::LastNewOrderId(warehouse_id, district_id);

    auto neworder_start = NewOrder::Key::New(
        warehouse_id, district_id, oid_min, 0);
    auto neworder_end = NewOrder::Key::New(
        warehouse_id, district_id, std::numeric_limits<uint32_t>::max(), 0);

    auto no_key = NewOrder::Key::New(0, 0, 0, 0);

    for (auto no_it = mgr.Get<NewOrder>().IndexSearchIterator(
             neworder_start.EncodeFromRoutine(), neworder_end.EncodeFromRoutine());
         no_it->IsValid(); no_it->Next()) {
      if (no_it->row()->ShouldScanSkip(serial_id())) continue;
      no_key = no_it->key().template ToType<NewOrder::Key>();
      oid_min = ClientBase::LastNewOrderId(warehouse_id, district_id);
      if (no_key.no_o_id <= oid_min) continue;
      if (ClientBase::IncrementLastNewOrderId(
              warehouse_id, district_id, oid_min, no_key.no_o_id)) {
        // logger->info("district {} oid {} ver {} < sid {}", district_id, no_key.no_o_id, no_it.row()->first_version(), serial_id());
        goto found;
      }
    }
    state->nodes[i] = NodeBitmap();
    continue;
 found:

    auto oid = no_key.no_o_id;
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

  root->AssignAffinity(warehouse_id - 1);
}

// #define SPLIT_CUSTOMER_PIECE

void DeliveryTxn::Run()
{
  if (EpochClient::g_enable_granola)
    PrepareImpl();

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
      auto aff = std::numeric_limits<uint64_t>::max();

      aff = AffinityFromRows(0x03, {state->oorders[i], state->new_orders[i]});

      root->Then(
          MakeContext(bitmap, i, o_carrier_id), node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle, bitmap, i, carrier_id] = ctx;

            probes::TpccDelivery{0, __builtin_popcount(bitmap)}();

            if (bitmap & (1 << 4)) {
              index_handle(state->new_orders[i]).Delete();
              ClientBase::OnUpdateRow(state->new_orders[i]);
            }

            if (bitmap & (1 << 2)) {
              // logger->info("row {} district {} sid {}", (void *) state->oorders[i], district_id, index_handle.serial_id());
              auto oorder = index_handle(state->oorders[i]).template Read<OOrder::Value>();
              oorder.o_carrier_id = carrier_id;
              index_handle(state->oorders[i]).WriteTryInline(oorder);
              ClientBase::OnUpdateRow(state->oorders[i]);
            }

            return nullopt;
          }, aff);

      VHandle *valid_rows[16];
      int nr_valid_rows = 0;
      for (int j = 0; j < 15; j++) {
        if (state->order_lines[i][j] == nullptr) break;
        valid_rows[nr_valid_rows++] = state->order_lines[i][j];
      }
      valid_rows[nr_valid_rows++] = state->customers[i];

      aff = AffinityFromRows((1 << nr_valid_rows) - 1, valid_rows);

      auto next = root->Then(
          MakeContext(bitmap, i, ts), node,
          [](const auto &ctx, auto args) -> Optional<Tuple<int>> {
            auto &[state, index_handle, bitmap, i, ts] = ctx;

            if (bitmap & (1 << 0)) {
              int sum = 0;
              for (int j = 0; j < 15; j++) {
                if (state->order_lines[i][j] == nullptr) break;
                auto handle = index_handle(state->order_lines[i][j]);
                auto ol = handle.template Read<OrderLine::Value>();
                sum += ol.ol_amount;
                ol.ol_delivery_d = ts;

                probes::TpccDelivery{1, 1}();

                handle.WriteTryInline(ol);
                ClientBase::OnUpdateRow(state->order_lines[i][j]);
              }

#ifdef SPLIT_CUSTOMER_PIECE
              return Tuple<int>(sum);
#else
              probes::TpccDelivery{1, 1}();

              auto customer = index_handle(state->customers[i]).template Read<Customer::Value>();
              customer.c_balance = sum;
              index_handle(state->customers[i]).Write(customer);
              ClientBase::OnUpdateRow(state->customers[i]);
#endif
            }

            return nullopt;
          }, aff);

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
