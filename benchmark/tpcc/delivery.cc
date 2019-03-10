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

  memcpy(s.last_no_o_ids, g_last_no_o_ids, sizeof(int) * 10);
  for (int i = 0; i < kTPCCConfig.districts_per_warehouse; i++) {
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
  auto node = Client::warehouse_to_node_id(warehouse_id);

  memset(state->rows.new_orders, 0, sizeof(VHandle *) * 10);

  proc
      | TxnPipelineProc(
          node,
          [](const auto &ctx, auto _) -> Optional<Tuple<int>> {
            const auto &[state, handle] = ctx.value();
            for (int i = 0; i < kTPCCConfig.districts_per_warehouse; i++) {
              ctx.Yield(Tuple<int>(i));
            }
            return nullopt;
          },
          kTPCCConfig.districts_per_warehouse)
      | TxnProc(
          node,
          [](const auto &ctx, auto args) -> Optional<Tuple<int, VHandle *, std::array<VHandle *, 15>, VHandle *, VHandle *>> {
            INIT_ROUTINE_BRK(4096);

            auto &[state, index_handle, warehouse_id, oids] = ctx;
            auto &[i] = args;
            auto district_id = i + 1;

            std::array<VHandle *, 15> order_lines;
            order_lines.fill(nullptr);

            auto &mgr = util::Instance<RelationManager>();

            auto neworder_start = NewOrder::Key::New(
                warehouse_id, district_id, oids[i], 0);
            auto neworder_end = NewOrder::Key::New(
                warehouse_id, district_id, oids[i], std::numeric_limits<int>::max());

            auto &neworder_table = mgr.Get<NewOrder>();
            auto no_it = neworder_table.IndexSearchIterator(
                neworder_start.EncodeFromRoutine(), neworder_end.EncodeFromRoutine());

            if (!no_it.IsValid()) {
              return nullopt;
            }

            auto customer_id = no_it.key().template ToType<NewOrder::Key>().no_c_id;

            auto &orderline_table = mgr.Get<OrderLine>();
            auto orderline_start = OrderLine::Key::New(
                warehouse_id, district_id, oids[i], 0);
            auto orderline_end = OrderLine::Key::New(
                warehouse_id, district_id, oids[i], std::numeric_limits<int>::max());
            auto it = orderline_table.IndexSearchIterator(
                orderline_start.EncodeFromRoutine(), orderline_end.EncodeFromRoutine());
            for (int j = 0; it.IsValid() && j < 15; it.Next(), j++) {
              order_lines[j] = it.row();
            }

            // Should we parallize these lookups with the orderline?
            auto &oorder_table = mgr.Get<OOrder>();
            VHandle *oorder = oorder_table.Search(
                OOrder::Key::New(warehouse_id, district_id, oids[i]).EncodeFromRoutine());

            auto &customer_table = mgr.Get<Customer>();
            VHandle *customer = customer_table.Search(Customer::Key::New(
                warehouse_id, district_id, customer_id).EncodeFromRoutine());

            return Tuple<int, VHandle *, std::array<VHandle *, 15>, VHandle *, VHandle *>(i, no_it.row(), order_lines, oorder, customer);
          },
          warehouse_id, last_no_o_ids)
      | TxnProc(
          node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle] = ctx;
            auto &[i, new_order_row, order_line_rows, oorder_row, customer_row] = args;
            state->rows.new_orders[i] = new_order_row;
            for (int j = 0; j < 15; j++) {
              state->rows.order_lines[i][j] = order_line_rows[j];
            }
            state->rows.oorders[i] = oorder_row;
            state->rows.customers[i] = customer_row;
            return nullopt;
          });
}

void DeliveryTxn::Run()
{
  auto node = Client::warehouse_to_node_id(warehouse_id);
  uint32_t bitmap = 0;
  int nr_districts = 0;
  for (int i = 0; i < 10; i++) {
    if (state->rows.new_orders[i] != nullptr) {
      bitmap |= (1 << i);
      nr_districts++;
    }
  }
  proc
      | TxnPipelineProc(
          node,
          [](const auto &ctx, auto _) -> Optional<Tuple<int>> {
            uint32_t bitmap = ctx.template _<2>();
            for (int i = 0; i < 10; i++) {
              if (bitmap & (1 << i))
                ctx.Yield(Tuple<int>(i));
            }
            return nullopt;
          },
          nr_districts, bitmap)
      | TxnProc(
          node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle, carrier_id, ts] = ctx;
            auto &[i] = args;
            auto district_id = i + 1;

            index_handle(state->rows.new_orders[i]).Delete();

            auto oorder = index_handle(state->rows.oorders[i]).template Read<OOrder::Value>();
            oorder.o_carrier_id = carrier_id;
            index_handle(state->rows.oorders[i]).Write(oorder);

            int sum = 0;
            for (int j = 0; j < 15; j++) {
              if (state->rows.order_lines[i][j] == nullptr) break;
              auto handle = index_handle(state->rows.order_lines[i][j]);
              auto ol = handle.template Read<OrderLine::Value>();
              sum += ol.ol_amount;
              ol.ol_delivery_d = ts;
              handle.Write(ol);
            }

            auto customer = index_handle(state->rows.customers[i]).template Read<Customer::Value>();
            customer.c_balance = sum;
            index_handle(state->rows.customers[i]).Write(customer);

            return nullopt;
          }, o_carrier_id, ts);
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
