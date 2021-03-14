#include "delivery.h"

namespace tpcc {

template <>
DeliveryStruct ClientBase::GenerateTransactionInput<DeliveryStruct>()
{
  DeliveryStruct s;
  s.warehouse_id = PickWarehouse();
  s.o_carrier_id = PickDistrict();
  s.ts = GetCurrentTime();

  for (int i = 0; i < g_tpcc_config.districts_per_warehouse; i++) {
    auto district_id = i + 1;
    s.oid[i] = ClientBase::AcquireLastNewOrderId(s.warehouse_id, district_id);
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
  void PrepareInsert() override final;
};

void DeliveryTxn::PrepareInsert()
{
}

void DeliveryTxn::Prepare()
{
  if (VHandleSyncService::g_lock_elision) {
    if (Client::g_enable_granola || Client::g_enable_pwv) {
      root = new Promise<DummyValue>(nullptr, 128 + BasePromise::kInlineLimit);
    } else {
      root = new Promise<DummyValue>(nullptr, 64 + BasePromise::kInlineLimit);
    }
  }
  INIT_ROUTINE_BRK(16384);
  auto &mgr = util::Instance<TableManager>();
  for (int i = 0; i < g_tpcc_config.districts_per_warehouse; i++) {
    auto district_id = i + 1;
    auto oid_min = oid[i];

    auto neworder_start = NewOrder::Key::New(
        warehouse_id, district_id, oid_min, 0);
    auto neworder_end = NewOrder::Key::New(
        warehouse_id, district_id, std::numeric_limits<uint32_t>::max(), 0);

    auto no_key = NewOrder::Key::New(0, 0, 0, 0);

    // FIXME: we have to do a scan here because the following initialization
    // would depend on the OID scanned from this loop. This is totally fine for
    // Felis/Caracal because of the dedicate insertion phase.
    //
    // But for partitioning based system, the scan maybe staled and thus
    // non-serializable. Although this is unlikely to happen if the epoch size
    // is small enough.
    for (auto no_it = mgr.Get<NewOrder>().IndexSearchIterator(
             neworder_start.EncodeFromRoutine(), neworder_end.EncodeFromRoutine());
         no_it->IsValid(); no_it->Next()) {
      no_key = no_it->key().template ToType<NewOrder::Key>();

      if (no_it->row()->ShouldScanSkip(serial_id())) {
        logger->warn("TPCC Delivery: skipping w {} d {} oid {} (from node {}), first ver {}",
                     warehouse_id, district_id, no_key.no_o_id >> 8, no_key.no_o_id & 0xFF,
                     no_it->row()->first_version());
        break;
      }
      goto found;
    }
    state->nodes[i] = NodeBitmap();
    logger->warn("TPCC Delivery: sid {} oid_min {}", serial_id(), oid_min >> 8);
    // std::abort();
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

    if (!VHandleSyncService::g_lock_elision || g_tpcc_config.IsWarehousePinnable()) {
      if (VHandleSyncService::g_lock_elision) {
        txn_indexop_affinity = warehouse_id - 1;
        if (Client::g_enable_pwv) {
          util::Instance<PWVGraphManager>()[txn_indexop_affinity]->ReserveEdge(serial_id(), 4);
        }
      }

      state->nodes[i] =
          TxnIndexLookup<TpccSliceRouter, DeliveryState::Completion, Tuple<int>>(
              &args,
              RangeParam<OrderLine>(orderline_start, orderline_end),
              KeyParam<OOrder>(oorder_key),
              KeyParam<Customer>(customer_key),
              KeyParam<NewOrder>(dest_neworder_key));
    } else {
      int parts[4] = {
        g_tpcc_config.PWVDistrictToCoreId(district_id, 10),
        g_tpcc_config.PWVDistrictToCoreId(district_id, 30),
        g_tpcc_config.PWVDistrictToCoreId(district_id, 40),
        g_tpcc_config.PWVDistrictToCoreId(district_id, 20),
      };

      if (Client::g_enable_pwv) {
        auto &gm = util::Instance<PWVGraphManager>();
        for (auto part_id: parts)
          gm[part_id]->ReserveEdge(serial_id());
      }

      txn_indexop_affinity = parts[0];
      state->nodes[i] =
          TxnIndexLookup<TpccSliceRouter, DeliveryState::Completion, Tuple<int>>(
              &args,
              RangeParam<OrderLine>(orderline_start, orderline_end));

      txn_indexop_affinity = parts[1];
      state->nodes[i] +=
          TxnIndexLookup<TpccSliceRouter, DeliveryState::Completion, Tuple<int>>(
              &args,
              PlaceholderParam(2),
              PlaceholderParam(),
              PlaceholderParam(),
              KeyParam<NewOrder>(dest_neworder_key));

      txn_indexop_affinity = parts[2];
      state->nodes[i] +=
          TxnIndexLookup<TpccSliceRouter, DeliveryState::Completion, Tuple<int>>(
              &args,
              PlaceholderParam(2),
              KeyParam<OOrder>(oorder_key));

      txn_indexop_affinity = parts[3];
      state->nodes[i] +=
          TxnIndexLookup<TpccSliceRouter, DeliveryState::Completion, Tuple<int>>(
              &args,
              PlaceholderParam(2),
              PlaceholderParam(),
              KeyParam<Customer>(customer_key));
    }
  }
}

// #define SPLIT_CUSTOMER_PIECE

void DeliveryTxn::Run()
{
  if (Options::kEnablePartition && !g_tpcc_config.IsWarehousePinnable()
      && !Client::g_enable_granola && !Client::g_enable_pwv) {
    root = new Promise<DummyValue>(nullptr, 64 + BasePromise::kInlineLimit);
  }

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

      static auto constexpr DeleteNewOrder = [](auto state, auto index_handle, int i) -> void {
        index_handle(state->new_orders[i]).Delete();
        ClientBase::OnUpdateRow(state->new_orders[i]);
      };

      static auto constexpr UpdateOOrder = [](auto state, auto index_handle, int i, uint carrier_id) -> void {
        // logger->info("row {} district {} sid {}", (void *) state->oorders[i], district_id, index_handle.serial_id());
        auto oorder = index_handle(state->oorders[i]).template Read<OOrder::Value>();
        oorder.o_carrier_id = carrier_id;
        index_handle(state->oorders[i]).WriteTryInline(oorder);
        ClientBase::OnUpdateRow(state->oorders[i]);
      };

      static auto constexpr CalcSum = [](auto state, auto index_handle, int i, uint32_t ts) -> int {
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
        return sum;
      };

      static auto constexpr WriteCustomer = [](auto state, auto index_handle, int i, int sum) -> void {
        probes::TpccDelivery{1, 1}();
        auto customer = index_handle(state->customers[i]).template Read<Customer::Value>();
        customer.c_balance += sum;
        index_handle(state->customers[i]).Write(customer);
        ClientBase::OnUpdateRow(state->customers[i]);
      };


      if (!Options::kEnablePartition || g_tpcc_config.IsWarehousePinnable()) {
#ifndef SPLIT_CUSTOMER_PIECE
        if (bitmap & 0x01) {
          state->customer_future[i] = UpdateForKey(
              node, state->customers[i],
              [](const auto &ctx, VHandle *row) {
                auto &[state, index_handle, i, ts] = ctx;
                int sum = CalcSum(state, index_handle, i, ts);
                WriteCustomer(state, index_handle, i, sum);
              }, i, ts);
        }

        if (bitmap != 0x81 || state->customer_future[i].has_callback()) {
          if (Options::kEnablePartition)
            aff = g_tpcc_config.WarehouseToCoreId(warehouse_id);

          root->Then(
              MakeContext(bitmap, i, ts, o_carrier_id), node,
              [](const auto &ctx, auto args) -> Optional<VoidValue> {
                auto &[state, index_handle, bitmap, i, ts, carrier_id] = ctx;

                probes::TpccDelivery{0, __builtin_popcount(bitmap)}();

                if (bitmap & (1 << 4)) {
                  DeleteNewOrder(state, index_handle, i);
                  if (Client::g_enable_pwv) {
                    util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                        index_handle.serial_id(),
                        PWVGraph::VHandleToResource(state->new_orders[i]));
                  }
                }

                if (bitmap & (1 << 2)) {
                  UpdateOOrder(state, index_handle, i, carrier_id);
                  if (Client::g_enable_pwv) {
                    util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                        index_handle.serial_id(),
                        PWVGraph::VHandleToResource(state->oorders[i]));
                  }
                }

                state->customer_future[i].Invoke(state, index_handle, i, ts);
                if (Client::g_enable_pwv) {
                  auto g = util::Instance<PWVGraphManager>().local_graph();
                  g->ActivateResource(
                      index_handle.serial_id(),
                      PWVGraph::VHandleToResource(state->order_lines[i][0]));
                  g->ActivateResource(
                      index_handle.serial_id(),
                      PWVGraph::VHandleToResource(state->customers[i]));
                }

                return nullopt;
              },
              aff);
        }
#endif

#ifdef SPLIT_CUSTOMER_PIECE
        // TODO: This code isn't well designed, nor well-debugged!

        // Under hash sharding, we need to split an intra-txn dependency because
        // customer table may on a different machine.
        if (node == sum_node) {
          root->Then(
              MakeContext(bitmap, i, ts), node,
              [](const auto &ctx, auto args) -> Optional<Tuple<int>> {
                auto &[state, index_handle, bitmap, i, ts] = ctx;
                return Tuple<int>(CalcSum(state, index_handle, i, ts));
              }, aff)->Then(
                  MakeContext(i), customer_node,
                  [](const auto &ctx, auto args) -> Optional<VoidValue> {
                    auto [sum] = args;
                    auto &[state, index_handle, i] = ctx;
                    WriteCustomer(state, index_handle, i, sum);
                    return nullopt;
                  });
        }
#endif
      } else { // kEnablePartition && !WarehousePinnable()
        int d = i + 1;
        aff = g_tpcc_config.PWVDistrictToCoreId(d, 30);
        root->Then(
            MakeContext(i), node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, i] = ctx;
              abort_if(state->new_orders[i] == nullptr, "??? i {}", i);
              DeleteNewOrder(state, index_handle, i);
              if (Client::g_enable_pwv) {
                util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                    index_handle.serial_id(),
                    PWVGraph::VHandleToResource(state->new_orders[i]));
              }
              return nullopt;
            }, aff);

        aff = g_tpcc_config.PWVDistrictToCoreId(d, 40);
        root->Then(
            MakeContext(i, o_carrier_id), node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, i, carrier_id] = ctx;
              UpdateOOrder(state, index_handle, i, carrier_id);
              if (Client::g_enable_pwv) {
                util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                    index_handle.serial_id(),
                    PWVGraph::VHandleToResource(state->oorders[i]));
              }
              return nullopt;
            }, aff);

        aff = g_tpcc_config.PWVDistrictToCoreId(d, 10);
        state->sum_future_values[i] = FutureValue<int>();
        root->Then(
            MakeContext(i, ts), node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, i, ts] = ctx;
              state->sum_future_values[i].Signal(CalcSum(state, index_handle, i, ts));
              if (Client::g_enable_pwv) {
                auto &gm = util::Instance<PWVGraphManager>();
                auto g = gm.local_graph();
                g->ActivateResource(
                    index_handle.serial_id(),
                    PWVGraph::VHandleToResource(state->order_lines[i][0]));
                if (RVPInfo::FromRoutine(state->customer_last[i])->indegree.fetch_sub(1) == 1) {
                  gm.NotifyRVPChange(
                      index_handle.serial_id(),
                      g_tpcc_config.PWVDistrictToCoreId(i + 1, 20));
                }
              }
              return nullopt;
            }, aff);

        aff = g_tpcc_config.PWVDistrictToCoreId(d, 20);
        root->Then(
            MakeContext(i), node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, i] = ctx;
              int sum = state->sum_future_values[i].Wait();
              WriteCustomer(state, index_handle, i, sum);
              if (Client::g_enable_pwv) {
                util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                    index_handle.serial_id(),
                    PWVGraph::VHandleToResource(state->customers[i]));
              }
              return nullopt;
            }, aff);
        state->customer_last[i] = root->last();
        RVPInfo::MarkRoutine(root->last());
      }
    }
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
