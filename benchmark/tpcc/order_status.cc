#include "order_status.h"

namespace tpcc {

template <>
OrderStatusStruct ClientBase::GenerateTransactionInput<OrderStatusStruct>()
{
  OrderStatusStruct s;
  s.warehouse_id = PickWarehouse();
  s.district_id = PickDistrict();
  s.customer_id = GetCustomerId();
  return s;
}

class OrderStatusTxn : public Txn<OrderStatusState>, public OrderStatusStruct {
  Client *client;
 public:
  OrderStatusTxn(Client *client, uint64_t serial_id)
      : Txn<OrderStatusState>(serial_id),
        OrderStatusStruct(client->GenerateTransactionInput<OrderStatusStruct>()),
        client(client)
  {}
  void Run() override final;
  void PrepareInsert() override final {}
  void Prepare() override final;
};

static void LookupCustomerIndex(
    const OrderStatusTxn::State &state,
    int warehouse_id, int district_id, int customer_id)
{
  auto &mgr = util::Instance<TableManager>();
  INIT_ROUTINE_BRK(8 << 10);
  auto customer_key = Customer::Key::New(warehouse_id, district_id, customer_id);
  state->customer = mgr.Get<Customer>().Search(customer_key.EncodeFromRoutine());
}

static void ScanOrdersIndex(
    const OrderStatusTxn::State &state,
    uint64_t sid, int warehouse_id, int district_id, int customer_id)
{
  auto cididx_start = OOrderCIdIdx::Key::New(warehouse_id, district_id, customer_id,
                                             std::numeric_limits<int32_t>::max());
  auto cididx_end = OOrderCIdIdx::Key::New(warehouse_id, district_id, customer_id,
                                           0);
  auto &mgr = util::Instance<TableManager>();
  INIT_ROUTINE_BRK(8 << 10);
  int oid = -1;
  for (auto it = mgr.Get<OOrderCIdIdx>().IndexReverseIterator(
           cididx_start.EncodeFromRoutine(),
           cididx_end.EncodeFromRoutine()); it->IsValid(); it->Next()) {
    if (it->row()->ShouldScanSkip(sid)) continue;
    auto cididx_key = OOrderCIdIdx::Key();
    cididx_key.Decode(&it->key());
    oid = cididx_key.o_o_id;
    break;
  }

  abort_if(oid == -1, "OrderStatus cannot find oid for customer {} {} {}",
           warehouse_id, district_id, customer_id);
  state->oid = oid;

  auto ol_start = OrderLine::Key::New(warehouse_id, district_id, oid,
                                      0);
  auto ol_end = OrderLine::Key::New(warehouse_id, district_id, oid,
                                    std::numeric_limits<int32_t>::max());

  int i = 0;
  std::fill(state->order_line, state->order_line + 15, nullptr);
  for (auto it = mgr.Get<OrderLine>().IndexSearchIterator(
           ol_start.EncodeFromRoutine(),
           ol_end.EncodeFromRoutine()); it->IsValid() && i < 15; it->Next()) {
    if (it->row()->ShouldScanSkip(sid)) continue;
    state->order_line[i] = it->row();
    i++;
  }
}

void OrderStatusTxn::Prepare()
{
  if (!VHandleSyncService::g_lock_elision) {
    if (!Options::kTpccReadOnlyDelayQuery) {
      LookupCustomerIndex(state, warehouse_id, district_id, customer_id);
      ScanOrdersIndex(state, serial_id(), warehouse_id, district_id, customer_id);
    }
  } else {
    int parts[2] = {
      (int) warehouse_id - 1, (int) warehouse_id - 1,
    };
    if (!g_tpcc_config.IsWarehousePinnable()) {
      parts[0] = g_tpcc_config.PWVDistrictToCoreId(district_id, 20);
      parts[1] = g_tpcc_config.PWVDistrictToCoreId(district_id, 10);
    }

    if (Client::g_enable_pwv) {
      for (auto part_id: parts) util::Instance<PWVGraphManager>()[part_id]->ReserveEdge(serial_id());
    }

    // Need to partition the index lookup!
    root->Then(
        MakeContext(warehouse_id, district_id, customer_id, parts[0]), 1,
        [](auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, handle, warehouse_id, district_id, customer_id, p] = ctx;
          LookupCustomerIndex(state, warehouse_id, district_id, customer_id);
          if (Client::g_enable_pwv) {
            util::Instance<PWVGraphManager>()[p]->AddResource(
                handle.serial_id(),
                PWVGraph::VHandleToResource(state->customer));
          }
          return nullopt;
        },
        parts[0]);
    root->Then(
        MakeContext(warehouse_id, district_id, customer_id, parts[1]), 1,
        [](auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, handle, warehouse_id, district_id, customer_id, p] = ctx;
          ScanOrdersIndex(state, handle.serial_id(), warehouse_id, district_id, customer_id);
          if (Client::g_enable_pwv) {
            util::Instance<PWVGraphManager>()[p]->AddResource(
                handle.serial_id(),
                PWVGraph::VHandleToResource(state->order_line[0]));
          }
          return nullopt;
        },
        parts[1]);
  }
}

void OrderStatusTxn::Run()
{
  // TODO: This does not work for Hash Sharded TPC-C.
  auto aff = std::numeric_limits<uint64_t>::max();

  static constexpr auto ReadCustomer = [](auto state, auto index_handle, int warehouse_id, int district_id, int customer_id) -> void {
    auto customer = index_handle(state->customer).template Read<Customer::Value>();
  };

  static constexpr auto ScanOrderLine = [](auto state, auto index_handle, int warehouse_id, int district_id, int oid) -> void {
    for (int i = 0; i < 15; i++) {
      if (state->order_line[i] == nullptr) break;
      index_handle(state->order_line[i]).template Read<OrderLine::Value>();
    }
  };

  if (!Options::kEnablePartition || g_tpcc_config.IsWarehousePinnable()) {
    if (g_tpcc_config.IsWarehousePinnable()) {
      aff = Config::WarehouseToCoreId(warehouse_id);
    }
    root->Then(
        MakeContext(warehouse_id, district_id, customer_id), 0,
        [](const auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, index_handle, warehouse_id, district_id, customer_id] = ctx;
          INIT_ROUTINE_BRK(8 << 10);

          if (Options::kTpccReadOnlyDelayQuery)
            LookupCustomerIndex(state, warehouse_id, district_id, customer_id);

          ReadCustomer(state, index_handle, warehouse_id, district_id, customer_id);
          if (Client::g_enable_pwv) {
            util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                index_handle.serial_id(),
                PWVGraph::VHandleToResource(state->customer));
          }

          if (Options::kTpccReadOnlyDelayQuery)
            ScanOrdersIndex(state, index_handle.serial_id(), warehouse_id, district_id, customer_id);

          int oid = state->oid;
          ScanOrderLine(state, index_handle, warehouse_id, district_id, oid);
          if (Client::g_enable_pwv) {
            util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                index_handle.serial_id(),
                PWVGraph::VHandleToResource(state->order_line[0]));
          }

          return nullopt;
        },
        aff);
  } else { // kEnablePartition && !IsWarehousePinnable()
    aff = g_tpcc_config.PWVDistrictToCoreId(district_id, 20);
    root->Then(
        MakeContext(warehouse_id, district_id, customer_id), 0,
        [](const auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, index_handle, warehouse_id, district_id, customer_id] = ctx;
          INIT_ROUTINE_BRK(8 << 10);
          ReadCustomer(state, index_handle, warehouse_id, district_id, customer_id);
          if (Client::g_enable_pwv) {
            util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                index_handle.serial_id(),
                PWVGraph::VHandleToResource(state->customer));
          }
          return nullopt;
        },
        aff);

    aff = g_tpcc_config.PWVDistrictToCoreId(district_id, 10);
    root->Then(
        MakeContext(warehouse_id, district_id, customer_id), 0,
        [](const auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, index_handle, warehouse_id, district_id, customer_id] = ctx;
          int oid = state->oid;
          INIT_ROUTINE_BRK(8 << 10);
          ScanOrderLine(state, index_handle, warehouse_id, district_id, oid);
          if (Client::g_enable_pwv) {
            util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                index_handle.serial_id(),
                PWVGraph::VHandleToResource(state->order_line[0]));
          }
          return nullopt;
        },
        aff);
  }
}

}

namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::OrderStatus), Client *, uint64_t>::Construct(tpcc::Client * client, uint64_t serial_id)
{
  return new OrderStatusTxn(client, serial_id);
}

}
