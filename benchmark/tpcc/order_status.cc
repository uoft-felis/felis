#include "order_status.h"
#include "pwv_graph.h"

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

void OrderStatusTxn::Prepare()
{
  auto &mgr = util::Instance<TableManager>();
  INIT_ROUTINE_BRK(8 << 10);
  auto customer_key = Customer::Key::New(warehouse_id, district_id, customer_id);
  state->customer = mgr.Get<Customer>().Search(customer_key.EncodeFromRoutine());

  auto cididx_start = OOrderCIdIdx::Key::New(warehouse_id, district_id, customer_id,
                                             std::numeric_limits<int32_t>::max());
  auto cididx_end = OOrderCIdIdx::Key::New(warehouse_id, district_id, customer_id,
                                           0);
  int oid = -1;
  for (auto it = mgr.Get<OOrderCIdIdx>().IndexReverseIterator(
           cididx_start.EncodeFromRoutine(),
           cididx_end.EncodeFromRoutine()); it->IsValid(); it->Next()) {
    if (it->row()->ShouldScanSkip(serial_id())) continue;
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
    if (it->row()->ShouldScanSkip(serial_id())) continue;
    state->order_line[i] = it->row();
    i++;
  }

  if (VHandleSyncService::g_lock_elision && Client::g_enable_pwv) {
    auto &gm = util::Instance<PWVGraphManager>();
    if (g_tpcc_config.IsWarehousePinnable()) {
      gm[warehouse_id - 1]->ReserveEdge(serial_id(), 2);
      for (auto &row: {state->customer, state->order_line[0]}) {
        gm[warehouse_id - 1]->AddResource(serial_id(), PWVGraph::VHandleToResource(row));
      }
    } else {
      int parts[2] = {
        g_tpcc_config.PWVDistrictToCoreId(district_id, 20),
        g_tpcc_config.PWVDistrictToCoreId(district_id, 10),
      };

      for (auto part_id: parts) {
        gm[part_id]->ReserveEdge(serial_id());
      }
      gm[parts[0]]->AddResource(serial_id(),
                                PWVGraph::VHandleToResource(state->customer));
      gm[parts[1]]->AddResource(serial_id(),
                                PWVGraph::VHandleToResource(state->order_line[0]));
    }
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

          ReadCustomer(state, index_handle, warehouse_id, district_id, customer_id);
          if (Client::g_enable_pwv) {
            util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                index_handle.serial_id(),
                PWVGraph::VHandleToResource(state->customer));
          }
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
