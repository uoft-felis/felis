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
  void Prepare() override final {
    // client->get_execution_locality_manager().PlanLoad(warehouse_id - 1, 4);
  }
};

void OrderStatusTxn::Run()
{
  // TODO: This does not work for Hash Sharded TPC-C.
  auto aff = std::numeric_limits<uint64_t>::max();

  static constexpr auto ReadCustomer = [](auto state, auto index_handle, int warehouse_id, int district_id, int customer_id) -> void {
    auto &mgr = util::Instance<TableManager>();
    auto customer_key = Customer::Key::New(warehouse_id, district_id, customer_id);
    auto customer = index_handle(
        mgr.Get<Customer>().Search(customer_key.EncodeFromRoutine()))
                    .template Read<Customer::Value>();
  };

  static constexpr auto ScanIndex = [](auto state, auto index_handle, int warehouse_id, int district_id, int customer_id) -> int {
    auto &mgr = util::Instance<TableManager>();
    auto cididx_start = OOrderCIdIdx::Key::New(warehouse_id, district_id, customer_id,
                                               std::numeric_limits<int32_t>::max());
    auto cididx_end = OOrderCIdIdx::Key::New(warehouse_id, district_id, customer_id,
                                             0);
    int oid = -1;
    for (auto it = mgr.Get<OOrderCIdIdx>().IndexReverseIterator(
             cididx_start.EncodeFromRoutine(),
             cididx_end.EncodeFromRoutine()); it->IsValid(); it->Next()) {
      if (it->row()->ShouldScanSkip(index_handle.serial_id())) continue;
      auto cididx_key = OOrderCIdIdx::Key();
      cididx_key.Decode(&it->key());
      oid = cididx_key.o_o_id;
      break;
    }

    abort_if(oid == -1, "OrderStatus cannot find oid for customer {} {} {}",
             warehouse_id, district_id, customer_id);
    return oid;
  };

  static constexpr auto ScanOrderLine = [](auto state, auto index_handle, int warehouse_id, int district_id, int oid) -> void {
    auto &mgr = util::Instance<TableManager>();
    auto ol_start = OrderLine::Key::New(warehouse_id, district_id, oid,
                                        0);
    auto ol_end = OrderLine::Key::New(warehouse_id, district_id, oid,
                                      std::numeric_limits<int32_t>::max());
    for (auto it = mgr.Get<OrderLine>().IndexSearchIterator(
             ol_start.EncodeFromRoutine(),
             ol_end.EncodeFromRoutine()); it->IsValid(); it->Next()) {
      if (it->row()->ShouldScanSkip(index_handle.serial_id())) continue;
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
          int oid = ScanIndex(state, index_handle, warehouse_id, district_id, customer_id);
          ScanOrderLine(state, index_handle, warehouse_id, district_id, oid);

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
          return nullopt;
        },
        aff);

    aff = g_tpcc_config.PWVDistrictToCoreId(district_id, 40);
    state->oid_future = FutureValue<int>();
    root->Then(
        MakeContext(warehouse_id, district_id, customer_id), 0,
        [](const auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, index_handle, warehouse_id, district_id, customer_id] = ctx;
          INIT_ROUTINE_BRK(8 << 10);
          state->oid_future.Signal(ScanIndex(state, index_handle, warehouse_id, district_id, customer_id));
          return nullopt;
        },
        aff);

    aff = g_tpcc_config.PWVDistrictToCoreId(district_id, 10);
    root->Then(
        MakeContext(warehouse_id, district_id, customer_id), 0,
        [](const auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, index_handle, warehouse_id, district_id, customer_id] = ctx;
          int oid = state->oid_future.Wait();
          INIT_ROUTINE_BRK(8 << 10);
          ScanOrderLine(state, index_handle, warehouse_id, district_id, oid);
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
