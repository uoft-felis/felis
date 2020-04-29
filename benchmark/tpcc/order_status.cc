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
  void Prepare() override final {}
};

void OrderStatusTxn::Run()
{
  // TODO: This does not work for distributed TPC-C.
  root->Then(
      MakeContext(warehouse_id, district_id, customer_id), 0,
      [](const auto &ctx, auto _) -> Optional<VoidValue> {
        auto &[state, index_handle, warehouse_id, district_id, customer_id] = ctx;
        auto &mgr = util::Instance<TableManager>();
        INIT_ROUTINE_BRK(8 << 10);

        auto customer_key = Customer::Key::New(warehouse_id, district_id, customer_id);
        auto customer = index_handle(
            mgr.Get<Customer>().Search(customer_key.EncodeFromRoutine()))
                        .template Read<Customer::Value>();

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

        auto ol_start = OrderLine::Key::New(warehouse_id, district_id, oid,
                                            0);
        auto ol_end = OrderLine::Key::New(warehouse_id, district_id, oid,
                                          std::numeric_limits<int32_t>::max());
        for (auto it = mgr.Get<OrderLine>().IndexSearchIterator(
                 ol_start.EncodeFromRoutine(),
                 ol_end.EncodeFromRoutine()); it->IsValid(); it->Next()) {
          if (it->row()->ShouldScanSkip(index_handle.serial_id())) continue;
        }

        return nullopt;
      });
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
