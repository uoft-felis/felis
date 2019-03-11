#include "payment.h"

namespace tpcc {

template <>
PaymentStruct ClientBase::GenerateTransactionInput<PaymentStruct>()
{
  PaymentStruct s;
  s.warehouse_id = PickWarehouse();
  s.district_id = PickDistrict();
  if (nr_warehouses() == 1
      || RandomNumber(1, 100) > int(kPaymentRemoteCustomer * 100)) {
    s.customer_warehouse_id = s.warehouse_id;
    s.customer_district_id = s.district_id;
  } else {
    s.customer_warehouse_id = RandomNumberExcept(1, nr_warehouses(), s.warehouse_id);
    s.customer_district_id = PickDistrict();
  }
  s.payment_amount = RandomNumber(100, 500000);
  s.ts = GetCurrentTime();

  s.customer_id = GetCustomerId();
  return s;
}

class PaymentTxn : public Txn<PaymentState>, public PaymentStruct {
  Client *client;
 public:
  PaymentTxn(Client *client, uint64_t serial_id)
      : Txn<PaymentState>(serial_id),
        PaymentStruct(client->GenerateTransactionInput<PaymentStruct>()),
        client(client)
  {}

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
};

void PaymentTxn::Prepare()
{
  INIT_ROUTINE_BRK(4096);

  int lookup_node = client->warehouse_to_lookup_node_id(warehouse_id);
  int customer_lookup_node = client->warehouse_to_lookup_node_id(customer_warehouse_id);
  int node = Client::warehouse_to_node_id(warehouse_id);
  int customer_node = Client::warehouse_to_node_id(customer_warehouse_id);

  auto warehouse_key = Warehouse::Key::New(warehouse_id);
  auto district_key = District::Key::New(warehouse_id, district_id);

  proc
      | TxnLookup<Warehouse>(lookup_node, warehouse_key)
      | TxnAppendVersion(
          node,
          [](const auto &ctx, auto *handle, int _) {
            auto &[state, _1, _2] = ctx;
            state->rows.warehouse = handle;
          });
  proc
      | TxnLookup<District>(lookup_node, district_key)
      | TxnAppendVersion(
          node,
          [](const auto &ctx, auto *handle, int _) {
            auto &[state, _1, _2] = ctx;
            state->rows.district = handle;
          });

  Customer::Key customer_key;
  customer_key = Customer::Key::New(
      customer_warehouse_id, customer_district_id, customer_id);

  proc
      | TxnLookup<Customer>(customer_lookup_node, customer_key)
      | TxnAppendVersion(
          customer_node,
          [](const auto &ctx, auto *handle, int _) {
            auto &[state, _1, _2] = ctx;
            state->rows.customer = handle;
          });
}

void PaymentTxn::Run()
{
  int node = Client::warehouse_to_node_id(warehouse_id);
  int customer_node = Client::warehouse_to_node_id(customer_warehouse_id);

  proc
      | TxnProc(
          node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle, payment_amount] = ctx;
            TxnVHandle vhandle = index_handle(state->rows.warehouse);
            auto warehouse = vhandle.Read<Warehouse::Value>();
            warehouse.w_ytd += payment_amount;
            vhandle.Write(warehouse);
            ClientBase::OnUpdateRow(state->rows.warehouse);
            return nullopt;
          },
          payment_amount);

  proc
      | TxnProc(
          node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle, payment_amount] = ctx;
            TxnVHandle vhandle = index_handle(state->rows.district);
            auto district = vhandle.Read<District::Value>();
            district.d_ytd += payment_amount;
            vhandle.Write(district);
            ClientBase::OnUpdateRow(state->rows.district);
            return nullopt;
          },
          payment_amount);

  proc
      | TxnProc(
          customer_node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle, payment_amount] = ctx;
            TxnVHandle vhandle = index_handle(state->rows.customer);
            auto customer = vhandle.Read<Customer::Value>();
            customer.c_balance -= payment_amount;
            customer.c_ytd_payment += payment_amount;
            customer.c_payment_cnt++;
            vhandle.Write(customer);
            ClientBase::OnUpdateRow(state->rows.customer);
            return nullopt;
          },
          payment_amount);
}

}


namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::Payment), Client *, uint64_t>::Construct(tpcc::Client * client, uint64_t serial_id)
{
  return new PaymentTxn(client, serial_id);
}

}
