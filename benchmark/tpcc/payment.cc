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

  auto warehouse_key = Warehouse::Key::New(warehouse_id);
  auto district_key = District::Key::New(warehouse_id, district_id);
  auto customer_key = Customer::Key::New(
      customer_warehouse_id, customer_district_id, customer_id);

  state->nodes =
      TxnIndexLookup<PaymentState::Completion, void>(
          tpcc::SliceRouter, nullptr,
          KeyParam<Warehouse>(warehouse_key),
          KeyParam<District>(district_key),
          KeyParam<Customer>(customer_key));
}

void PaymentTxn::Run()
{
  for (auto &p: state->nodes) {
    auto [node, bitmap] = p;
    proc
        | TxnProc(
            node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, bitmap, payment_amount] = ctx;
              if (bitmap & 0x01) {
                // Warehouse
                TxnVHandle vhandle = index_handle(state->warehouse);
                auto w = vhandle.Read<Warehouse::Value>();
                w.w_ytd += payment_amount;
                vhandle.Write(w);
                ClientBase::OnUpdateRow(state->warehouse);
              }
              if (bitmap & 0x02) {
                // District
                TxnVHandle vhandle = index_handle(state->district);
                auto d = vhandle.Read<District::Value>();
                d.d_ytd += payment_amount;
                vhandle.Write(d);
                ClientBase::OnUpdateRow(state->district);
              }
              if (bitmap & 0x04) {
                // Customer
                TxnVHandle vhandle = index_handle(state->customer);
                auto c = vhandle.Read<Customer::Value>();
                c.c_balance -= payment_amount;
                c.c_ytd_payment += payment_amount;
                c.c_payment_cnt++;
                vhandle.Write(c);
                ClientBase::OnUpdateRow(state->customer);
              }
              return nullopt;
            },
            bitmap, payment_amount);
  }
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
