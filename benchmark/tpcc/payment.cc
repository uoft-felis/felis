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

  void Prepare() override final {
    if (!Client::g_enable_granola)
      PrepareImpl();
  }
  void Run() override final;
  void PrepareInsert() override final {
    client->get_initialization_locality_manager().PlanLoad(warehouse_id - 1, 4);
  }
  void PrepareImpl();
};

void PaymentTxn::PrepareImpl()
{
  INIT_ROUTINE_BRK(4096);

  auto warehouse_key = Warehouse::Key::New(warehouse_id);
  auto district_key = District::Key::New(warehouse_id, district_id);
  auto customer_key = Customer::Key::New(
      customer_warehouse_id, customer_district_id, customer_id);

  if (!VHandleSyncService::g_lock_elision) {
    state->nodes =
        TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, void>(
            nullptr,
            KeyParam<Warehouse>(warehouse_key),
            KeyParam<District>(district_key),
            KeyParam<Customer>(customer_key));

    if (g_tpcc_config.IsWarehousePinnable()) {
      root->AssignAffinity(g_tpcc_config.WarehouseToCoreId(warehouse_id));
    }
  } else {
    // Bohm partitioning

    state->nodes = NodeBitmap();
    auto offset = Tuple<int>(0);
    if (g_tpcc_config.IsWarehousePinnable()) {
      txn_indexop_affinity = warehouse_id - 1;
      TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, Tuple<int>>(
          &offset, KeyParam<Warehouse>(warehouse_key), KeyParam<District>(district_key));

      offset = Tuple<int>(2);
      txn_indexop_affinity = customer_warehouse_id - 1;
      TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, Tuple<int>>(
          &offset, KeyParam<Customer>(customer_key));
    } else {
      ASSERT_BOHM_CONT;

      txn_indexop_affinity = 1; // Warehouse(1) partition
      TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, Tuple<int>>(
          &offset, KeyParam<Warehouse>(warehouse_key));

      offset = Tuple<int>(1);
      txn_indexop_affinity = (district_id - 1 + kBohmExtraPartitions) % NodeConfiguration::g_nr_threads;
      TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, Tuple<int>>(
          &offset, KeyParam<District>(district_key));

      offset = Tuple<int>(2);
      txn_indexop_affinity = (customer_district_id - 1 + kBohmExtraPartitions) % NodeConfiguration::g_nr_threads;
      TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, Tuple<int>>(
          &offset, KeyParam<Customer>(customer_key));
    }

    state->nodes.MergeOrAdd(1, 0x07);
  }
}

void PaymentTxn::Run()
{
  if (Client::g_enable_granola)
    PrepareImpl();

  auto &conf = util::Instance<NodeConfiguration>();
  for (auto &p: state->nodes) {
    auto [node, bitmap] = p;
    std::array<int, 2> filters;

    if (conf.node_id() == node) {
      state->warehouse_future = UpdateForKey(
          node, state->warehouse,
          [](const auto &ctx, VHandle *row) {
            auto &[state, index_handle, payment_amount] = ctx;
            TxnRow vhandle = index_handle(state->warehouse);
            auto w = vhandle.Read<Warehouse::CommonValue>();
            w.w_ytd += payment_amount;
            vhandle.Write(w);
            ClientBase::OnUpdateRow(state->warehouse);
          }, payment_amount);

      state->district_future = UpdateForKey(
          node, state->district,
          [](const auto &ctx, VHandle *row) {
            auto &[state, index_handle, payment_amount] = ctx;
            TxnRow vhandle = index_handle(state->district);
            auto d = vhandle.Read<District::CommonValue>();
            d.d_ytd += payment_amount;
            vhandle.Write(d);
            ClientBase::OnUpdateRow(state->district);
          }, payment_amount);


      state->customer_future = UpdateForKey(
          node, state->customer,
          [](const auto &ctx, VHandle *row) {
            auto &[state, index_handle, payment_amount] = ctx;
            TxnRow vhandle = index_handle(state->customer);
            auto c = vhandle.Read<Customer::CommonValue>();
            c.c_balance -= payment_amount;
            c.c_ytd_payment += payment_amount;
            c.c_payment_cnt++;
            vhandle.Write(c);
            ClientBase::OnUpdateRow(state->customer);
          }, payment_amount);

      if (!state->warehouse_future.has_callback()
          && !state->district_future.has_callback()
          && !state->customer_future.has_callback())
        continue;

      if (!Client::g_enable_granola) {
        filters = {0x03, 0};
      } else {
        filters = {0x01, 0x02};
      }

      for (auto filter: filters) {
        if (filter == 0) continue;
        auto aff = std::numeric_limits<uint64_t>::max();
        if (filter == 0x01) {
          aff = warehouse_id - 1;
        } else if (filter == 0x02) {
          aff = customer_warehouse_id - 1;
        } else {
          if (g_tpcc_config.IsWarehousePinnable())
            aff = g_tpcc_config.WarehouseToCoreId(warehouse_id);
        }

        root->Then(
            MakeContext(payment_amount, bitmap, filter), node,
            [](const auto &ctx, auto args) -> Optional<VoidValue> {
              auto &[state, index_handle, payment_amount, bitmap, filter] = ctx;

              probes::TpccPayment{0, __builtin_popcount(bitmap), (int) state->warehouse->object_coreid()}();

              if ((bitmap & 0x01) && (filter & 0x01)) {
                state->warehouse_future.Invoke(state, index_handle, payment_amount);
              }

              if ((bitmap & 0x02) && (filter & 0x01)) {
                state->district_future.Invoke(state, index_handle, payment_amount);
              }

              if ((bitmap & 0x04) && (filter & 0x02)) {
                state->customer_future.Invoke(state, index_handle, payment_amount);
              }
              return nullopt;
            },
            aff);
      }
    } else {
      root->Then(
          MakeContext(bitmap, payment_amount), node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle, bitmap, payment_amount] = ctx;

            if (bitmap & 0x01) {
              TxnRow vhandle = index_handle(state->warehouse);
              auto w = vhandle.Read<Warehouse::CommonValue>();
              w.w_ytd += payment_amount;
              vhandle.Write(w);
              ClientBase::OnUpdateRow(state->warehouse);
            }

            if (bitmap & 0x02) {
              TxnRow vhandle = index_handle(state->district);
              auto d = vhandle.Read<District::CommonValue>();
              d.d_ytd += payment_amount;
              vhandle.Write(d);
              ClientBase::OnUpdateRow(state->district);
            }

            if (bitmap & 0x04) {
              TxnRow vhandle = index_handle(state->customer);
              auto c = vhandle.Read<Customer::CommonValue>();
              c.c_balance -= payment_amount;
              c.c_ytd_payment += payment_amount;
              c.c_payment_cnt++;
              vhandle.Write(c);
              ClientBase::OnUpdateRow(state->customer);
            }

            return nullopt;
          });
    }
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
