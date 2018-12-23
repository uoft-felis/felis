#include "tpcc.h"
#include "txn.h"
#include "promise.h"
#include <tuple>

namespace tpcc {

struct NewOrderStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_id;
  uint nr_items;

  ulong new_order_id;

  uint ts_now;

  uint item_id[15];
  uint supplier_warehouse_id[15];
  uint order_quantities[15];
};

template <>
NewOrderStruct Client::GenerateTransactionInput<NewOrderStruct>()
{
  NewOrderStruct s;
  s.warehouse_id = PickWarehouse();
  s.district_id = PickDistrict();
  s.customer_id = GetCustomerId();
  s.nr_items = RandomNumber(5, 15);
  s.new_order_id = PickNewOrderId(s.warehouse_id, s.district_id);

  for (int i = 0; i < s.nr_items; i++) {
 again:
    auto id = GetItemId();
    // Check duplicates. This is our customization to TPC-C because we cannot
    // handle duplicate keys.
    //
    // In practice, this should be handle by the client application.
    for (int j = 0; j < i; j++)
      if (s.item_id[j] == id) goto again;

    s.item_id[i] = id;
    s.order_quantities[i] = RandomNumber(1, 10);
    if (nr_warehouses() == 1
        || RandomNumber(1, 100) > int(kNewOrderRemoteItem * 100)) {
      s.supplier_warehouse_id[i] = s.warehouse_id;
    } else {
      s.supplier_warehouse_id[i] =
          RandomNumberExcept(1, nr_warehouses(), s.warehouse_id);
    }
  }
  s.ts_now = GetCurrentTime();
  return s;
}

using namespace felis;

struct NewOrderState {
  struct {
    VHandle *district;
    VHandle *stocks[15];
  } rows;
};

class NewOrderTxn : public Txn<NewOrderState>, public NewOrderStruct, public Util {
  Client *client;
 public:
  NewOrderTxn(Client *client, uint64_t serial_id);
  void Run() override final;
};

NewOrderTxn::NewOrderTxn(Client *client, uint64_t serial_id)
    : Txn<NewOrderState>(serial_id),
      NewOrderStruct(client->GenerateTransactionInput<NewOrderStruct>()),
      client(client)
{
  INIT_ROUTINE_BRK(4096);

  auto district_key = District::Key::New(warehouse_id, district_id);

  int node = warehouse_to_node_id(warehouse_id);
  int lookup_node = client->warehouse_to_lookup_node_id(warehouse_id);

  // Looks like we only need this when FastIdGen is off? In a distributed
  // environment, it makes sense to assume FastIdGen is always on.
  /*

  proc >> TxnLookup<District>(lookup_node, district_key)
       >> TxnSetupVersion(
           node,
           [](const auto &ctx, auto *handle) {
             ctx.template _<0>()->rows.district = handle;
           });

  */

  for (auto i = 0; i < nr_items; i++) {
    auto stock_key = Stock::Key::New(supplier_warehouse_id[i], item_id[i]);
    node = warehouse_to_node_id(supplier_warehouse_id[i]);
    lookup_node = client->warehouse_to_lookup_node_id(supplier_warehouse_id[i]);

    proc >> TxnLookup<Stock>(lookup_node, stock_key)
         >> TxnSetupVersion(
             node,
             [](const auto &ctx, auto *handle) {
               auto &[state, _1, _2, i] = ctx;
               state->rows.stocks[i] = handle;
             },
             i);
  }
}

void NewOrderTxn::Run()
{
  for (auto i = 0; i < nr_items; i++) {
    int node = warehouse_to_node_id(supplier_warehouse_id[i]);
    proc >> TxnProc(
        node,
        [](const auto &ctx, auto args) -> Optional<VoidValue> {
          auto &[state, index_handle,
                 i, ol_quantity, ol_supply_warehouse, warehouse_id] = ctx;
          TxnVHandle vhandle = index_handle(state->rows.stocks[i]);
          auto stock = vhandle.Read<Stock::Value>();
          if (stock.s_quantity - ol_quantity < 10) {
            stock.s_quantity += 91;
          }
          stock.s_quantity -= ol_quantity;
          stock.s_ytd += ol_quantity;
          stock.s_remote_cnt += (ol_supply_warehouse == warehouse_id) ? 0 : 1;

          vhandle.Write(stock);
          return nullopt;
        },
        i, order_quantities[i], supplier_warehouse_id[i], warehouse_id);
  }
}

}

namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::NewOrder), Client *, uint64_t>::Construct(tpcc::Client * client, uint64_t serial_id)
{
  return new tpcc::NewOrderTxn(client, serial_id);
}

}
