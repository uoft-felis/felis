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

using felis::VHandle;

struct NewOrderState {
  struct {
    VHandle *district;
    VHandle *stocks[15];
  } rows;
};

using namespace felis;

class NewOrderTxn : public Txn<NewOrderState>, public NewOrderStruct, public Util {
  Client *client;
 public:
  NewOrderTxn(Client *client);
  void Run() override final;
};

NewOrderTxn::NewOrderTxn(Client *client)
    : client(client),
      NewOrderStruct(client->GenerateTransactionInput<NewOrderStruct>())
{
  INIT_ROUTINE_BRK(4096);

  PromiseProc _;
  auto district_key = District::Key::New(warehouse_id, district_id);

  int p = partition(warehouse_id);
  _ >> T(IndexContext<District>(warehouse_id, district_id), p, TxnIndexLookupOp<>)
    >> T(Context(district_key), p, [](auto ctx, Tuple<VHandle *> vhandle) -> Optional<VoidValue> {
        return nullopt;
      });

#if 0
  _ >> T(Context(warehouse_id, district_id), p, [](auto ctx, auto _) -> Optional<VoidValue> {
      uint warehouse_id;
      uint district_id;
      State state = ctx.state();
      TxnHandle handle = ctx.handle();
      ctx.Unpack(warehouse_id, district_id);

      state->rows.district =
          handle(relation(TableType::District, warehouse_id))
          .Lookup(District::Key::New(warehouse_id, district_id).EncodeFromRoutine());
      handle(state->rows.district).AppendNewVersion();

      VHandle *oorder_range =
          handle(relation(TableType::OOrder, warehouse_id))
          .Lookup(OOrder::Key::New(warehouse_id, district_id, std::numeric_limits<int>::max()).EncodeFromRoutine());
      handle(oorder_range).AppendNewVersion();

      return nullopt;
    });

  for (uint i = 0; i < nr_items; i++) {
    int p = partition(supplier_warehouse_id[i]);
    printf("partition %d\n", p);
    _ >> T(Context(i, supplier_warehouse_id[i], item_id[i]), p, [](auto ctx, auto _) -> Optional<VoidValue> {
        uint i;
        uint warehouse_id;
        uint item_id;
        State state = ctx.state();
        TxnHandle handle = ctx.handle();
        ctx.Unpack(i, warehouse_id, item_id);

        state->rows.stocks[i] =
            handle(relation(TableType::Stock, warehouse_id))
            .Lookup(Stock::Key::New(warehouse_id, item_id).EncodeFromRoutine());
        handle(state->rows.stocks[i]).AppendNewVersion();

        return nullopt;
      });
  }
#endif
}

void NewOrderTxn::Run()
{
}

}

namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::NewOrder), Client *>::Construct(tpcc::Client * client)
{
  return new tpcc::NewOrderTxn(client);
}

}
