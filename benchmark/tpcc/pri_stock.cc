#include <numeric>
#include "pri_stock.h"

namespace tpcc {

template <>
PriStockStruct ClientBase::GenerateTransactionInput<PriStockStruct>()
{
  PriStockStruct in;
  in.warehouse_id = PickWarehouse();
  in.nr_items = RandomNumber(1, PriStockStruct::kStockMaxItems);

  for (int i = 0; i < in.nr_items; i++) {
 again:
    auto id = GetItemId();
    // Check duplicates. Got this from NewOrder.
    for (int j = 0; j < i; j++)
      if (in.detail.item_id[j] == id) goto again;
    in.detail.item_id[i] = id;
    in.detail.stock_quantities[i] = RandomNumber(50, 100);
  }
  return in;
}

class PriStockTxn : public Txn<PriStockState>, public PriStockStruct {
  Client *client;
 public:
  PriStockTxn(Client *client, uint64_t serial_id)
      : Txn<PriStockState>(serial_id),
        PriStockStruct(client->GenerateTransactionInput<PriStockStruct>()),
        client(client)
  {}
  void Run() override final;
  void Prepare() override final {
    if (!Client::g_enable_granola)
      PrepareImpl();
  }
  void PrepareInsert() override final {
    if (!Client::g_enable_granola)
      PrepareInsertImpl();
  }

  void PrepareImpl();
  void PrepareInsertImpl();
};

void PriStockTxn::PrepareInsertImpl()
{}


void PriStockTxn::PrepareImpl()
{
  Stock::Key stock_keys[kStockMaxItems];
  for (int i = 0; i < nr_items; i++) {
    stock_keys[i] =
        Stock::Key::New(warehouse_id, detail.item_id[i]);
  }

  INIT_ROUTINE_BRK(8192);

  state->stocks_nodes =
      TxnIndexLookup<TpccSliceRouter, PriStockState::StocksLookupCompletion, void>(
          nullptr,
          KeyParam<Stock>(stock_keys, nr_items));
}

void PriStockTxn::Run()
{
  if (Client::g_enable_granola) {
    // Search the index. AppendNewVersion() is automatically Nop when
    // g_enable_granola is on.
    PrepareInsertImpl();
    PrepareImpl();
  }

  int nr_nodes = util::Instance<NodeConfiguration>().nr_nodes();

  struct {
    unsigned int quantities[PriStockStruct::kStockMaxItems];
    int warehouse;
  } params;

  params.warehouse = warehouse_id;
  for (auto i = 0; i < nr_items; i++) {
    params.quantities[i] = detail.stock_quantities[i];
  }

  for (auto &p: state->stocks_nodes) {
    auto [node, bitmap] = p;

    // Granola needs partitioning, that's why we need to split this into
    // multiple pieces even if this piece could totally be executed on the same
    // node.
    std::array<int, PriStockStruct::kStockMaxItems> warehouse_filters;
    int nr_warehouse_filters = 0;
    warehouse_filters.fill(0);

    if (!Client::g_enable_granola) {
      warehouse_filters[nr_warehouse_filters++] = -1; // Don't filter
    } else {
      for (int i = 0; i < PriStockStruct::kStockMaxItems; i++) {
        if ((bitmap & (1 << i)) == 0) continue;

        auto supp_warehouse = warehouse_id;
        if (std::find(warehouse_filters.begin(), warehouse_filters.begin() + nr_warehouse_filters,
                      supp_warehouse) == warehouse_filters.begin() + nr_warehouse_filters) {
          warehouse_filters[nr_warehouse_filters++] = supp_warehouse;
        }
      }
    }

    for (int f = 0; f < nr_warehouse_filters; f++) {
      auto filter = warehouse_filters[f];
      auto aff = std::numeric_limits<uint64_t>::max();

      if (filter > 0) aff = filter - 1;

      root->Then(
          MakeContext(bitmap, params, filter), node,
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle, bitmap, params, filter] = ctx;
            for (int i = 0; i < PriStockStruct::kStockMaxItems; i++) {
              if ((bitmap & (1 << i)) == 0) continue;
              if (filter > 0 && params.warehouse != filter) continue;

              TxnVHandle vhandle = index_handle(state->stocks[i]);
              auto stock = vhandle.Read<Stock::Value>();
              stock.s_quantity += params.quantities[i];

              vhandle.Write(stock);
              ClientBase::OnUpdateRow(state->stocks[i]);
            }
            return nullopt;
          },
          aff);
    }
  }

  if (Client::g_enable_granola)
    root_promise()->AssignAffinity(warehouse_id - 1);
}

}

namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::PriStock), Client *, uint64_t>::Construct(tpcc::Client * client, uint64_t serial_id)
{
  return new tpcc::PriStockTxn(client, serial_id);
}

}
