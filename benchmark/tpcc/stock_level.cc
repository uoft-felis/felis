#include "stock_level.h"
#include "pwv_graph.h"

namespace tpcc {

using namespace felis;

template <>
StockLevelStruct ClientBase::GenerateTransactionInput<StockLevelStruct>()
{
  StockLevelStruct s;
  s.warehouse_id = PickWarehouse();
  s.district_id = PickDistrict();
  s.threshold = RandomNumber(10, 20);
  return s;
}

class StockLevelTxn : public Txn<StockLevelState>, public StockLevelStruct {
  Client *client;
 public:
  StockLevelTxn(Client *client, uint64_t serial_id)
      : Txn<StockLevelState>(serial_id),
        StockLevelStruct(client->GenerateTransactionInput<StockLevelStruct>()),
        client(client)
  {}

  void PrepareInsert() override final;
  void Prepare() override final;
  void Run() override final;
};

void StockLevelTxn::PrepareInsert()
{
  auto &mgr = util::Instance<TableManager>();
  auto auto_inc_zone = warehouse_id * 10 + district_id;
  state->current_oid = mgr.Get<OOrder>().GetCurrentAutoIncrement(auto_inc_zone);
  // client->get_execution_locality_manager().PlanLoad(Config::WarehouseToCoreId(warehouse_id), 150);
}

void StockLevelTxn::Prepare()
{
  auto &mgr = util::Instance<TableManager>();
  auto lower = std::max<int>(state->current_oid - (20 << 8), 0);
  auto upper = std::max<int>(state->current_oid, 0);

  auto ol_start = OrderLine::Key::New(warehouse_id, district_id, lower, 0);
  auto ol_end = OrderLine::Key::New(warehouse_id, district_id, upper, 0);

  INIT_ROUTINE_BRK(8 << 10);

  state->n = 0;
  PWVGraph::Resource *res = nullptr;
  int nr_res = 0;

  if (Client::g_enable_pwv) {
    res = (PWVGraph::Resource *) alloca(sizeof(PWVGraph::Resource) * 60); // maximum 60 resources
  }

  for (auto it = mgr.Get<OrderLine>().IndexSearchIterator(
           ol_start.EncodeFromRoutine(),
           ol_end.EncodeFromRoutine()); it->IsValid(); it->Next()) {
    if (it->row()->ShouldScanSkip(serial_id())) continue;
    state->items.at(state->n++) = it->row();
    OrderLine::Key ol_key;
    ol_key.Decode(&it->key());

    if (Client::g_enable_pwv && ol_key.ol_number == 1) {
      res[nr_res++] = PWVGraph::VHandleToResource(it->row());
    }
  }

  if (VHandleSyncService::g_lock_elision && Client::g_enable_pwv) {
    auto &gm = util::Instance<PWVGraphManager>();
    if (g_tpcc_config.IsWarehousePinnable()) {
      gm[warehouse_id - 1]->ReserveEdge(serial_id(), nr_res + 1);

      gm[warehouse_id - 1]->AddResources(serial_id(), res, nr_res);
      gm[warehouse_id - 1]->AddResource(
          serial_id(), &ClientBase::g_pwv_stock_resources[warehouse_id - 1]);
    } else {
      int parts[2] = {
        g_tpcc_config.PWVDistrictToCoreId(district_id, 10),
        0,
      };
      gm[parts[0]]->ReserveEdge(serial_id(), nr_res);
      gm[parts[1]]->ReserveEdge(serial_id());

      gm[parts[0]]->AddResources(serial_id(), res, nr_res);
      gm[parts[1]]->AddResource(
          serial_id(), &ClientBase::g_pwv_stock_resources[0]);
    }
  }
}

void StockLevelTxn::Run()
{
  static constexpr auto ScanOrderLine = [](
      auto state, auto index_handle, int warehouse_id, int district_id) -> void {
    for (int i = 0; i < state->n; i++) {
      state->item_ids[i] = index_handle(state->items[i]).template Read<OrderLine::Value>().ol_i_id;
    }
    std::sort(state->item_ids.begin(), state->item_ids.begin() + state->n);
  };

  static constexpr auto ScanStocks = [](
      auto state, auto index_handle, int warehouse_id, int district_id, int threshold) -> void {
    auto &mgr = util::Instance<TableManager>();
    // Distinct item keys
    int last = -1;
    int result = 0;

    for (int i = 0; i < state->n; i++) {
      auto id = state->item_ids[i];
      if (last == id) continue;
      last = id;

      uint8_t stk_data[4UL << 10];
      go::RoutineScopedData sb(mem::Brk::New(stk_data, 4UL << 10));

      auto stock_key = Stock::Key::New(warehouse_id, id);
      auto stock_value = index_handle(mgr.Get<Stock>().Search(stock_key.EncodeFromRoutine()))
                         .template Read<Stock::Value>();
      if (stock_value.s_quantity < threshold) result++;
    }
  };

  auto aff = std::numeric_limits<uint64_t>::max();
  state->n = 0;

  if (!Options::kEnablePartition || g_tpcc_config.IsWarehousePinnable()) {
    if (g_tpcc_config.IsWarehousePinnable()) {
      aff = Config::WarehouseToCoreId(warehouse_id);
    }
    root->Then(
        MakeContext(warehouse_id, district_id, threshold), 0,
        [](const auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, index_handle, warehouse_id, district_id, threshold] = ctx;
          ScanOrderLine(state, index_handle, warehouse_id, district_id);
          ScanStocks(state, index_handle, warehouse_id, district_id, threshold);
          return nullopt;
        },
        aff);
    if (!Client::g_enable_granola) {
      root->AssignSchedulingKey(serial_id() + (1024ULL << 8));
    }
  } else { // kEnablePartition && !IsWarehousePinnable()
    state->barrier = FutureValue<void>();
    aff = g_tpcc_config.PWVDistrictToCoreId(district_id, 10);
    root->Then(
        MakeContext(warehouse_id, district_id), 0,
        [](const auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, index_handle, warehouse_id, district_id] = ctx;
          ScanOrderLine(state, index_handle, warehouse_id, district_id);
          state->barrier.Signal();
          return nullopt;
        },
        aff);

    root->Then(
        MakeContext(warehouse_id, district_id, threshold), 0,
        [](const auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, index_handle, warehouse_id, district_id, threshold] = ctx;
          state->barrier.Wait();
          ScanStocks(state, index_handle, warehouse_id, district_id, threshold);
          return nullopt;
        },
        0); // Stock(0) partition
  }
}

}

namespace util {

using namespace felis;
using namespace tpcc;

template <>
BaseTxn *Factory<BaseTxn, static_cast<int>(TxnType::StockLevel), Client *, uint64_t>::Construct(tpcc::Client * client, uint64_t serial_id)
{
  return new StockLevelTxn(client, serial_id);
}

}
