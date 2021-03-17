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

static void ScanOrderLineIndex(
    const StockLevelTxn::State &state,
    uint64_t sid, int warehouse_id, int district_id)
{
  auto &mgr = util::Instance<TableManager>();
  auto lower = std::max<int>(state->current_oid - (20 << 8), 0);
  auto upper = std::max<int>(state->current_oid, 0);

  auto ol_start = OrderLine::Key::New(warehouse_id, district_id, lower, 0);
  auto ol_end = OrderLine::Key::New(warehouse_id, district_id, upper, 0);

  INIT_ROUTINE_BRK(4096);

  state->n = 0;
  for (auto it = mgr.Get<OrderLine>().IndexSearchIterator(
           ol_start.EncodeViewRoutine(), ol_end.EncodeViewRoutine()); it->IsValid(); it->Next()) {
    if (it->row()->ShouldScanSkip(sid)) continue;
    state->items.at(state->n++) = it->row();
    auto ol_key = it->key().ToType<OrderLine::Key>();

    // Collecting resources for PWV
    if (state->res && ol_key.ol_number == 1) {
      state->res[state->nr_res++] = PWVGraph::VHandleToResource(it->row());
    }
  }
}

void StockLevelTxn::Prepare()
{
  state->res = nullptr;
  if (!VHandleSyncService::g_lock_elision) {
    if (!Options::kTpccReadOnlyDelayQuery)
      ScanOrderLineIndex(state, serial_id(), warehouse_id, district_id);
  } else {
    if (Client::g_enable_pwv) {
      state->nr_res = 0;
      state->res = (PWVGraph::Resource *) malloc(sizeof(PWVGraph::Resource) * 60);
    }

    int part = (int) warehouse_id - 1;
    if (!g_tpcc_config.IsWarehousePinnable()) {
      part = g_tpcc_config.PWVDistrictToCoreId(district_id, 10);
    }

    root->AttachRoutine(
        MakeContext(warehouse_id, district_id, part), 1,
        [](auto &ctx) {
          auto &[state, handle, warehouse_id, district_id, p] = ctx;
          ScanOrderLineIndex(state, handle.serial_id(), warehouse_id, district_id);
          if (Client::g_enable_pwv) {
            auto &gm = util::Instance<PWVGraphManager>();
            gm[p]->ReserveEdge(handle.serial_id(), state->nr_res);
            gm[warehouse_id - 1]->ReserveEdge(handle.serial_id());

            gm[p]->AddResources(
                handle.serial_id(),
                state->res, state->nr_res);
            gm[warehouse_id - 1]->AddResource(
                handle.serial_id(),
                &ClientBase::g_pwv_stock_resources[warehouse_id - 1]);
          }
        },
        part);
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
    void *buf = alloca(32);

    for (int i = 0; i < state->n; i++) {
      auto id = state->item_ids[i];
      if (last == id) continue;
      last = id;

      // uint8_t stk_data[4UL << 10];
      // go::RoutineScopedData sb(mem::Brk::New(stk_data, 4UL << 10));

      auto stock_key = Stock::Key::New(warehouse_id, id);
      auto stock_value = index_handle(mgr.Get<Stock>().Search(stock_key.EncodeView(buf)))
                         .template Read<Stock::Value>();
      if (stock_value.s_quantity < threshold) result++;
    }
  };

  auto aff = std::numeric_limits<uint64_t>::max();
  state->n = 0;

  if (!Options::kEnablePartition || g_tpcc_config.IsWarehousePinnable()) {
    if (Options::kEnablePartition) {
      aff = Config::WarehouseToCoreId(warehouse_id);
    }

    root->AttachRoutine(
        MakeContext(warehouse_id, district_id, threshold), 0,
        [](const auto &ctx) {
          auto &[state, index_handle, warehouse_id, district_id, threshold] = ctx;

          if (Options::kTpccReadOnlyDelayQuery)
            ScanOrderLineIndex(state, index_handle.serial_id(), warehouse_id, district_id);

          ScanOrderLine(state, index_handle, warehouse_id, district_id);
          ScanStocks(state, index_handle, warehouse_id, district_id, threshold);
          if (Client::g_enable_pwv) {
            auto g = util::Instance<PWVGraphManager>().local_graph();
            g->ActivateResources(
                index_handle.serial_id(), state->res, state->nr_res);
            g->ActivateResource(
                index_handle.serial_id(), &ClientBase::g_pwv_stock_resources[warehouse_id - 1]);
            free(state->res);
          }
        },
        aff);
    if (!Client::g_enable_granola && !Client::g_enable_pwv) {
      root->AssignSchedulingKey(serial_id() + (2024ULL << 8));
    }
  } else { // kEnablePartition && !IsWarehousePinnable()
    state->barrier = FutureValue<void>();
    aff = g_tpcc_config.PWVDistrictToCoreId(district_id, 10);
    root->AttachRoutine(
        MakeContext(warehouse_id, district_id), 0,
        [](const auto &ctx) {
          auto &[state, index_handle, warehouse_id, district_id] = ctx;
          ScanOrderLine(state, index_handle, warehouse_id, district_id);
          state->barrier.Signal();

          if (Client::g_enable_pwv) {
            auto &gm = util::Instance<PWVGraphManager>();
            gm.local_graph()->ActivateResources(
                index_handle.serial_id(),
                state->res, state->nr_res);
            free(state->res);

            if (RVPInfo::FromRoutine(state->last)->indegree.fetch_sub(1) == 1)
              gm.NotifyRVPChange(index_handle.serial_id(), 0);
          }
        },
        aff);

    root->AttachRoutine(
        MakeContext(warehouse_id, district_id, threshold), 0,
        [](const auto &ctx) {
          auto &[state, index_handle, warehouse_id, district_id, threshold] = ctx;
          state->barrier.Wait();
          ScanStocks(state, index_handle, warehouse_id, district_id, threshold);

          if (Client::g_enable_pwv) {
            util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                index_handle.serial_id(), &ClientBase::g_pwv_stock_resources[0]);
          }
        },
        0); // Stock(0) partition
    state->last = root->last();
    RVPInfo::MarkRoutine(state->last);
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
