#include "benchmark/tpcc/tpcc_priority.h"

namespace tpcc {

void GeneratePriorityTxn() {
  if (!NodeConfiguration::g_priority_txn)
    return;
  int txn_per_epoch = felis::PriorityTxnService::g_nr_priority_txn;
  for (auto i = 1; i < felis::EpochClient::g_max_epoch; ++i) {
    for (auto j = 1; j <= txn_per_epoch; ++j) {
      felis::PriorityTxn *txn = new felis::PriorityTxn(&StockTxn_Run);
      txn->epoch = i;
      txn->delay = 2200 * felis::PriorityTxnService::g_interval_priority_txn * j;
      util::Instance<felis::PriorityTxnService>().PushTxn(txn);
    }
  }
  logger->info("[Pri-init] pri txns pre-generated, {} per epoch", txn_per_epoch);
}

template <>
StockTxnInput ClientBase::GenerateTransactionInput<StockTxnInput>()
{
  StockTxnInput in;
  in.warehouse_id = PickWarehouse();
  in.nr_items = 20; // RandomNumber(1, StockTxnInput::kStockMaxItems);

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

std::string format_sid(uint64_t sid)
{
  return "node_id " + std::to_string(sid & 0x000000FF) +
         ", epoch " + std::to_string(sid >> 32) +
         ", txn sequence " + std::to_string(sid >> 8 & 0xFFFFFF);
}

bool StockTxn_Run(felis::PriorityTxn *txn)
{
  // record pri txn init queue time
  uint64_t start_tsc = __rdtsc();
  uint64_t diff = start_tsc - (txn->delay + felis::PriorityTxnService::g_tsc);
  felis::probes::PriInitQueueTime{diff / 2200, txn->epoch, txn->delay}();

  // generate txn input
  StockTxnInput txnInput = dynamic_cast<tpcc::Client*>
      (felis::EpochClient::g_workload_client)->GenerateTransactionInput<StockTxnInput>();
  std::vector<Stock::Key> stock_keys;
  for (int i = 0; i < txnInput.nr_items; ++i) {
    stock_keys.push_back(Stock::Key::New(txnInput.warehouse_id,
                                         txnInput.detail.item_id[i]));
  }
  // hack, subtract random gen time
  start_tsc = __rdtsc();

  // init
  std::vector<felis::VHandle*> stock_rows;
  if (!(txn->InitRegisterUpdate<tpcc::Stock>(stock_keys, stock_rows))) {
    // debug(TRACE_PRIORITY "init register failed!");
    return false;
  }
  uint64_t fail_tsc = start_tsc;
  int fail_cnt = 0;
  while (!txn->Init()) {
    fail_tsc = __rdtsc();
    ++fail_cnt;
  }
  uint64_t succ_tsc = __rdtsc();
  txn->measure_tsc = succ_tsc;
  uint64_t fail = fail_tsc - start_tsc, succ = succ_tsc - fail_tsc;
  // debug(TRACE_PRIORITY "Priority txn {:p} (stock) - Init() succuess, sid {} - {}", (void *)txn, txn->serial_id(), format_sid(txn->serial_id()));
  felis::probes::PriInitTime{succ / 2200, fail / 2200, fail_cnt, txn->serial_id()}();


  struct Context {
    uint warehouse_id;
    uint nr_items;
    felis::PriorityTxn *txn;
    uint stock_quantities[StockTxnInput::kStockMaxItems];
    felis::VHandle* stock_rows[StockTxnInput::kStockMaxItems];
  };

  // issue promise
  int core_id = util::Instance<felis::PriorityTxnService>().GetFastestCore();
  // int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  uint64_t cur_prog = util::Instance<felis::PriorityTxnService>().GetProgress(core_id) >> 8;
  uint64_t seq = (txn->serial_id() >> 8);
  uint64_t diff_to_cur_progress = (seq > cur_prog) ? (seq - cur_prog) : 0;
  felis::probes::Distance{diff_to_cur_progress, txn->serial_id()}();
  // distance: how many txn sids from the acquired sid to the core's current progress

  auto lambda =
      [](std::tuple<Context> capture) {
        auto [ctx] = capture;
        auto piece_id = ctx.txn->piece_count.fetch_sub(1);

        // record exec queue time
        auto queue_tsc = __rdtsc();
        auto diff = queue_tsc - ctx.txn->measure_tsc;
        felis::probes::PriExecQueueTime{diff / 2200, ctx.txn->serial_id()}();
        ctx.txn->measure_tsc = queue_tsc;

        for (int i = 0; i < ctx.nr_items; ++i) {
          auto stock = ctx.txn->Read<Stock::Value>(ctx.stock_rows[i]);
          stock.s_quantity += ctx.stock_quantities[i];
          ctx.txn->Write(ctx.stock_rows[i], stock);
          ClientBase::OnUpdateRow(ctx.stock_rows[i]);
        }

        // record exec time
        auto exec_tsc = __rdtsc();
        auto exec = exec_tsc - ctx.txn->measure_tsc;
        auto total = exec_tsc - (ctx.txn->delay + felis::PriorityTxnService::g_tsc);
        felis::probes::PriExecTime{exec / 2200, total / 2200, ctx.txn->serial_id()}();
      };
  Context ctx{txnInput.warehouse_id,
              txnInput.nr_items,
              txn};
  memcpy(ctx.stock_quantities, txnInput.detail.stock_quantities, sizeof(uint) * ctx.nr_items);
  memcpy(ctx.stock_rows, &stock_rows[0], sizeof(felis::VHandle*) * ctx.nr_items);
  txn->IssuePromise(ctx, lambda, core_id);
  // debug(TRACE_PRIORITY "Priority txn {:p} (stock) - Issued lambda into PQ", (void *)txn);

  // record exec issue time
  uint64_t issue_tsc = __rdtsc();
  diff = issue_tsc - succ_tsc;
  txn->measure_tsc = issue_tsc;
  felis::probes::PriExecIssueTime{diff / 2200, txn->serial_id()}();

  return txn->Commit();
}

}
