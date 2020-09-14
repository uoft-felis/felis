#include "benchmark/ycsb/ycsb_priority.h"

namespace ycsb {

void GeneratePriorityTxn() {
  if (!felis::NodeConfiguration::g_priority_txn)
    return;
  int txn_per_epoch = felis::PriorityTxnService::g_nr_priority_txn;
  for (auto i = 1; i < felis::EpochClient::g_max_epoch; ++i) {
    for (auto j = 1; j <= txn_per_epoch; ++j) {
      felis::PriorityTxn txn(&MWTxn_Run);
      txn.epoch = i;
      txn.delay = 2200 * felis::PriorityTxnService::g_interval_priority_txn * j;
      util::Instance<felis::PriorityTxnService>().PushTxn(&txn);
    }
  }
  logger->info("[Pri-init] pri txns pre-generated, {} per epoch", txn_per_epoch);
}

template <>
MWTxnInput Client::GenerateTransactionInput<MWTxnInput>()
{
  MWTxnInput in;
  in.nr = 2;

  for (int i = 0; i < in.nr; i++) {
 again:
    auto id = rand.next() % g_table_size;
    // Check duplicates. Got this from NewOrder.
    for (int j = 0; j < i; j++)
      if (in.keys[j] == id) goto again;
    in.keys[i] = id;
  }
  return in;
}

std::string format_sid(uint64_t sid)
{
  return "node_id " + std::to_string(sid & 0x000000FF) +
         ", epoch " + std::to_string(sid >> 32) +
         ", txn sequence " + std::to_string(sid >> 8 & 0xFFFFFF);
}

bool MWTxn_Run(felis::PriorityTxn *txn)
{
  auto core_id = go::Scheduler::CurrentThreadPoolId() - 1;

  // record pri txn init queue time
  uint64_t start_tsc = __rdtsc();
  uint64_t diff = start_tsc - (txn->delay + felis::PriorityTxnService::g_tsc);
  felis::probes::PriInitQueueTime{diff / 2200, txn->epoch, txn->delay}();

  // generate txn input
  MWTxnInput txnInput = dynamic_cast<ycsb::Client*>
      (felis::EpochClient::g_workload_client)->GenerateTransactionInput<MWTxnInput>();
  std::vector<Ycsb::Key> keys;
  for (int i = 0; i < txnInput.nr; ++i) {
    keys.push_back(Ycsb::Key::New(txnInput.keys[i]));
  }
  // hack, subtract random gen time
  start_tsc = __rdtsc();

  // init
  std::vector<felis::VHandle*> rows;
  if (!(txn->InitRegisterUpdate<ycsb::Ycsb>(keys, rows))) {
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
  uint64_t fail = fail_tsc - start_tsc, succ = succ_tsc - fail_tsc;
  // fail = ((util::Instance<felis::PriorityTxnService>().GetMaxProgress() -  util::Instance<felis::PriorityTxnService>().GetProgress(core_id)) & 0xFFFFFFFF) >> 8;
  // debug(TRACE_PRIORITY "Priority txn {:p} (MW) - Init() succuess, sid {} - {}", (void *)txn, txn->serial_id(), format_sid(txn->serial_id()));
  felis::probes::PriInitTime{succ / 2200, fail / 2200, fail_cnt, txn->serial_id()}();


  struct Context {
    int nr;
    uint64_t key;
    felis::VHandle* row;
    felis::PriorityTxn *txn;
  };

  // issue promise
  txn->piece_count.store(txnInput.nr);
  for (int i = 0; i < txnInput.nr; ++i) {
    auto lambda =
        [](std::tuple<Context> capture) {
          auto [ctx] = capture;
          auto piece_id = ctx.txn->piece_count.fetch_sub(1);

          // record exec queue time
          if (piece_id == ctx.nr) {
            auto queue_tsc = __rdtsc();
            auto diff = queue_tsc - ctx.txn->measure_tsc;
            felis::probes::PriExecQueueTime{diff / 2200, ctx.txn->serial_id()}();
            ctx.txn->measure_tsc = queue_tsc;
          }

          auto row = ctx.txn->Read<Ycsb::Value>(ctx.row);
          row.v.resize_junk(90);
          ctx.txn->Write(ctx.row, row);

          // record exec time
          if (piece_id == 1) {
            auto exec_tsc = __rdtsc();
            auto exec = exec_tsc - ctx.txn->measure_tsc;
            auto total = exec_tsc - (ctx.txn->delay + felis::PriorityTxnService::g_tsc);
            felis::probes::PriExecTime{exec / 2200, total / 2200, ctx.txn->serial_id()}();
          }
        };
    Context ctx{txnInput.nr,
                txnInput.keys[i],
                rows[i],
                txn};
    int core_id = util::Instance<felis::PriorityTxnService>().GetFastestCore();
    txn->IssuePromise(ctx, lambda, core_id);
    // debug(TRACE_PRIORITY "Priority txn {:p} (MW) - Issued lambda into PQ", (void *)txn);
  }

  // record exec issue time
  uint64_t issue_tsc = __rdtsc();
  diff = issue_tsc - succ_tsc;
  txn->measure_tsc = issue_tsc;
  felis::probes::PriExecIssueTime{diff / 2200, txn->serial_id()}();

  return txn->Commit();
}

}
