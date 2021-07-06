#include "benchmark/ycsb/ycsb_priority.h"

namespace ycsb {

using namespace felis;

void GeneratePriorityTxn() {
  if (!NodeConfiguration::g_priority_txn)
    return;
  int txn_per_epoch = PriorityTxnService::g_nr_priority_txn;
  for (auto i = 1; i < EpochClient::g_max_epoch; ++i) {
    for (auto j = 1; j <= txn_per_epoch; ++j) {
      PriorityTxn txn(&MWTxn_Run);
      txn.epoch = i;
      auto interval = PriorityTxnService::g_interval_priority_txn;
      txn.delay = static_cast<uint64_t>(static_cast<double>(interval * j) * 2.2);
      util::Instance<PriorityTxnService>().PushTxn(&txn);
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

bool MWTxn_Run(PriorityTxn *txn)
{
  // record pri txn init queue time
  uint64_t start_tsc = __rdtsc();
  uint64_t init_q = start_tsc - (txn->delay + PriorityTxnService::g_tsc);
  INIT_ROUTINE_BRK(4096);

  // generate txn input
  MWTxnInput input = dynamic_cast<ycsb::Client*>
      (EpochClient::g_workload_client)->GenerateTransactionInput<MWTxnInput>();
  std::vector<Ycsb::Key> keys(input.nr);
  for (int i = 0; i < input.nr; ++i) {
    keys[i] = Ycsb::Key::New(input.keys[i]);
  }
  // hack, subtract random gen time
  start_tsc = __rdtsc();

  // register update
  std::vector<VHandle*> rows(input.nr);
  for (int i = 0; i < input.nr; ++i) {
    abort_if(!txn->InitRegisterUpdate<ycsb::Ycsb>(keys[i], rows[i]), "init register failed!");
  }
  // init
  uint64_t fail_tsc = start_tsc;
  int fail_cnt = 0;
  while (!txn->Init()) {
    fail_tsc = __rdtsc();
    ++fail_cnt;
  }

  uint64_t succ_tsc = __rdtsc();
  uint64_t fail = fail_tsc - start_tsc, succ = succ_tsc - fail_tsc;
  txn->measure_tsc = succ_tsc;
  probes::PriInitQueueTime{init_q, txn->serial_id()}(); // recorded before
  probes::PriInitTime{succ / 2200, fail / 2200, fail_cnt, txn->serial_id()}();

  struct Context {
    int nr;
    uint64_t key;
    VHandle* row;
    PriorityTxn *txn;
  };

  // issue promise
  txn->piece_count.store(input.nr);
  for (int i = 0; i < input.nr; ++i) {
    auto lambda =
        [](std::tuple<Context> capture) {
          auto [ctx] = capture;
          auto piece_id = ctx.txn->piece_count.fetch_sub(1);
          INIT_ROUTINE_BRK(4096);

          // record exec queue time
          if (piece_id == ctx.nr) {
            auto queue_tsc = __rdtsc();
            auto diff = queue_tsc - ctx.txn->measure_tsc;
            probes::PriExecQueueTime{diff / 2200, ctx.txn->serial_id()}();
            ctx.txn->measure_tsc = queue_tsc;
          }

          auto row = ctx.txn->Read<Ycsb::Value>(ctx.row);
          row.v.resize_junk(90);
          ctx.txn->Write(ctx.row, row);

          // record exec time
          if (piece_id == 1) {
            auto exec_tsc = __rdtsc();
            auto exec = exec_tsc - ctx.txn->measure_tsc;
            auto total = exec_tsc - (ctx.txn->delay + PriorityTxnService::g_tsc);
            probes::PriExecTime{exec / 2200, total / 2200, ctx.txn->serial_id()}();
          }
        };
    Context ctx{input.nr, input.keys[i], rows[i], txn};
    txn->IssuePromise(ctx, lambda);
    // trace(TRACE_PRIORITY "Priority txn {:p} (MW) - Issued lambda into PQ", (void *)txn);
  }

  // record acquired SID's difference from current max progress
  uint64_t max_prog = util::Instance<PriorityTxnService>().GetMaxProgress() >> 8;
  uint64_t seq = txn->serial_id() >> 8;
  int64_t diff_to_max_progress = seq - max_prog;
  probes::Distance{diff_to_max_progress, txn->serial_id()}();

  return txn->Commit();
}

}
