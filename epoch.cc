#include <algorithm>
#include <fstream>

#include <syscall.h>

#include "epoch.h"
#include "txn.h"
#include "log.h"
#include "vhandle.h"
#include "contention_manager.h"
#include "threshold_autotune.h"
#include "pwv_graph.h"

#include "console.h"
#include "mem.h"
#include "gc.h"
#include "opts.h"
#include "commit_buffer.h"

#include "literals.h"
#include "util/os.h"
#include "json11/json11.hpp"

namespace felis {

EpochClient *EpochClient::g_workload_client = nullptr;
bool EpochClient::g_enable_granola = false;
bool EpochClient::g_enable_pwv = false;
long EpochClient::g_corescaling_threshold = 0;
long EpochClient::g_splitting_threshold = std::numeric_limits<long>::max();
size_t EpochClient::g_txn_per_epoch = 100000;

void EpochCallback::operator()(unsigned long cnt)
{
  auto p = static_cast<int>(phase);

  trace(TRACE_COMPLETION "callback cnt {} on core {}",
        cnt, go::Scheduler::CurrentThreadPoolId() - 1);

  if (cnt == 0) {
    perf.End();
    perf.Show(label);
    printf("\n");

    // TODO: We might Reset() the PromiseAllocationService, which would free the
    // current go::Routine. Is it necessary to run some function in the another
    // go::Routine?

    static void (EpochClient::*phase_mem_funcs[])() = {
      &EpochClient::OnInsertComplete,
      &EpochClient::OnInitializeComplete,
      &EpochClient::OnExecuteComplete,
    };
    abort_if(go::Scheduler::Current()->current_routine() == &client->control,
             "Cannot call control thread from itself");

    client->control.Reset(phase_mem_funcs[p]);
    go::Scheduler::Current()->WakeUp(&client->control);
  }
}

void EpochCallback::PreComplete()
{
  if (Options::kVHandleBatchAppend || Options::kOnDemandSplitting) {
    if (phase == EpochPhase::Initialize || phase == EpochPhase::Insert) {
      util::Instance<ContentionManager>().FinalizeFlush(
          util::Instance<EpochManager>().current_epoch_nr());
    }
  }
}

volatile std::atomic_bool __g_l1_measurement = false;

static double GetCpuMHz()
{
  // Read frequencies from /proc/cpuinfo
  double freq_mhz = 0.0;
  std::ifstream fin("/proc/cpuinfo");
  while (!fin.eof()) {
    std::string line;
    std::getline(fin, line);
    if (line.substr(0, 7) == "cpu MHz") {
      freq_mhz = std::max(freq_mhz, std::stod(line.substr(line.find(": ") + 2)));
    }
  }
  logger->info("found CPU Frequency {} MHz", freq_mhz);
  return freq_mhz;
}

long EpochClient::WaitCountPerMS()
{
  volatile long s = 0;
  unsigned long long before, after;
  long wait_cnt = 0;

  before = __rdtsc();
  while (!__g_l1_measurement.load()) {
    wait_cnt++;
    if ((wait_cnt & 0x0FFFF) == 0) {
      // 4 extra cycles
      volatile int k = 4;
      while(--k);
      if (++s == 20000) {
        __g_l1_measurement = true;
      }
    }
    if (unlikely((wait_cnt & 0x7FFFFFFF) == 0)) {
      __g_l1_measurement = true;
    }
  }
  after = __rdtsc();
  double freq_mhz = GetCpuMHz();
  long dur = (after - before) / (freq_mhz * 1000);
  return wait_cnt / dur;
}

static ThresholdAutoTuneController g_threshold_autotune;

EpochClient::EpochClient()
    : control(this),
      callback(EpochCallback(this)),
      completion(0, callback),
      conf(util::Instance<NodeConfiguration>())
{
  callback.perf.End();

  best_core = std::numeric_limits<int>::max();
  best_duration = std::numeric_limits<int>::max();
  core_limit = conf.g_nr_threads;

  auto cnt_len = conf.nr_nodes() * conf.nr_nodes() * PromiseRoutineTransportService::kPromiseMaxLevels;
  unsigned long *cnt_mem = nullptr;
  EpochWorkers *workers_mem = nullptr;

  for (int t = 0; t < NodeConfiguration::g_nr_threads; t++) {
    auto d = std::div(t, mem::kNrCorePerNode);
    auto numa_node = d.quot;
    auto numa_offset = d.rem;
    if (numa_offset == 0) {
      cnt_mem = (unsigned long *) mem::AllocMemory(
          mem::Epoch,
          cnt_len * sizeof(unsigned long) * mem::kNrCorePerNode,
          numa_node);
      workers_mem = (EpochWorkers *) mem::AllocMemory(
          mem::Epoch,
          sizeof(EpochWorkers) * mem::kNrCorePerNode,
          numa_node);
    }
    per_core_cnts[t] = cnt_mem + cnt_len * numa_offset;
    workers[t] = new (workers_mem + numa_offset) EpochWorkers(t, this);
  }

  if (Options::kCoreScaling) {
    long wc = WaitCountPerMS();
    g_corescaling_threshold = Options::kCoreScaling.ToInt() * wc / 100;
    logger->info("WaitCount per ms {} , calculated CoreScaling threshold {}",
                 wc, g_corescaling_threshold);
  }

  if (Options::kOnDemandSplitting) {
    g_splitting_threshold = Options::kOnDemandSplitting.ToInt();
  }

  commit_buffer = new CommitBuffer();
}

EpochTxnSet::EpochTxnSet()
{
  auto nr_threads = NodeConfiguration::g_nr_threads;
  auto d = std::div((int) EpochClient::g_txn_per_epoch, nr_threads);
  for (auto t = 0; t < nr_threads; t++) {
    size_t nr = d.quot;
    if (t < d.rem) nr++;
    auto numa_node = t / mem::kNrCorePerNode;
    auto p = mem::AllocMemory(mem::Txn, (nr + 1) * sizeof(BaseTxn *), numa_node);
    per_core_txns[t] = new (p) TxnSet(nr);
  }
}

EpochTxnSet::~EpochTxnSet()
{
  // TODO: free these pointers via munmap().
}

void EpochClient::GenerateBenchmarks()
{
  all_txns = new EpochTxnSet[g_max_epoch - 1];
  for (auto i = 1; i < g_max_epoch; i++) {
    for (uint64_t j = 1; j <= NumberOfTxns(); j++) {
      auto d = std::div((int)(j - 1), NodeConfiguration::g_nr_threads);
      auto t = d.rem, pos = d.quot;
      BaseTxn::g_cur_numa_node = t / mem::kNrCorePerNode;
      all_txns[i - 1].per_core_txns[t]->txns[pos] = CreateTxn(GenerateSerialId(i, j));
    }
  }
}

void EpochClient::Start()
{
  // Ready to start!
  control.Reset(&EpochClient::InitializeEpoch);

  logger->info("load percentage {}%", LoadPercentage());

  perf = PerfLog();
  go::GetSchedulerFromPool(0)->WakeUp(&control);
}

uint64_t EpochClient::GenerateSerialId(uint64_t epoch_nr, uint64_t sequence)
{
  return (epoch_nr << 32)
      | (sequence << 8)
      | (conf.node_id() & 0x00FF);
}

void AllocStateTxnWorker::Run()
{
  if (EpochClient::g_enable_pwv) {
    util::Instance<PWVGraphManager>().local_graph()->Reset();
  }
  for (auto i = 0; i < client->cur_txns.load()->per_core_txns[t]->nr; i++) {
    auto txn = client->cur_txns.load()->per_core_txns[t]->txns[i];
    txn->PrepareState();
  }
  if (comp.fetch_sub(1) == 2) {
    // client->insert_lmgr.Balance();
    // client->insert_lmgr.PrintLoads();
    comp.fetch_sub(1);
  }
  client->commit_buffer->Clear(t);
}

void CallTxnsWorker::Run()
{
  auto nr_nodes = client->conf.nr_nodes();
  auto cnt = client->per_core_cnts[t];
  auto cnt_len = nr_nodes * nr_nodes * PromiseRoutineTransportService::kPromiseMaxLevels;
  std::fill(cnt, cnt + cnt_len, 0);

  set_urgent(true);
  auto pq = client->cur_txns.load()->per_core_txns[t];

  while (AllocStateTxnWorker::comp.load() != 0) _mm_pause();

  for (auto i = 0; i < pq->nr; i++) {
    auto txn = pq->txns[i];
    txn->ResetRoot();
    std::invoke(mem_func, txn);
    client->conf.CollectBufferPlan(txn->root_promise(), cnt);
  }

  bool node_finished = client->conf.FlushBufferPlan(client->per_core_cnts[t]);

  // Try to assign a default partition scheme if nothing has been
  // assigned. Because transactions are already round-robinned, there is no
  // imbalanced here.

  // These are used for corescaling, I think they are deprecated.
  long extra_offset = 0;
  for (auto i = client->core_limit; i < t; i++) {
    extra_offset += client->cur_txns.load()->per_core_txns[i]->nr;
  }
  extra_offset %= client->core_limit;

  auto &transport = util::Impl<PromiseRoutineTransportService>();
  for (size_t i = 0; i < pq->nr; i++) {
    auto txn = pq->txns[i];
    auto aff = t;

    if (client->callback.phase == EpochPhase::Execute
        && t >= client->core_limit) {
      // auto avail_nr_zones = client->core_limit / mem::kNrCorePerNode;
      // auto zone = t % avail_nr_zones;
      aff = (i + extra_offset) % client->core_limit;
    }

    auto root = txn->root_promise();
    root->AssignAffinity(aff);
    root->Complete();

    // Doesn't seems to work that well, but just in case it works well for some
    // workloads. For example, issuing takes a longer time.
    if ((i & 0xFF) == 0) transport.PrefetchInbound();
  }
  set_urgent(false);

  // Here we set the finished flag a bit earlier, so that FinishCompletion()
  // could create the ExecutionRoutine a bit earlier.
  finished = true;
  transport.FinishCompletion(0);

  // Granola doesn't support out of order scheduling. In the original paper,
  // Granola uses a single thread to issue. We use multiple threads, so here we
  // have to barrier.
  if ((EpochClient::g_enable_granola || EpochClient::g_enable_pwv) && client->callback.phase == EpochPhase::Execute) {
    g_finished.fetch_add(1);

    while (EpochClient::g_enable_granola && g_finished.load() != NodeConfiguration::g_nr_threads)
      _mm_pause();
  }

  if (client->callback.phase == EpochPhase::Execute) {
    VHandle::Quiescence();
    RowEntity::Quiescence();

    mem::GetDataRegion().Quiescence();
  } else if (client->callback.phase == EpochPhase::Initialize) {
  } else if (client->callback.phase == EpochPhase::Insert) {
    util::Instance<GC>().RunGC();
  }

  trace(TRACE_COMPLETION "complete issueing and flushing network {}", node_finished);

  client->completion.Complete();
  if (node_finished) {
    client->completion.Complete();
  }
}

void EpochClient::CallTxns(uint64_t epoch_nr, TxnMemberFunc func, const char *label)
{
  auto nr_threads = NodeConfiguration::g_nr_threads;
  conf.ResetBufferPlan();
  conf.SendStartPhase();
  callback.label = label;
  callback.perf.Clear();
  callback.perf.Start();

  if ((EpochClient::g_enable_granola || EpochClient::g_enable_pwv) && callback.phase == EpochPhase::Execute)
    CallTxnsWorker::g_finished = 0;

  // When another node sends its counter to us, it will not send pieces
  // first. This makes it hard to esitmate when we are about to finish all in a
  // phase. Therefore, we pretend all other nodes are going to send the maximum
  // pieces in this phase, and adjust this value when the counter arrives
  // eventually.
  completion.Increment((conf.nr_nodes() - 1) * kMaxPiecesPerPhase + 1 + nr_threads);

  // The order here is very very important. First, we reset all issue
  // workers. After this step, the scheduler's IsReady() would return false.
  for (auto t = 0; t < nr_threads; t++) {
    auto r = &workers[t]->call_worker;
    r->Reset();
    r->set_function(func);
  }

  // Second, we kick everyone out if they are inside the scheduler (Peek()
  // function), and reset the scheduler queue.
  util::Impl<PromiseRoutineDispatchService>().Reset();

  // Third, We can now absorb pieces from the network. Notice if any
  // ExecutionRoutine has just been kicked out, IsReady() would make sure it's
  // not going to exit.
  conf.ContinueInboundPhase();

  // Last, let's start issuing txns.
  for (auto t = 0; t < nr_threads; t++) {
    auto r = &workers[t]->call_worker;
    go::GetSchedulerFromPool(t + 1)->WakeUp(r);
  }
}

void EpochClient::InitializeEpoch()
{
  auto &mgr = util::Instance<EpochManager>();
  mgr.DoAdvance(this);
  auto epoch_nr = mgr.current_epoch_nr();

  util::Impl<PromiseAllocationService>().Reset();

  auto nr_threads = NodeConfiguration::g_nr_threads;

  cur_txns = &all_txns[epoch_nr - 1];
  total_nr_txn = NumberOfTxns();

  cont_lmgr.Reset();

  logger->info("Using EpochTxnSet {}", (void *) &all_txns[epoch_nr - 1]);

  util::Instance<GC>().PrepareGCForAllCores();

  commit_buffer->Reset();
  AllocStateTxnWorker::comp = nr_threads + 1;
  for (auto t = 0; t < nr_threads; t++) {
    auto r = &workers[t]->alloc_state_worker;
    r->Reset();
    r->set_urgent(true);
    go::GetSchedulerFromPool(t + 1)->WakeUp(r);
  }

  callback.phase = EpochPhase::Insert;
  CallTxns(epoch_nr, &BaseTxn::PrepareInsert0, "Insert");
}

void EpochClient::OnInsertComplete()
{
  // GC must have been completed
  auto &gc = util::Instance<GC>();
  gc.PrintStats();
  gc.ClearStats();
  probes::EndOfPhase{util::Instance<EpochManager>().current_epoch_nr(), 0}();

  stats.insert_time_ms += callback.perf.duration_ms();

  callback.phase = EpochPhase::Initialize;
  CallTxns(
      util::Instance<EpochManager>().current_epoch_nr(),
      &BaseTxn::Prepare0,
      "Initialization");
}

void EpochClient::OnInitializeComplete()
{
  stats.initialize_time_ms += callback.perf.duration_ms();
  probes::EndOfPhase{util::Instance<EpochManager>().current_epoch_nr(), 1}();

  callback.phase = EpochPhase::Execute;

  if (NodeConfiguration::g_data_migration && util::Instance<EpochManager>().current_epoch_nr() == 1) {
    logger->info("Starting data scanner thread");
    auto &peer = util::Instance<felis::NodeConfiguration>().config().row_shipper_peer;
    go::GetSchedulerFromPool(NodeConfiguration::g_nr_threads + 1)->WakeUp(
      new felis::RowScannerRoutine());
  }

  if (Options::kVHandleBatchAppend || Options::kOnDemandSplitting) {
    util::Instance<ContentionManager>().Reset();
  }

  util::Impl<VHandleSyncService>().ClearWaitCountStats();

  // exec_lmgr.PrintLoads();
  if (!Options::kBinpackSplitting) {
    cont_lmgr.Balance();
    // cont_lmgr.PrintLoads();
  }

  auto &mgr = util::Instance<EpochManager>();

  CallTxns(
      util::Instance<EpochManager>().current_epoch_nr(),
      &BaseTxn::Run0,
      "Execution");
}

void EpochClient::OnExecuteComplete()
{
  stats.execution_time_ms += callback.perf.duration_ms();
  fmt::memory_buffer buf;
  long ctt = 0;
  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto c = util::Impl<VHandleSyncService>().GetWaitCountStat(i);
    ctt += c / core_limit;
    fmt::format_to(buf, "{} ", c);
  }
  logger->info("Wait Counts {}", std::string_view(buf.begin(), buf.size()));
  if (Options::kCoreScaling && cur_epoch_nr > 1) {
    auto ctt_rate = ctt / callback.perf.duration_ms();

    logger->info("duration {} vs last_duration {}, ctt_rate {}",
                 callback.perf.duration_ms(), best_duration, ctt_rate);
    if (best_core == std::numeric_limits<int>::max() && ctt_rate < g_corescaling_threshold) {
      best_core = core_limit;
    }

    if (core_limit < best_core) {
      if (callback.perf.duration_ms() * 1.05 < best_duration) {
        best_duration = callback.perf.duration_ms();
        best_core = core_limit;
        sample_count = 1;
      }

      if (callback.perf.duration_ms() / 1.15 > best_duration) {
        sample_count = 1;
      }

      if (--sample_count == 0) {
        core_limit -= mem::kNrCorePerNode;
        sample_count = 3;
      }
      if (core_limit == 0)
        core_limit = best_core;
    }
    logger->info("Contention Control new core_limit {}", core_limit);
  }

  probes::EndOfPhase{cur_epoch_nr, 2}();

  if (Options::kAutoTuneThreshold) {
    g_splitting_threshold = g_threshold_autotune.GetNextThreshold(
        g_splitting_threshold,
        util::Instance<ContentionManager>().estimated_splits(),
        callback.perf.duration_ms());
    logger->info("Autotune threshold={}", g_splitting_threshold);
  }

  if (cur_epoch_nr + 1 < g_max_epoch) {
    InitializeEpoch();
  } else {
    // End of the experiment.
    perf.Show("All epochs done in");
    auto thr = NumberOfTxns() * 1000 * (g_max_epoch - 1) / perf.duration_ms();
    logger->info("Throughput {} txn/s", thr);
    logger->info("Insert / Initialize / Execute {} ms {} ms {} ms",
                 stats.insert_time_ms, stats.initialize_time_ms, stats.execution_time_ms);
    mem::PrintMemStats();
    mem::GetDataRegion().PrintUsageEachClass();

    if (Options::kOutputDir) {
      json11::Json::object result {
        {"cpu", static_cast<int>(NodeConfiguration::g_nr_threads)},
        {"duration", static_cast<int>(perf.duration_ms())},
        {"throughput", static_cast<int>(thr)},
        {"insert_time", stats.insert_time_ms},
        {"initialize_time", stats.initialize_time_ms},
        {"execution_time", stats.execution_time_ms},
      };
      auto node_name = util::Instance<NodeConfiguration>().config().name;
      time_t tm;
      char now[80];
      time(&tm);
      strftime(now, 80, "-%F-%X", localtime(&tm));
      std::ofstream result_output(
          Options::kOutputDir.Get() + "/" + node_name + now + ".json");
      result_output << json11::Json(result).dump() << std::endl;
    }
    conf.CloseAndShutdown();
    util::Instance<Console>().UpdateServerStatus(Console::ServerStatus::Exiting);
  }
}

static constexpr size_t kEpochPromiseAllocationWorkerLimit = 1024_M;
static constexpr size_t kEpochPromiseAllocationMainLimit = 64_M;
static constexpr size_t kEpochPromiseMiniBrkSize = 4 * CACHE_LINE_SIZE;

EpochPromiseAllocationService::EpochPromiseAllocationService()
{
  size_t acc = 0;
  for (size_t i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    auto s = kEpochPromiseAllocationWorkerLimit / NodeConfiguration::g_nr_threads;
    int numa_node = -1;
    if (i == 0) {
      s = kEpochPromiseAllocationMainLimit;
    } else {
      numa_node = (i - 1) / mem::kNrCorePerNode;
    }
    brks[i] = mem::Brk::New(mem::AllocMemory(mem::Promise, s, numa_node), s);
    acc += s;
    constexpr auto mini_brk_size = 4 * CACHE_LINE_SIZE;
    minibrks[i] = mem::Brk::New(
        brks[i]->Alloc(kEpochPromiseMiniBrkSize),
        kEpochPromiseMiniBrkSize);
  }
  // logger->info("Memory allocated: PromiseAllocator {}GB", acc >> 30);
}

void *EpochPromiseAllocationService::Alloc(size_t size)
{
  int thread_id = go::Scheduler::CurrentThreadPoolId();
  if (size < CACHE_LINE_SIZE) {
    auto b = minibrks[thread_id];
    if (!b->Check(size)) {
      b = mem::Brk::New(
          brks[thread_id]->Alloc(kEpochPromiseMiniBrkSize),
          kEpochPromiseMiniBrkSize);
    }
    return b->Alloc(size);
  } else {
    return brks[thread_id]->Alloc(util::Align(size, CACHE_LINE_SIZE));
  }
}

void EpochPromiseAllocationService::Reset()
{
  for (size_t i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    // logger->info("  PromiseAllocator {} used {}MB. Resetting now.", i,
    // brks[i].current_size() >> 20);
    brks[i]->Reset();
    minibrks[i] = mem::Brk::New(
        brks[i]->Alloc(kEpochPromiseMiniBrkSize),
        kEpochPromiseMiniBrkSize);
  }
}

static constexpr size_t kEpochMemoryLimitPerCore = 16_M;

EpochMemory::EpochMemory()
{
  logger->info("Allocating EpochMemory");
  auto &conf = util::Instance<NodeConfiguration>();
  for (int i = 0; i < conf.nr_nodes(); i++) {
    node_mem[i].mmap_buf =
        (uint8_t *) mem::AllocMemory(
            mem::Epoch, kEpochMemoryLimitPerCore * conf.g_nr_threads, -1, true);
    for (int t = 0; t < conf.g_nr_threads; t++) {
      auto p = node_mem[i].mmap_buf + t * kEpochMemoryLimitPerCore;
      auto numa_node = t / mem::kNrCorePerNode;
      util::OSMemory::BindMemory(p, kEpochMemoryLimitPerCore, numa_node);
    }
    util::OSMemory::LockMemory(node_mem[i].mmap_buf, kEpochMemoryLimitPerCore * conf.g_nr_threads);
  }
  Reset();
}

EpochMemory::~EpochMemory()
{
  logger->info("Freeing EpochMemory");
  auto &conf = util::Instance<NodeConfiguration>();
  for (int i = 0; i < conf.nr_nodes(); i++) {
    munmap(node_mem[i].mmap_buf, kEpochMemoryLimitPerCore * conf.g_nr_threads);
  }
}

void EpochMemory::Reset()
{
  auto &conf = util::Instance<NodeConfiguration>();
  // I only manage the current node.
  auto node_id = conf.node_id();
  auto &m = node_mem[node_id - 1];
  for (int t = 0; t < conf.g_nr_threads; t++) {
    auto p = m.mmap_buf + t * kEpochMemoryLimitPerCore;
    m.brks[t] = mem::Brk::New(p, kEpochMemoryLimitPerCore);
  }
}

Epoch *EpochManager::epoch(uint64_t epoch_nr) const
{
  abort_if(epoch_nr != cur_epoch_nr, "Confused by epoch_nr {} since current epoch is {}",
           epoch_nr, cur_epoch_nr);
  return cur_epoch;
}

uint8_t *EpochManager::ptr(uint64_t epoch_nr, int node_id, uint64_t offset) const
{
  abort_if(epoch_nr != cur_epoch_nr,
           "Confused by epoch_nr {} since current epoch is {}, node {}, offset "
           "{}, current core {}",
           epoch_nr, cur_epoch_nr, node_id, offset, go::Scheduler::CurrentThreadPoolId() - 1);
  return epoch(epoch_nr)->mem->node_mem[node_id - 1].mmap_buf + offset;
}

static Epoch *g_epoch; // We don't support concurrent epochs for now.

void EpochManager::DoAdvance(EpochClient *client)
{
  cur_epoch_nr.fetch_add(1);
  cur_epoch.load()->~Epoch();
  cur_epoch = new (cur_epoch) Epoch(cur_epoch_nr, client, mem);
  logger->info("We are going into epoch {}", cur_epoch_nr);
}

EpochManager::EpochManager(EpochMemory *mem, Epoch *epoch)
    : cur_epoch_nr(0), cur_epoch(epoch), mem(mem)
{
  cur_epoch.load()->mem = mem;
}

}

namespace util {

using namespace felis;

EpochManager *InstanceInit<EpochManager>::instance = nullptr;

InstanceInit<EpochManager>::InstanceInit()
{
  // We currently do not support concurrent epochs.
  static Epoch g_epoch;
  static EpochMemory mem;
  instance = new EpochManager(&mem, &g_epoch);
}

}
