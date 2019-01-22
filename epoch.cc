#include <sys/mman.h>
#include <algorithm>

#include "epoch.h"
#include "txn.h"
#include "log.h"
#include "vhandle.h"
#include "iface.h"
namespace felis {

EpochClient *EpochClient::g_workload_client = nullptr;

void EpochCallback::operator()()
{
  perf.End();
  perf.Show(label + std::string(" finishes"));

  // Don't call the continuation directly.
  //
  // This is because we might Reset() the PromiseAllocationService, which would free
  // the current go::Routine.
  go::Scheduler::Current()->WakeUp(go::Make(continuation));
}

EpochClient::EpochClient() noexcept
    : callback(EpochCallback(this)),
      completion(0, callback),
      disable_load_balance(false),
      conf(util::Instance<NodeConfiguration>())
{
  callback.perf.End();
}

void EpochClient::Start()
{
  auto worker = go::Make(std::bind(&EpochClient::InitializeEpoch, this));
  go::GetSchedulerFromPool(0)->WakeUp(worker);
}

uint64_t EpochClient::GenerateSerialId(uint64_t sequence)
{
  return (sequence << 8) | (conf.node_id() & 0x00FF);
}

void EpochClient::RunTxnPromises(std::string label, std::function<void ()> continuation)
{
  callback.label = label;
  callback.continuation = continuation;
  callback.perf.Clear();
  callback.perf.Start();
  auto nr_threads = NodeConfiguration::g_nr_threads;
  for (ulong i = 0; i < nr_threads; i++) {
    auto r = go::Make(
        [i, nr_threads, this] {
          conf.IncrementExtraIOPending(i);
          for (ulong j = i * total_nr_txn / nr_threads;
               j < (i + 1) * total_nr_txn / nr_threads;
               j++) {
            txns[j]->root_promise()->Complete(VarStr());
            txns[j]->ResetRoot();
          }
          conf.DecrementExtraIOPending(i);
          logger->info("core {} finished issuing promises", i);
        });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }
}

void EpochClient::InitializeEpoch()
{
  // TODO: Add epoch management here? At least right now this is essential.
  util::Instance<EpochManager>().DoAdvance(this);

  util::Impl<PromiseAllocationService>().Reset();

  conf.ResetBufferPlan();
  conf.FlushBufferPlanCompletion(0);

  printf("load percentage %d\n", LoadPercentage());
  total_nr_txn = kEpochBase * LoadPercentage();

  txns.reset(new BaseTxn*[total_nr_txn]);

  disable_load_balance = true;
  for (uint64_t i = 0; i < total_nr_txn; i++) {
    auto sequence = i + 1;
    auto *txn = RunCreateTxn(GenerateSerialId(i + 1));
    conf.CollectBufferPlan(txn->root_promise());
    txns[i] = txn;
  }
  conf.FlushBufferPlan(false);

  // RunTxnPromises("Epoch Initialization",
  // std::bind(&EpochClient::ExecuteEpoch, this));
  RunTxnPromises("Epoch Initialization", []() {});
}

void EpochClient::ExecuteEpoch()
{
  // util::Impl<PromiseAllocationService>().Reset();

  conf.ResetBufferPlan();
  conf.FlushBufferPlanCompletion(1);

  for (ulong i = 0; i < total_nr_txn; i++) {
    txns[i]->Run();
    txns[i]->root_promise()->AssignSchedulingKey(txns[i]->serial_id());
    conf.CollectBufferPlan(txns[i]->root_promise());
  }
  conf.FlushBufferPlan(true);
  RunTxnPromises("Epoch Execution", []() {});
}

EpochExecutionDispatchService::EpochExecutionDispatchService()
{
  currents.fill(std::make_tuple(nullptr, VarStr()));
  for (auto &queue: queues) {
    queue.zq.end = queue.zq.start = 0;
    queue.zq.q =
        (PromiseRoutineWithInput *)
        mmap(nullptr, kMaxItemPerCore * sizeof(PromiseRoutineWithInput),
             PROT_READ | PROT_WRITE,
             MAP_HUGETLB | MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE,
             -1, 0);
    queue.pq.len = 0;
    queue.pq.q =
        (QueueItem *)
        mmap(nullptr, kMaxItemPerCore * sizeof(QueueItem),
             PROT_READ | PROT_WRITE,
             MAP_HUGETLB | MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE,
             -1, 0);
    queue.lock = false;
  }
  tot_bubbles = 0;
}

void EpochExecutionDispatchService::Reset()
{
  for (auto &q: queues) {
    q.zq.end = q.zq.start = 0;
    q.pq.len = 0;
  }
  tot_bubbles = 0;
}

static bool GreaterOrEqual(const EpochExecutionDispatchService::QueueItem &a,
                           const EpochExecutionDispatchService::QueueItem &b)
{
  return a.key >= b.key;
}

void EpochExecutionDispatchService::Add(int core_id, PromiseRoutineWithInput *routines,
                                        size_t nr_routines)
{
  bool locked = false;
  auto &lock = queues[core_id].lock;
  while (!lock.compare_exchange_strong(locked, true)) {
    locked = false;
    __builtin_ia32_pause();
  }

  auto &zq = queues[core_id].zq;
  auto &q = queues[core_id].pq;
  size_t zdelta = 0;
  for (size_t i = 0; i < nr_routines; i++) {
    auto r = routines[i];
    auto key = std::get<0>(r)->sched_key;

    if (key == 0) {
      zq.q[zq.end + zdelta++] = r;
    } else {
      auto &e = q.q[q.len++];
      auto [rt, in] = r;
      e.key = key;
      e.state = State::NormalState;
      e.input_len = in.len;
      e.input_data = in.data;
      e.routine = rt;
      std::push_heap(q.q, q.q + q.len, GreaterOrEqual);
    }
    abort_if(zq.end == kMaxItemPerCore || q.len == kMaxItemPerCore,
             "Preallocation of DispatchService is too small");
  }
  zq.end.fetch_add(zdelta, std::memory_order_release);
  lock.store(false);
}

std::tuple<PromiseRoutineWithInput, PromiseRoutineDispatchService::State>
EpochExecutionDispatchService::Pop(int core_id)
{
  auto &zq = queues[core_id].zq;
  auto &q = queues[core_id].pq;
  if (zq.start < zq.end.load(std::memory_order_acquire)) {
    auto r = zq.q[zq.start++];
    currents[core_id] = r;
    return std::make_tuple(r, State::NormalState);
  }
  if (q.len > 0) {
    std::pop_heap(q.q, q.q + q.len, GreaterOrEqual);
    auto &item = q.q[--q.len];
    currents[core_id] = {
      item.routine, VarStr(item.input_len, 0, item.input_data)
    };
    return std::make_tuple(currents[core_id], item.state);
  }

  // We do not need locks to protect completion counters. There can only be MT
  // access on Pop() and Add(), the counters are per-core anyway.
  auto &c = completed_counters[core_id];
  auto n = c.completed;
  auto comp = EpochClient::g_workload_client->completion_object();
  c.completed = 0;

  unsigned long nr_bubbles = tot_bubbles.load();
  while (!tot_bubbles.compare_exchange_strong(nr_bubbles, 0));

  if (n + nr_bubbles > 0) {
    comp->Complete(n + nr_bubbles);
  }
  return std::make_tuple(
      std::make_tuple(nullptr, VarStr()),
      State::NormalState);
}

void EpochExecutionDispatchService::AddBubble()
{
  tot_bubbles.fetch_add(1);
}

void EpochExecutionDispatchService::Preempt(int core_id)
{
  auto &lock = queues[core_id].lock;
  bool locked = false;
  while (!lock.compare_exchange_strong(locked, true)) {
    locked = false;
    __builtin_ia32_pause();
  }

  auto r = currents[core_id];
  currents[core_id] = {nullptr, VarStr()};
  auto &zq = queues[core_id].zq;
  auto &q = queues[core_id].pq;
  if (std::get<0>(r)->sched_key == 0) {
    zq.q[zq.end] = r;
    zq.end.fetch_add(1, std::memory_order_release);
  } else {
    q.q[q.len] = QueueItem(r, State::PreemptState);
    std::push_heap(q.q, q.q + q.len, GreaterOrEqual);
  }
  lock.store(false);
}

void EpochExecutionDispatchService::Complete(int core_id)
{
  auto &c = completed_counters[core_id];
  c.completed++;
}

void EpochExecutionDispatchService::PrintInfo()
{
  puts("===================================");
  for (int core_id = 0; core_id < NodeConfiguration::g_nr_threads; core_id++) {
    printf("DEBUG: %lu on this core (core %d)\n",
           queues[core_id].pq.q[0].key, core_id);
  }
  puts("===================================");
}

static constexpr size_t kEpochPromiseAllocationPerThreadLimit = 512ULL << 20;
static constexpr size_t kEpochPromiseAllocationMainLimit = 1ULL << 30;

EpochPromiseAllocationService::EpochPromiseAllocationService()
{
  brks = new mem::Brk[NodeConfiguration::g_nr_threads + 1];
  minibrks = new mem::Brk[NodeConfiguration::g_nr_threads + 1];

  size_t acc = 0;
  for (size_t i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    auto s = kEpochPromiseAllocationPerThreadLimit;
    if (i == 0) s = kEpochPromiseAllocationMainLimit;
    brks[i].move(mem::Brk(
        mmap(NULL, s,
             PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE,
             -1, 0),
        s));
    acc += s;
    minibrks[i].move(mem::Brk(
        brks[i].Alloc(CACHE_LINE_SIZE), CACHE_LINE_SIZE));
  }
  logger->info("Memory used: PromiseAllocator {}GB", acc >> 30);
}

void *EpochPromiseAllocationService::Alloc(size_t size)
{
  int thread_id = go::Scheduler::CurrentThreadPoolId();
  if (size < CACHE_LINE_SIZE) {
    auto &b = minibrks[thread_id];
    if (!b.Check(size)) {
      b.move(mem::Brk(
          brks[thread_id].Alloc(CACHE_LINE_SIZE), CACHE_LINE_SIZE));
    }
    return b.Alloc(size);
  } else {
    return brks[thread_id].Alloc(util::Align(size, CACHE_LINE_SIZE));
  }
}

void EpochPromiseAllocationService::Reset()
{
  for (size_t i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    brks[i].Reset();
    minibrks[i].move(
        mem::Brk(brks[i].Alloc(CACHE_LINE_SIZE), CACHE_LINE_SIZE));
  }
}

static constexpr size_t kEpochMemoryLimit = 256 << 20;

EpochMemory::EpochMemory(mem::Pool *pool)
    : pool(pool)
{
  logger->info("Setting up epoch memory pool and brks");
  auto &conf = util::Instance<NodeConfiguration>();
  for (int i = 0; i < conf.nr_nodes(); i++) {
    brks[i].mem = (uint8_t *) pool->Alloc();
    brks[i].off = 0;
  }
}

EpochMemory::~EpochMemory()
{
  auto &conf = util::Instance<NodeConfiguration>();
  for (int i = 0; i < conf.nr_nodes(); i++) {
    pool->Free(brks[i].mem);
  }
}

EpochManager *EpochManager::instance = nullptr;

Epoch *EpochManager::epoch(uint64_t epoch_nr) const
{
  Epoch *e = concurrent_epochs[epoch_nr % kMaxConcurrentEpochs];
  abort_if(e == nullptr, "current epoch is null");
  abort_if(e->epoch_nr != epoch_nr, "epoch number mismatch {} != {}",
           e->epoch_nr, epoch_nr);
  return e;
}

uint8_t *EpochManager::ptr(uint64_t epoch_nr, int node_id, uint64_t offset) const
{
  return epoch(epoch_nr)->brks[node_id - 1].mem + offset;
}

void EpochManager::DoAdvance(EpochClient *client)
{
  cur_epoch_nr++;
  delete concurrent_epochs[cur_epoch_nr % kMaxConcurrentEpochs];
  concurrent_epochs[cur_epoch_nr % kMaxConcurrentEpochs] = new Epoch(cur_epoch_nr, client,pool);
}

EpochManager::EpochManager()
    : pool(new mem::Pool(kEpochMemoryLimit,
                         util::Instance<NodeConfiguration>().nr_nodes() * kMaxConcurrentEpochs)),
      cur_epoch_nr(0)
{
}

}
