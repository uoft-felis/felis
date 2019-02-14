#include <sys/mman.h>
#include <algorithm>

#include "epoch.h"
#include "txn.h"
#include "log.h"
#include "vhandle.h"

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
  go::GetSchedulerFromPool(0)->WakeUp(go::Make(continuation));
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
          for (ulong j = i * total_nr_txn / nr_threads;
               j < (i + 1) * total_nr_txn / nr_threads;
               j++) {
            txns[j]->root_promise()->Complete(VarStr());
            txns[j]->ResetRoot();
          }
          conf.DecrementUrgencyCount();
          logger->info("core {} finished issuing promises", i);
        });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }
}

void EpochClient::IssueTransactions(uint64_t epoch_nr, std::function<void (BaseTxn *)> func, bool sync)
{
  auto nr_threads = NodeConfiguration::g_nr_threads;
  conf.ResetBufferPlan();
  conf.FlushBufferPlanCompletion(epoch_nr);

  uint8_t buf[nr_threads];
  go::BufferChannel *comp = new go::BufferChannel(nr_threads);

  conf.IncrementUrgencyCount(nr_threads);
  for (ulong t = 0; t < nr_threads; t++) {
    auto r = go::Make(
        [func, t, comp, this, nr_threads]() {
          for (uint64_t i = t * total_nr_txn / nr_threads;
               i < (t + 1) * total_nr_txn / nr_threads; i++) {
            func(txns[i]);
          }
          logger->info("Issuer done on core {}", t);
          uint8_t done = 0;
          comp->Write(&done, 1);
        });
    r->set_urgent(true);
    go::GetSchedulerFromPool(t + 1)->WakeUp(r);
  }

  comp->Read(buf, nr_threads);
  for (uint64_t i = 0; i < total_nr_txn; i++) {
    conf.CollectBufferPlan(txns[i]->root_promise());
  }
  conf.FlushBufferPlan(sync);
}

void EpochClient::InitializeEpoch()
{
  // TODO: Add epoch management here? At least right now this is essential.
  util::Instance<EpochManager>().DoAdvance(this);

  util::Impl<PromiseAllocationService>().Reset();

  auto nr_threads = NodeConfiguration::g_nr_threads;

  printf("load percentage %d\n", LoadPercentage());
  total_nr_txn = kEpochBase * LoadPercentage();

  txns.reset(new BaseTxn*[total_nr_txn]);

  disable_load_balance = true;
  for (uint64_t i = 0; i < total_nr_txn; i++) {
    auto sequence = i + 1;
    txns[i] = RunCreateTxn(GenerateSerialId(i + 1));
  }

  IssueTransactions(1, std::mem_fn(&BaseTxn::PrepareInsert));

  RunTxnPromises(
      "Epoch Initialization (Insert)",
      [this] () {
        IssueTransactions(1, std::mem_fn(&BaseTxn::Prepare));
        RunTxnPromises(
            "Epoch Initialization (Lookup/RangeScan)",
            std::bind(&EpochClient::ExecuteEpoch, this));
      });
}

void EpochClient::ExecuteEpoch()
{
  IssueTransactions(1, std::mem_fn(&BaseTxn::RunAndAssignSchedulingKey));
  RunTxnPromises("Epoch Execution", []() {});
}

EpochExecutionDispatchService::EpochExecutionDispatchService()
{
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
    queue.pq.pool.move(mem::Pool(sizeof(QueueValue), kMaxItemPerCore));

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

static bool Greater(const EpochExecutionDispatchService::QueueItem &a,
                    const EpochExecutionDispatchService::QueueItem &b)
{
  return a.key > b.key;
}

void EpochExecutionDispatchService::Add(int core_id, PromiseRoutineWithInput *routines,
                                        size_t nr_routines)
{
  bool locked = false;
  bool should_preempt = false;
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
      e.value = (QueueValue *) q.pool.Alloc();
      e.value->promise_routine = r;
      e.value->state = nullptr;
      auto old_key = q.q[0].key;
      std::push_heap(q.q, q.q + q.len, Greater);
      if (q.q[0].key < old_key)
        should_preempt = true;
    }
    abort_if(zq.end == kMaxItemPerCore || q.len == kMaxItemPerCore,
             "Preallocation of DispatchService is too small");
  }
  zq.end.fetch_add(zdelta, std::memory_order_release);
  lock.store(false);
  if (should_preempt)
    util::Impl<VHandleSyncService>().Notify(1 << core_id);
}

bool
EpochExecutionDispatchService::Peek(int core_id, DispatchPeekListener &should_pop)
{
  auto &zq = queues[core_id].zq;
  auto &q = queues[core_id].pq;
  auto &lock = queues[core_id].lock;

  // This is the fast path because we do not need to grab the lock. However,
  // when Add() is called concurrently, we might end up ignoring some of the
  // latest items they put into the queue. In that case, we grab the lock and
  // fallback to the slow path. The slow path might release the lock and retry
  // the fast path.
retry:
  if (zq.start < zq.end.load(std::memory_order_acquire)) {
    states[core_id]->running.store(true, std::memory_order_release);
    auto r = zq.q[zq.start];
    if (should_pop(r, nullptr)) {
      zq.start++;
      states[core_id]->current = r;
      return true;
    }
    return false;
  }

  bool locked = false;
  while (!lock.compare_exchange_strong(locked, true)) {
    locked = false;
    __builtin_ia32_pause();
  }

  if (zq.start < zq.end.load(std::memory_order_acquire)) {
    lock.store(false);
    goto retry;
  }

  if (q.len > 0) {
    auto value = q.q[0].value;
    bool pop = false;

    states[core_id]->running.store(true, std::memory_order_relaxed);
    if (should_pop(value->promise_routine, value->state)) {
      std::pop_heap(q.q, q.q + q.len, Greater);

      states[core_id]->current = value->promise_routine;
      q.pool.Free(value);
      q.len--;
      pop = true;
    }

    lock.store(false);
    return pop;
  }

  states[core_id]->running.store(false, std::memory_order_relaxed);
  lock.store(false);

  // We do not need locks to protect completion counters. There can only be MT
  // access on Pop() and Add(), the counters are per-core anyway.
  auto &c = states[core_id]->complete_counter;
  auto n = c.completed;
  auto comp = EpochClient::g_workload_client->completion_object();
  c.completed = 0;

  unsigned long nr_bubbles = tot_bubbles.load();
  while (!tot_bubbles.compare_exchange_strong(nr_bubbles, 0));

  if (n + nr_bubbles > 0) {
    logger->info("DispatchService on core {} notifies {} completions",
                 core_id, n + nr_bubbles);
    comp->Complete(n + nr_bubbles);
  }
  return false;
}

void EpochExecutionDispatchService::AddBubble()
{
  tot_bubbles.fetch_add(1);
}

bool EpochExecutionDispatchService::Preempt(int core_id, bool force)
{
  auto &lock = queues[core_id].lock;
  bool new_routine = true;
  bool locked = false;
  while (!lock.compare_exchange_strong(locked, true)) {
    locked = false;
    __builtin_ia32_pause();
  }

  auto &r = states[core_id]->current;
  auto &zq = queues[core_id].zq;
  auto &q = queues[core_id].pq;
  auto key = std::get<0>(r)->sched_key;

  if (!force && zq.end.load(std::memory_order_relaxed) == zq.start) {
    if (q.len == 0 || key < q.q[0].key) {
      new_routine = false;
      goto done;
    }
  }

  if (key == 0) {
    zq.q[zq.end.load(std::memory_order_relaxed)] = r;
    zq.end.fetch_add(1, std::memory_order_release);
  } else  {
    auto value = (QueueValue *) q.pool.Alloc();
    value->promise_routine = r;
    value->state = (BasePromise::ExecutionRoutine *) go::Scheduler::Current()->current_routine();
    q.q[q.len++] = QueueItem{std::get<0>(r)->sched_key, value};
    std::push_heap(q.q, q.q + q.len, Greater);
  }
done:
  lock.store(false);
  return new_routine;
}

void EpochExecutionDispatchService::Complete(int core_id)
{
  auto &c = states[core_id]->complete_counter;
  c.completed++;
}

void EpochExecutionDispatchService::PrintInfo()
{
  puts("===================================");
  for (int core_id = 0; core_id < NodeConfiguration::g_nr_threads; core_id++) {
    auto &q = queues[core_id].pq.q;
    printf("DEBUG: %lu and %lu,%lu on core %d\n",
           q[0].key, q[1].key, q[2].key, core_id);
  }
  puts("===================================");
}

static constexpr size_t kEpochPromiseAllocationPerThreadLimit = 2ULL << 30;
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
