#include <sys/mman.h>
#include <algorithm>

#include "epoch.h"
#include "txn.h"
#include "log.h"
#include "vhandle.h"
#include "mem.h"

namespace felis {

EpochClient *EpochClient::g_workload_client = nullptr;

void EpochCallback::operator()()
{
  perf.End();
  perf.Show(label + std::string(" finishes"));
  mem::PrintMemStats();

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
  return (util::Instance<EpochManager>().current_epoch_nr() << 32)
      | (sequence << 8)
      | (conf.node_id() & 0x00FF);
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
          conf.DecrementUrgencyCount(i);
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

  for (ulong t = 0; t < nr_threads; t++) {
    auto r = go::Make(
        [func, t, comp, this, nr_threads]() {
          conf.IncrementUrgencyCount(t);
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
  util::Instance<EpochManager>().DoAdvance(this);

  util::Impl<PromiseAllocationService>().Reset();

  auto nr_threads = NodeConfiguration::g_nr_threads;

  printf("load percentage %d\n", LoadPercentage());
  total_nr_txn = kEpochBase * LoadPercentage();

  txns.reset(new BaseTxn*[total_nr_txn]);

  //if (NodeConfiguration::g_data_migration)
  disable_load_balance = true;
  for (uint64_t i = 0; i < total_nr_txn; i++) {
    auto sequence = i + 1;
    txns[i] = CreateTxn(GenerateSerialId(i + 1));
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
  if (NodeConfiguration::g_data_migration) {
    logger->info("Starting data scanner thread");
    auto &peer = util::Instance<felis::NodeConfiguration>().config().row_shipper_peer;
    go::GetSchedulerFromPool(NodeConfiguration::g_nr_threads + 1)->WakeUp(
      new felis::RowScannerRoutine());
  }
  IssueTransactions(1, std::mem_fn(&BaseTxn::RunAndAssignSchedulingKey));
  RunTxnPromises(
      "Epoch Execution",
      [this]() {
        if (util::Instance<EpochManager>().current_epoch_nr() + 1 < kMaxEpoch) {
          InitializeEpoch();
        }
      });
}

const size_t EpochExecutionDispatchService::kMaxItemPerCore = 4 << 20;
const size_t EpochExecutionDispatchService::kHashTableSize = 100001;

EpochExecutionDispatchService::EpochExecutionDispatchService()
{
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto &queue = queues[i];
    queue.zq.end = queue.zq.start = 0;
    queue.zq.q = (PromiseRoutineWithInput *)
                 mem::MemMapAlloc(mem::EpochQueuePromise,
                                  kMaxItemPerCore * sizeof(PromiseRoutineWithInput));
    queue.pq.len = 0;
    queue.pq.q = (PriorityQueueHeapEntry *)
                 mem::MemMapAlloc(mem::EpochQueueItem,
                                  kMaxItemPerCore * sizeof(PriorityQueueHeapEntry));
    queue.pq.ht = (PriorityQueueHashHeader *)
                  mem::MemMapAlloc(mem::EpochQueueItem,
                                   kHashTableSize * sizeof(PriorityQueueHashHeader));
    queue.pq.pending.q = (PromiseRoutineWithInput *)
                         mem::MemMapAlloc(mem::EpochQueuePromise,
                                          kMaxItemPerCore * sizeof(PromiseRoutineWithInput));
    queue.pq.pending.start = 0;
    queue.pq.pending.end = 0;

    for (size_t t = 0; t < kHashTableSize; t++) {
      queue.pq.ht[t].Initialize();
    }

    queue.pq.pool.move(mem::BasicPool(mem::EpochQueuePool, kPriorityQueuePoolElementSize, kMaxItemPerCore));

    new (&queue.lock) util::SpinLock();
  }
  tot_bubbles = 0;
}

void EpochExecutionDispatchService::Reset()
{
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto &q = queues[i];
    q.zq.end = q.zq.start = 0;
    q.pq.len = 0;
  }
  tot_bubbles = 0;
}

static bool Greater(const EpochExecutionDispatchService::PriorityQueueHeapEntry &a,
                    const EpochExecutionDispatchService::PriorityQueueHeapEntry &b)
{
  return a.key > b.key;
}

void EpochExecutionDispatchService::Add(int core_id, PromiseRoutineWithInput *routines,
                                        size_t nr_routines)
{
  bool locked = false;
  bool should_preempt = false;
  auto &lock = queues[core_id].lock;
  lock.Lock();

  auto &zq = queues[core_id].zq;
  auto &pq = queues[core_id].pq.pending;
  size_t i = 0;

again:
  size_t zdelta = 0,
           zend = zq.end.load(std::memory_order_acquire),
         zlimit = kMaxItemPerCore;

  size_t pdelta = 0,
           pend = pq.end.load(std::memory_order_acquire),
         plimit = kMaxItemPerCore
                  - (pend - pq.start.load(std::memory_order_acquire));

  for (; i < nr_routines; i++) {
    auto r = routines[i];
    auto key = std::get<0>(r)->sched_key;

    if (key == 0) {
      auto pos = zend + zdelta++;
      abort_if(pos >= zlimit,
               "Preallocation of DispatchService is too small. {} < {}", pos, zlimit);
      zq.q[pos] = r;
    } else {
      auto pos = pend + pdelta++;
      if (pdelta >= plimit) goto again;
      pq.q[pos % kMaxItemPerCore] = r;
    }
  }
  if (zdelta)
    zq.end.fetch_add(zdelta, std::memory_order_release);
  if (pdelta)
    pq.end.fetch_add(pdelta, std::memory_order_release);
  lock.Unlock();
  util::Impl<VHandleSyncService>().Notify(1 << core_id);
}

bool
EpochExecutionDispatchService::AddToPriorityQueue(PriorityQueue &q, PromiseRoutineWithInput &r)
{
  bool smaller = false;
  auto [rt, in] = r;
  auto node = (PriorityQueueValue *) q.pool.Alloc();
  node->promise_routine = r;
  node->state = nullptr;
  auto key = rt->sched_key;

  auto &hl = q.ht[Hash(key) % kHashTableSize];
  auto *ent = hl.next;
  while (ent != &hl) {
    if (ent->object()->key == key)
      goto found;
    ent = ent->next;
  }
  ent = (PriorityQueueHashEntry *) q.pool.Alloc();
  ent->object()->key = key;
  ent->object()->values.Initialize();
  ent->InsertAfter(hl.prev);

  if (q.len > 0 && q.q[0].key > key) {
    smaller = true;
  }
  q.q[q.len++] = {key, ent->object()};
  std::push_heap(q.q, q.q + q.len, Greater);

found:
  node->InsertAfter(ent->object()->values.prev);
  return smaller;
}

void
EpochExecutionDispatchService::ProcessPending(PriorityQueue &q)
{
  size_t pstart = q.pending.start.load(std::memory_order_acquire),
           plen = q.pending.end.load(std::memory_order_acquire) - pstart;

  for (size_t i = 0; i < plen; i++) {
    auto pos = pstart + i;
    AddToPriorityQueue(q, q.pending.q[pos % kMaxItemPerCore]);
  }
  if (plen)
    q.pending.start.fetch_add(plen);
}

bool
EpochExecutionDispatchService::Peek(int core_id, DispatchPeekListener &should_pop)
{
  auto &zq = queues[core_id].zq;
  auto &q = queues[core_id].pq;
  auto &lock = queues[core_id].lock;

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

  ProcessPending(q);

  if (q.len > 0) {
    auto node = q.q[0].ent->values.next;

    auto promise_routine = node->object()->promise_routine;

    states[core_id]->running.store(true, std::memory_order_relaxed);
    if (should_pop(promise_routine, node->object()->state)) {
      node->Remove();
      q.pool.Free(node);

      auto top = q.q[0];
      if (top.ent->values.empty()) {
        std::pop_heap(q.q, q.q + q.len, Greater);
        q.q[q.len - 1].ent = nullptr;
        q.len--;

        top.ent->Remove();
        q.pool.Free(top.ent);
      }

      states[core_id]->current = promise_routine;
      return true;
    }
    return false;
  }

  states[core_id]->running.store(false, std::memory_order_relaxed);

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
  auto &zq = queues[core_id].zq;
  auto &q = queues[core_id].pq;

  ProcessPending(q);

  lock.Lock();

  auto &r = states[core_id]->current;
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
    AddToPriorityQueue(q, r);
  }
  states[core_id]->running.store(false, std::memory_order_relaxed);

done:
  lock.Unlock();
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

static constexpr size_t kEpochPromiseAllocationWorkerLimit = 1ULL << 30;
static constexpr size_t kEpochPromiseAllocationMainLimit = 128ULL << 20;

EpochPromiseAllocationService::EpochPromiseAllocationService()
{
  brks = new mem::Brk[NodeConfiguration::g_nr_threads + 1];
  minibrks = new mem::Brk[NodeConfiguration::g_nr_threads + 1];

  size_t acc = 0;
  for (size_t i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    auto s = kEpochPromiseAllocationWorkerLimit / NodeConfiguration::g_nr_threads;
    if (i == 0) s = kEpochPromiseAllocationMainLimit;
    brks[i].move(mem::Brk(mem::MemMapAlloc(mem::Promise, s), s));
    acc += s;
    minibrks[i].move(mem::Brk(
        brks[i].Alloc(CACHE_LINE_SIZE), CACHE_LINE_SIZE));
  }
  logger->info("Memory allocated: PromiseAllocator {}GB", acc >> 30);
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
    logger->info("  PromiseAllocator {} used {}MB. Resetting now.", i, brks[i].current_size() >> 20);
    brks[i].Reset();
    minibrks[i].move(
        mem::Brk(brks[i].Alloc(CACHE_LINE_SIZE), CACHE_LINE_SIZE));
  }
}

static constexpr size_t kEpochMemoryLimit = 256 << 20;

EpochMemory::EpochMemory(mem::Pool *pool)
    : pool(pool)
{
  logger->info("Allocating EpochMemory");
  auto &conf = util::Instance<NodeConfiguration>();
  for (int i = 0; i < conf.nr_nodes(); i++) {
    brks[i].move(mem::Brk((uint8_t *) pool->Alloc(), kEpochMemoryLimit));
    brks[i].set_thread_safe(false);
  }
}

EpochMemory::~EpochMemory()
{
  logger->info("Freeing EpochMemory");
  auto &conf = util::Instance<NodeConfiguration>();
  for (int i = 0; i < conf.nr_nodes(); i++) {
    auto ptr = brks[i].ptr();
    pool->Free(ptr);
    brks[i].Reset();
  }
}

Epoch *EpochManager::epoch(uint64_t epoch_nr) const
{
  abort_if(epoch_nr != cur_epoch_nr, "Confused by epoch_nr {} since current epoch is {}",
           epoch_nr, cur_epoch_nr)
  return cur_epoch.get();
}

uint8_t *EpochManager::ptr(uint64_t epoch_nr, int node_id, uint64_t offset) const
{
  return epoch(epoch_nr)->brks[node_id - 1].ptr() + offset;
}

void EpochManager::DoAdvance(EpochClient *client)
{
  cur_epoch.reset();
  cur_epoch_nr++;
  cur_epoch.reset(new Epoch(cur_epoch_nr, client, pool));
  logger->info("We are going into epoch {}", cur_epoch_nr);
}

EpochManager::EpochManager(mem::Pool *pool)
    : pool(pool),
      cur_epoch_nr(0)
{
}

}

namespace util {

using namespace felis;

EpochManager *InstanceInit<EpochManager>::instance = nullptr;

InstanceInit<EpochManager>::InstanceInit()
{
  instance = new EpochManager(new mem::Pool(mem::Epoch, kEpochMemoryLimit,
                                            util::Instance<NodeConfiguration>().nr_nodes()));
}

}
