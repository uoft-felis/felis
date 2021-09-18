#include <sys/time.h>

#include "epoch.h"
#include "routine_sched.h"
#include "pwv_graph.h"
#include "priority.h"

namespace felis {

class ConservativePriorityScheduler final : public PrioritySchedulingPolicy {
  ~ConservativePriorityScheduler() {}
  struct PriorityQueueHeapEntry {
    uint64_t key;
    PriorityQueueHashEntry *ent;
  };

  static bool Greater(const PriorityQueueHeapEntry &a, const PriorityQueueHeapEntry &b) {
    return a.key > b.key;
  }

  bool ShouldPickWaiting(const WaitState &ws) override;
  PriorityQueueValue *Pick() override;
  void Consume(PriorityQueueValue *value) override;
  void IngestPending(PriorityQueueHashEntry *hent, PriorityQueueValue *value) override;
  void Reset() override {
    abort_if(len > 0, "Reset() called, but len {} > 0", len);
  }

 public:
  static ConservativePriorityScheduler *New(size_t maxlen, int numa_node);
 private:
  PriorityQueueHeapEntry q[];
};

ConservativePriorityScheduler *ConservativePriorityScheduler::New(size_t maxlen, int numa_node)
{
  auto p = mem::AllocMemory(mem::EpochQueueItem, maxlen * sizeof(PriorityQueueHeapEntry), numa_node);
  return new (p) ConservativePriorityScheduler();
}

void ConservativePriorityScheduler::IngestPending(PriorityQueueHashEntry *hent, PriorityQueueValue *value)
{
  if (hent->values.empty()) {
    q[len++] = {hent->key, hent};
    // HeapEntry contains HashEntry
    std::push_heap(q, q + len, Greater);
  }
  value->InsertAfter(hent->values.prev);
}

bool ConservativePriorityScheduler::ShouldPickWaiting(const WaitState &ws)
{
  if (len == 0)
    // if nothing in PQ, run the waitstate coroutine
    return true;
  if (q[0].key > ws.sched_key)
    return true;
    // if waitstate coroutine has higher priority, run coroutine
  return false;
}

PriorityQueueValue *ConservativePriorityScheduler::Pick()
{
  return q[0].ent->values.next->object();
}

void ConservativePriorityScheduler::Consume(PriorityQueueValue *node)
{
  node->Remove();
  // remove the piece from HashEntry
  auto top = q[0];
  if (top.ent->values.empty()) {
    // if the top HashEntry is empty
    std::pop_heap(q, q + len, Greater);
    // pop the top HashEntry from the heap
    q[len - 1].ent = nullptr;
    len--;
    top.ent->Remove(); // from the hashtable
  }
}

class PWVScheduler final : public PrioritySchedulingPolicy {
  PWVScheduler(void *p, size_t lmt)
      : brk(p, lmt) {
    inactive.Initialize();
    free.Initialize();
    rvp.Initialize();
    nr_free = 0;
    is_graph_built = false;
  }
  ~PWVScheduler() {}

  struct FreeNodeEntry : public util::GenericListNode<FreeNodeEntry> {
    PWVScheduler *sched;
    PriorityQueueHashEntry *ent;
    bool in_rvp_queue;
  };

  bool ShouldRetryBeforePick(std::atomic_ulong *zq_start, std::atomic_ulong *zq_end,
                             std::atomic_uint *pq_start, std::atomic_uint *pq_end) override;
  bool ShouldPickWaiting(const WaitState &ws) override;
  PriorityQueueValue *Pick() override;
  void Consume(PriorityQueueValue *value) override;
  void IngestPending(PriorityQueueHashEntry *hent, PriorityQueueValue *value) override;
  void Reset() override;

  static void OnNodeFree(void *p) {
    auto node_ent = (FreeNodeEntry *) p;
    node_ent->sched->OnNodeFreeImpl(node_ent);
  }
  static void OnNodeRVPChange(void *p) {
    auto node_ent = (FreeNodeEntry *) p;
    node_ent->sched->OnNodeRVPChangeImpl(node_ent);
  }

  void OnNodeFreeImpl(FreeNodeEntry *p);
  void OnNodeRVPChangeImpl(FreeNodeEntry *p);

  static RVPInfo *GetRVPInfo(PriorityQueueValue *value);
 public:
  static PWVScheduler *New(size_t maxlen, int numa_node);
 private:
  util::GenericListNode<FreeNodeEntry> free, inactive, rvp;
  util::MCSSpinLock qlock;
  std::atomic_ulong nr_free;
  std::atomic_bool is_graph_built;

  mem::Brk brk;
};

PWVScheduler *PWVScheduler::New(size_t maxlen, int numa_node)
{
  size_t sz = EpochClient::g_txn_per_epoch * util::Align(sizeof(FreeNodeEntry), 16);
  auto p = (uint8_t *) mem::AllocMemory(
      mem::EpochQueueItem,
      sizeof(PWVScheduler) + sz,
      numa_node);
  return new (p) PWVScheduler(p + sizeof(PWVScheduler), sz);
}

void PWVScheduler::OnNodeFreeImpl(FreeNodeEntry *node_ent)
{
  util::MCSSpinLock::QNode qnode;
  qlock.Acquire(&qnode);
  node_ent->Remove();
  node_ent->InsertAfter(free.prev);
  nr_free.fetch_add(1);
  qlock.Release(&qnode);
}

void PWVScheduler::OnNodeRVPChangeImpl(FreeNodeEntry *node_ent)
{
  if (!is_graph_built.load(std::memory_order_acquire)) {
    return; // Not ready
  }
  util::MCSSpinLock::QNode qnode;
  qlock.Acquire(&qnode);
  if (node_ent->in_rvp_queue) {
    // logger->info("notify rvp change for {} !", node_ent->ent->key);
    node_ent->Remove();
    node_ent->InsertAfter(free.prev);
    node_ent->in_rvp_queue = false;

    // put the non-RVP piece at the head.
    auto values = &node_ent->ent->values;
    for (auto it = values->next; it != values; it = it->next) {
      auto pv = it->object();
      auto info = GetRVPInfo(pv);
      if (!info->is_rvp || info->indegree == 0) {
        it->Remove();
        it->InsertAfter(values);
        // logger->info("RVP Change sid {}", node_ent->ent->key);
        nr_free.fetch_add(1);
        break;
      }
    }
  }
  qlock.Release(&qnode);
}

RVPInfo *PWVScheduler::GetRVPInfo(PriorityQueueValue *value)
{
  return RVPInfo::FromRoutine(value->routine);
}

void PWVScheduler::IngestPending(PriorityQueueHashEntry *hent, PriorityQueueValue *value)
{
  abort_if(is_graph_built, "graph is already built! core {}", go::Scheduler::CurrentThreadPoolId() - 1);
  auto g = util::Instance<PWVGraphManager>().local_graph();
  util::MCSSpinLock::QNode qnode;
  qlock.Acquire(&qnode);

  if (hent->values.empty()) {
    auto node_ent = (FreeNodeEntry *) brk.Alloc(sizeof(FreeNodeEntry));
    node_ent->sched = this;
    node_ent->ent = hent;
    node_ent->in_rvp_queue = false;
    node_ent->Initialize();

    node_ent->InsertAfter(inactive.prev);
    len++;
    g->RegisterFreeListener(hent->key, &PWVScheduler::OnNodeFree);
    g->RegisterRVPListener(hent->key, &PWVScheduler::OnNodeRVPChange);
    g->RegisterSchedEntry(hent->key, node_ent);
  }

  auto info = GetRVPInfo(value);
  if (info->is_rvp && info->indegree != 0) {
    value->InsertAfter(hent->values.prev);
  } else {
    value->InsertAfter(&hent->values);
  }

  qlock.Release(&qnode);
}

bool PWVScheduler::ShouldRetryBeforePick(std::atomic_ulong *zq_start, std::atomic_ulong *zq_end,
                                         std::atomic_uint *pq_start, std::atomic_uint *pq_end)
{
  while (CallTxnsWorker::g_finished < NodeConfiguration::g_nr_threads) {
    if (zq_start->load(std::memory_order_acquire) < zq_end->load(std::memory_order_acquire))
      return true;
    if (pq_start->load(std::memory_order_acquire) < pq_end->load(std::memory_order_acquire))
      return true;
  }

  return zq_start->load(std::memory_order_acquire) < zq_end->load(std::memory_order_acquire)
      || pq_start->load(std::memory_order_acquire) < pq_end->load(std::memory_order_acquire);
}

bool PWVScheduler::ShouldPickWaiting(const WaitState &ws)
{
  // util::MCSSpinLock::QNode qnode;
  // qlock.Acquire(&qnode);
  // if (len == 0) {
  //   abort_if(!free.empty(), "PWV: len is 0, but free queue isn't empty!");
  //   qlock.Release(&qnode);
  //   return true;
  // }
  // qlock.Release(&qnode);

  return len == 0;
}

PriorityQueueValue *PWVScheduler::Pick()
{
  abort_if(CallTxnsWorker::g_finished.load() != NodeConfiguration::g_nr_threads,
           "Should wait?");
  if (!is_graph_built.load(std::memory_order_acquire)) {
    util::Instance<PWVGraphManager>().local_graph()->Build();
    is_graph_built = true;
  }
  while (true) {
    while (nr_free.load() == 0) _mm_pause();
    util::MCSSpinLock::QNode qnode;
    qlock.Acquire(&qnode);

    auto free_node_ent = free.next->object();
    auto hashent = free_node_ent->ent;
    for (auto it = hashent->values.next; it != &hashent->values; it = it->next) {
      auto info = GetRVPInfo(it->object());
      if (!info->is_rvp || info->indegree.load() == 0) {
        auto result = it->object();
        it->Remove();
        it->InsertAfter(&hashent->values);
        qlock.Release(&qnode);
        return result;
      }
    }
    // full of RVPs! Move it to the rvp queue
    free_node_ent->Remove();
    free_node_ent->InsertAfter(rvp.prev);
    free_node_ent->in_rvp_queue = true;
    nr_free.fetch_sub(1);

    qlock.Release(&qnode);
  }
  // logger->info("seq {} inactive empty? {} free empty? {} len {}",
  //              0x00ffffff & (std::get<0>(value->promise_routine)->sched_key >> 8),
  //              inactive.empty(), free.empty(), len);
}

void PWVScheduler::Consume(PriorityQueueValue *value)
{
  util::MCSSpinLock::QNode qnode;
  qlock.Acquire(&qnode);
  abort_if(free.empty(), "PWV: cannot Consume() because free is empty");
  auto free_node_ent = free.next->object();
  auto hashent = free_node_ent->ent;
  abort_if(value != hashent->values.next->object(), "PWV: consume a different ptr than pick!");

  value->Remove();
  if (hashent->values.empty()) {
    hashent->Remove();
    free_node_ent->Remove();
    len--;
    nr_free.fetch_sub(1);
  }

  qlock.Release(&qnode);
}

void PWVScheduler::Reset()
{
  abort_if(!free.empty(), "PWV: free queue isn't empty! len {}", len);
  abort_if(!inactive.empty(), "PWV: inactive queue isn't empty!");
  abort_if(!rvp.empty(), "PWV: RVP queue isn't empty!");
  brk.Reset();
  // logger->info("free {} inactive {} rvp {} len {} nr free {}",
  //              (void *) &free, (void *) &inactive, (void *) &rvp, len, nr_free);
  is_graph_built = false;
}

size_t EpochExecutionDispatchService::g_max_item = 20_M;
const size_t EpochExecutionDispatchService::kHashTableSize = 100001;

EpochExecutionDispatchService::EpochExecutionDispatchService()
{
  auto max_item_percore = g_max_item / NodeConfiguration::g_nr_threads;
  auto max_txn_percore = PriorityTxnService::g_queue_length / NodeConfiguration::g_nr_threads;
  logger->info("{} per_core pool capacity {}, element size {}",
               (void *) this, max_item_percore, kPriorityQueuePoolElementSize);
  logger->info("{} per_core priority transaction queue length {}",
               (void *) this, max_txn_percore);
  Queue *qmem = nullptr;

  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto &queue = queues[i];
    auto d = std::div(i, mem::kNrCorePerNode);
    auto numa_node = d.quot;
    auto offset_in_node = d.rem;

    if (offset_in_node == 0) {
      qmem = (Queue *) mem::AllocMemory(
          mem::EpochQueueItem, sizeof(Queue) * mem::kNrCorePerNode, numa_node);
    }
    queue = qmem + offset_in_node;

    queue->zq.end = queue->zq.start = 0;
    queue->zq.q = (PieceRoutine **)
                 mem::AllocMemory(
                     mem::EpochQueuePromise,
                     max_item_percore * sizeof(PieceRoutine *),
                     numa_node);
    if (EpochClient::g_enable_pwv) {
      queue->pq.sched_pol = PWVScheduler::New(max_item_percore, numa_node);
    } else {
      queue->pq.sched_pol = ConservativePriorityScheduler::New(max_item_percore, numa_node);
    }
    queue->pq.ht = (PriorityQueueHashHeader *)
                  mem::AllocMemory(
                      mem::EpochQueueItem,
                      kHashTableSize * sizeof(PriorityQueueHashHeader),
                      numa_node);
    queue->pq.pending.q = (PieceRoutine **)
                         mem::AllocMemory(
                             mem::EpochQueuePromise,
                             max_item_percore * sizeof(PieceRoutine *),
                             numa_node);
    queue->pq.pending.start = 0;
    queue->pq.pending.end = 0;

    queue->pq.waiting.len = 0;

    for (size_t t = 0; t < kHashTableSize; t++) {
      queue->pq.ht[t].Initialize();
    }

    auto brk_sz = 64 * max_item_percore;
    queue->pq.brk = mem::Brk(
        mem::AllocMemory(mem::EpochQueueItem, brk_sz, numa_node),
        brk_sz);

    queue->tq.start = queue->tq.end = 0;
    queue->tq.q = nullptr;
    if (NodeConfiguration::g_priority_txn) {
      queue->tq.q = (PriorityTxn *)
                   mem::AllocMemory(
                       mem::EpochQueueItem,
                       max_txn_percore * sizeof(PriorityTxn),
                       numa_node);
    }

    new (&queue->state) State();
    new (&queue->lock) util::SpinLock();
  }
  tot_bubbles = 0;
}

void EpochExecutionDispatchService::Reset()
{
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto &q = queues[i];
    while (q->state.running == State::kDeciding) _mm_pause();
    q->zq.end.store(0);
    q->zq.start.store(0);

    q->state.ts = 0;
    q->state.current_sched_key = 0;
    // q->pq.len = 0;
    q->pq.brk.Reset();
    q->pq.sched_pol->Reset();
  }
  tot_bubbles = 0;
}


void EpochExecutionDispatchService::Add(int core_id, PieceRoutine **routines,
                                        size_t nr_routines, bool from_pri)
{
  auto &lock = queues[core_id]->lock;
  lock.Lock();

  auto &zq = queues[core_id]->zq;
  auto &pq = queues[core_id]->pq.pending;
  size_t i = 0;

  auto max_item_percore = g_max_item / NodeConfiguration::g_nr_threads;

again:
  size_t zdelta = 0,
           zend = zq.end.load(std::memory_order_acquire),
         zlimit = max_item_percore;

  size_t pdelta = 0,
           pend = pq.end.load(std::memory_order_acquire),
         plimit = max_item_percore
                  - (pend - pq.start.load(std::memory_order_acquire));

  for (; i < nr_routines; i++) {
    auto r = routines[i];
    auto key = r->sched_key;

    if (key == 0) {
      auto pos = zend + zdelta++;
      abort_if(pos >= zlimit,
               "Preallocation of DispatchService is too small. {} < {}", pos, zlimit);
      zq.q[pos] = r;
    } else {
      if (pdelta >= plimit) goto again;
      auto pos = pend + pdelta++;
      pq.q[pos % max_item_percore] = r;
    }
  }
  if (zdelta)
    zq.end.fetch_add(zdelta, std::memory_order_release);
  if (pdelta)
    pq.end.fetch_add(pdelta, std::memory_order_release);
  lock.Unlock();
  // util::Impl<VHandleSyncService>().Notify(1 << core_id);

  if (from_pri)
    ProcessPending(queues[core_id]->pq);
}

void EpochExecutionDispatchService::Add(int core_id, PriorityTxn *txn)
{
  auto &lock = queues[core_id]->lock;
  lock.Lock();

  auto &tq = queues[core_id]->tq;

  size_t tlimit = PriorityTxnService::g_queue_length / NodeConfiguration::g_nr_threads,
            pos = tq.end.load(std::memory_order_acquire);
  abort_if(pos >= tlimit,
           "Preallocation of priority txn queue is too small. {} < {}", pos, tlimit);

  tq.q[pos] = *txn;
  tq.end.fetch_add(1, std::memory_order_release);
  lock.Unlock();

  // trace(TRACE_PRIORITY "Priority txn {:p} - copied to queue {} at pos {}", (void*)txn, core_id, pos);
}

void
EpochExecutionDispatchService::AddToPriorityQueue(
    PriorityQueue &q, PieceRoutine *&rt,
    BasePieceCollection::ExecutionRoutine *state)
{
  bool smaller = false;
  auto node = (PriorityQueueValue *) q.brk.Alloc(64);
  // PQ Value is the piece
  node->Initialize();
  node->routine = rt;
  node->state = state;
  auto key = rt->sched_key;

  // HashHeader stores all the HashEntry with the same key>>8.
  // HashEntry stores all the piece with the same SID.
  auto &hl = q.ht[Hash(key) % kHashTableSize];
  auto *ent = hl.next;
  while (ent != &hl) {
    // looped linked list, till the end
    if (ent->object()->key == key) {
      // found this txn's HashEntry
      goto found;
    }
    ent = ent->next;
  }

  // first piece of this txn, create HashEntry
  ent = (PriorityQueueHashEntry *) q.brk.Alloc(64);
  ent->Initialize();
  // Initialize() is prev = next = this
  ent->object()->key = key;
  ent->object()->values.Initialize();
  // the linked list for all the pieces of this txn
  ent->InsertAfter(hl.prev);
  // insert this HashEntry after the tail of the HashHeader

found:
  // insert HashEntry into Heap, and insert the piece into the HashEntry
  q.sched_pol->IngestPending(ent->object(), node);
  // heap array inside scheduling policy
}

void
EpochExecutionDispatchService::ProcessPending(PriorityQueue &q)
{
  size_t pstart = q.pending.start.load(std::memory_order_acquire);
  long plen = q.pending.end.load(std::memory_order_acquire) - pstart;

  for (size_t i = 0; i < plen; i++) {
    auto pos = pstart + i;
    AddToPriorityQueue(q, q.pending.q[pos % (g_max_item / NodeConfiguration::g_nr_threads)]);
    // AddToPriorityQueue(q, q.pending.q[pos]);
  }
  if (plen) {
    q.pending.start.fetch_add(plen);
  }
}

// Peek the ZeroQueue and PQ to see if we have anything for this routine to run.
// if current ExecutionRoutine has more thing to run, return true
// if not (for instance you're temporarily spawned, or the q is empty), return false

// should_pop: callback function, use `BasePieceCollection::ExecutionRoutine *state`
// to determine should we pop another PieceRoutine from the queue (for current to execute).
//   para1: PieceRoutine, Peek() should pass the next routine to run
//   para2: ExecutionRoutine*, the state along with the previous PromiseRoutine

// the executionRoutines are stored in pq.waiting.states (WaitState, WaitState.state is ExecutionRoutine*)
// pq.state stores the state of the core (kSleeping, kRunning, kDeciding)
bool
EpochExecutionDispatchService::Peek(int core_id, DispatchPeekListener &should_pop)
{
  auto &zq = queues[core_id]->zq;
  auto &q = queues[core_id]->pq;
  auto &lock = queues[core_id]->lock;
  auto &state = queues[core_id]->state;
  uint64_t zstart = 0;

  state.running = State::kDeciding;

  if (!IsReady(core_id)) {
    state.running = State::kSleeping;
    return false;
  }

retry:
  zstart = zq.start.load(std::memory_order_acquire);
  if (zstart < zq.end.load(std::memory_order_acquire)) {
    state.running = State::kRunning;
    auto r = zq.q[zstart];
    if (should_pop(r, nullptr)) {
      zq.start.store(zstart + 1, std::memory_order_relaxed);
      state.current_sched_key = r->sched_key;
      return true;
    }
    return false;
  }
  // ZeroQueue has no piece to run


  // Setting state.running without poking around the data structure is very
  // important for performance. This let other thread create the co-routines
  // without spinning for State::kDeciding for a long time.
  if (q.sched_pol->empty() && q.waiting.len == 0
      && q.pending.end.load() == q.pending.start.load()) {
      // PQ has no piece, no coroutine waiting, pending has no piece
    state.running = State::kSleeping;
  } else {
    state.running = State::kRunning;
  }

  ProcessPending(q);
  if (q.sched_pol->ShouldRetryBeforePick(&zq.start, &zq.end, &q.pending.start, &q.pending.end))
    goto retry;
    // Caracal ConservativePriorityScheduler does not retry

  // check whether to run the waiting coroutine
  if (q.waiting.len > 0
      && (q.waiting.len == kOutOfOrderWindow
          || q.sched_pol->ShouldPickWaiting(q.waiting.states[q.waiting.len - 1]))) {
    auto &ws = q.waiting.states[q.waiting.len - 1];
    // q.waiting should be a stack so we don't get priority inversion
    if (should_pop(nullptr, ws.state)) {
      q.waiting.len--;
      state.current_sched_key = ws.sched_key;
      state.ts++;
      return true;
    }
    return false;
  }

  // if PQ not empty
  if (!q.sched_pol->empty()) {
    auto node = q.sched_pol->Pick();
    // PQValue*
    auto &rt = node->routine;

    // even should_pop(rt, nullptr) works, PQ has no pending pieces
    if (should_pop(rt, node->object()->state)) {
      q.sched_pol->Consume(node);
      state.current_sched_key = rt->sched_key;
      state.ts++;
      return true;
    }
    return false;
  }
  /*
  logger->info("pending start {} end {}, zstart {} zend {}, running {}, completed {}",
               q.pending.start.load(), q.pending.end.load(),
               zq.start.load(), zq.end.load(),
               state.running.load(), state.complete_counter.completed);
  */

  // We do not need locks to protect completion counters. There can only be MT
  // access on Pop() and Add(), the counters are per-core anyway.
  auto &c = state.complete_counter;
  auto n = c.completed;
  auto comp = EpochClient::g_workload_client->completion_object();
  c.completed = 0;

  unsigned long nr_bubbles = tot_bubbles.load();
  while (!tot_bubbles.compare_exchange_strong(nr_bubbles, 0));

  if (n + nr_bubbles > 0) {
    trace(TRACE_COMPLETION "DispatchService on core {} notifies {}+{} completions",
          core_id, n, nr_bubbles);
    comp->Complete(n + nr_bubbles);
  }

  auto &bc = state.batch_complete_counter;
  auto cnt = bc.completed;
  PriorityTxnService::BatchCnt.Decrement(cnt);
  bc.completed = 0;

  return false;
}

bool EpochExecutionDispatchService::Peek(int core_id, PriorityTxn *&txn, bool dry_run)
{
  if (!NodeConfiguration::g_priority_txn)
    return false;

  // hacks: for the time being we don't have occ yet, so only run priority txns
  //        during execution phase's pieces execution.
  // hack 1: if still during issuing, don't run
  if (!IsReady(core_id)) {
    return false;
  }

  // hack 2: if on this core, no batched pieces of this epoch has ever been run
  // (which leads to prog not being updated), don't run
  uint64_t prog = util::Instance<PriorityTxnService>().GetProgress(core_id);
  if (prog >> 32 != util::Instance<EpochManager>().current_epoch_nr())
    return false;

  // hack 3: make sure pq doesn't get run after this core has no piece to run
  if (queues[core_id]->pq.sched_pol->len == 0)
    return false;
  if (PriorityTxnService::BatchCnt.Get() == 0)
    return false;

  auto &tq = queues[core_id]->tq;
  auto tstart = tq.start.load(std::memory_order_acquire);
  if (tstart < tq.end.load(std::memory_order_acquire)) {
    PriorityTxn *candidate = tq.q + tstart;
    auto epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
    if (candidate->epoch != epoch_nr) {

      // hack 4: if a priority txn is from a previous epoch, skip it
      if (candidate->epoch < epoch_nr) {
        auto from = tstart;
        while (tstart < tq.end.load(std::memory_order_acquire) && candidate->epoch < epoch_nr)
          candidate = tq.q + ++tstart;
        tq.start.store(tstart, std::memory_order_release);
        // trace(TRACE_PRIORITY "core {} SKIPPED from pos {} ({}) to pos {} ({})", core_id, from, from * 32 + core_id + 1, tstart, tstart * 32 + core_id + 1);
      }

      return false;
    }

    if (__rdtsc() - PriorityTxnService::g_tsc < candidate->delay)
      return false;
    if (!dry_run) {
      tq.start.store(tstart + 1, std::memory_order_release);
      txn = candidate;
      EpochClient::g_workload_client->completion_object()->Increment(1);
    }

    // trace(TRACE_PRIORITY "core {} peeked on pos {} (pri id {}), txn {:p}", core_id, tstart, tstart * 32 + core_id + 1, (void*)txn);
    return true;
  }
  return false;
}

void EpochExecutionDispatchService::AddBubble()
{
  tot_bubbles.fetch_add(1);
}

// return: whether you should spawn a new coroutine.
//  if pieces with smaller priority exists in the PQ, then put the
//   ExecutionRoutine into the pq->waiting, and return true;
//  if not, return false.
bool EpochExecutionDispatchService::Preempt(int core_id, BasePieceCollection::ExecutionRoutine *routine_state)
{
  auto &lock = queues[core_id]->lock;
  bool can_preempt = true;
  auto &zq = queues[core_id]->zq;
  auto &q = queues[core_id]->pq;
  auto &state = queues[core_id]->state;

  ProcessPending(q);

  auto key = state.current_sched_key;
  if (key == 0)
    return false; // sched_key == 0 and preempt isn't supported.

  abort_if(q.waiting.len == kOutOfOrderWindow, "out-of-order scheduling window is full");

  auto &ws = q.waiting.states[q.waiting.len];
  ws.preempt_ts = state.ts;
  ws.sched_key = state.current_sched_key;
  ws.state = routine_state;
  // store everything first

  PriorityTxn* tmp = nullptr;
  if (NodeConfiguration::g_priority_txn &&
      PriorityTxnService::g_priority_preemption &&
      this->Peek(core_id, tmp, true)) {
    abort_if(tmp != nullptr, "dry run didn't come out dry");
    q.waiting.len++;
    // enqueue this coroutine
    return true;
  }

  // There is nothing to switch to!
  if (q.waiting.len == 0 && q.sched_pol->ShouldPickWaiting(ws))
    return false;

  q.waiting.len++;
  return true;
}

void EpochExecutionDispatchService::Complete(int core_id, bool priority)
{
  auto &state = queues[core_id]->state;
  auto &c = state.complete_counter;
  c.completed++;
  if (!priority) {
    auto &bc = state.batch_complete_counter;
    bc.completed++;
  }
}

int EpochExecutionDispatchService::TraceDependency(uint64_t key)
{
  for (int core_id = 0; core_id < NodeConfiguration::g_nr_threads; core_id++) {
    auto max_item_percore = g_max_item / NodeConfiguration::g_nr_threads;
    auto &q = queues[core_id]->pq.pending;
    if (q.end.load() > max_item_percore) puts("pending queue wraps around");
    abort_if(q.end.load() < q.start.load(), "WTF? pending queue underflows");
    for (auto i = q.start.load(); i < q.end.load(); i++) {
      if (q.q[i % max_item_percore]->sched_key == key) {
        printf("found %lu in the pending area of %d\n", key, core_id);
      }
    }
    for (auto i = 0; i < q.start.load(); i++) {
      if (q.q[i % max_item_percore]->sched_key == key) {
        printf("found %lu in the consumed pending area of %d\n", key, core_id);
      }
    }

    auto &hl = queues[core_id]->pq.ht[Hash(key) % kHashTableSize];
    auto ent = hl.next;
    while (ent != &hl) {
      if (ent->object()->key == key) {
        if (ent->object()->values.empty()) {
          printf("found but empty hash entry of key %lu on core %d\n", key, core_id);
        }
        return core_id;
      }
      ent = ent->next;
    }
    if (queues[core_id]->state.current_sched_key == key)
      return core_id;
  }
  return -1;
}

bool EpochExecutionDispatchService::IsReady(int core_id)
{
  return EpochClient::g_workload_client->get_worker(core_id)->call_worker.has_finished();
}

}
