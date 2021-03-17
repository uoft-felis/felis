#include <sys/time.h>

#include "epoch.h"
#include "routine_sched.h"
#include "pwv_graph.h"

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
    std::push_heap(q, q + len, Greater);
  }
  value->InsertAfter(hent->values.prev);
}

bool ConservativePriorityScheduler::ShouldPickWaiting(const WaitState &ws)
{
  if (len == 0)
    return true;
  if (q[0].key > ws.sched_key)
    return true;
  return false;
}

PriorityQueueValue *ConservativePriorityScheduler::Pick()
{
  return q[0].ent->values.next->object();
}

void ConservativePriorityScheduler::Consume(PriorityQueueValue *node)
{
  node->Remove();
  auto top = q[0];
  if (top.ent->values.empty()) {
    std::pop_heap(q, q + len, Greater);
    q[len - 1].ent = nullptr;
    len--;
    top.ent->Remove(); // from the hashtable
  }
}

class PWVScheduler final : public PrioritySchedulingPolicy {
  PWVScheduler(void *p, size_t lmt) : brk(p, lmt) {
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

  bool ShouldRetryBeforePick(std::atomic_ulong *zq_start, std::atomic_ulong *zq_end) override;
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
  abort_if(is_graph_built, "graph is already built!");
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

bool PWVScheduler::ShouldRetryBeforePick(std::atomic_ulong *zq_start, std::atomic_ulong *zq_end)
{
  while (CallTxnsWorker::g_finished < NodeConfiguration::g_nr_threads
         && zq_start->load(std::memory_order_acquire) == zq_end->load(std::memory_order_acquire))
      _mm_pause();
  return zq_start->load(std::memory_order_acquire) < zq_end->load(std::memory_order_acquire);
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
  logger->info("free {} inactive {} rvp {} len {} nr free {}",
               (void *) &free, (void *) &inactive, (void *) &rvp, len, nr_free);
  is_graph_built = false;
}

size_t EpochExecutionDispatchService::g_max_item = 20_M;
const size_t EpochExecutionDispatchService::kHashTableSize = 100001;

EpochExecutionDispatchService::EpochExecutionDispatchService()
{
  auto max_item_percore = g_max_item / NodeConfiguration::g_nr_threads;
  logger->info("per_core pool capacity {}, element size {}",
               max_item_percore, kPriorityQueuePoolElementSize);
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

    queue->pq.waiting.off = queue->pq.waiting.len = 0;

    for (size_t t = 0; t < kHashTableSize; t++) {
      queue->pq.ht[t].Initialize();
    }

    auto brk_sz = 64 * max_item_percore;
    queue->pq.brk = mem::Brk(
        mem::AllocMemory(mem::EpochQueueItem, brk_sz, numa_node),
        brk_sz);

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
                                        size_t nr_routines)
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
}

void
EpochExecutionDispatchService::AddToPriorityQueue(
    PriorityQueue &q, PieceRoutine *&rt,
    BasePieceCollection::ExecutionRoutine *state)
{
  bool smaller = false;
  auto node = (PriorityQueueValue *) q.brk.Alloc(64);
  node->Initialize();
  node->routine = rt;
  node->state = state;
  auto key = rt->sched_key;

  auto &hl = q.ht[Hash(key) % kHashTableSize];
  auto *ent = hl.next;
  while (ent != &hl) {
    if (ent->object()->key == key) {
      goto found;
    }
    ent = ent->next;
  }

  ent = (PriorityQueueHashEntry *) q.brk.Alloc(64);
  ent->Initialize();
  ent->object()->key = key;
  ent->object()->values.Initialize();
  ent->InsertAfter(hl.prev);

found:
  q.sched_pol->IngestPending(ent->object(), node);
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

  // Setting state.running without poking around the data structure is very
  // important for performance. This let other thread create the co-routines
  // without spinning for State::kDeciding for a long time.
  if (q.sched_pol->empty() && q.waiting.len == 0
      && q.pending.end.load() == q.pending.start.load()) {
    state.running = State::kSleeping;
  } else {
    state.running = State::kRunning;
  }

  ProcessPending(q);
  if (q.sched_pol->ShouldRetryBeforePick(&zq.start, &zq.end))
    goto retry;

  auto &ws = q.waiting.states[q.waiting.off];
  if (q.waiting.len > 0
      && (q.waiting.len == kOutOfOrderWindow
          || q.sched_pol->ShouldPickWaiting(q.waiting.states[q.waiting.off]))) {
    // TODO: is this right?
    if (should_pop(nullptr, ws.state)) {
      q.waiting.off = (q.waiting.off + 1) % kOutOfOrderWindow;
      q.waiting.len--;
      state.current_sched_key = ws.sched_key;
      state.ts++;
      return true;
    }
    return false;
  }

  if (!q.sched_pol->empty()) {
    auto node = q.sched_pol->Pick();
    auto &rt = node->routine;

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
  return false;
}

void EpochExecutionDispatchService::AddBubble()
{
  tot_bubbles.fetch_add(1);
}

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

  auto &ws = q.waiting.states[(q.waiting.off + q.waiting.len) % kOutOfOrderWindow];
  ws.preempt_ts = state.ts;
  ws.sched_key = state.current_sched_key;
  ws.state = routine_state;

  // There is nothing to switch to!
  if (q.waiting.len == 0 && q.sched_pol->ShouldPickWaiting(ws))
    return false;

  q.waiting.len++;
  return true;
}

void EpochExecutionDispatchService::Complete(int core_id)
{
  auto &state = queues[core_id]->state;
  auto &c = state.complete_counter;
  c.completed++;
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
