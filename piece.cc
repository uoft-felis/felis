#include "piece.h"
#include "epoch.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

#include <queue>
#include "util/objects.h"
#include "util/arch.h"
#include "opts.h"
#include "mem.h"
#include "priority.h"

using util::Instance;
using util::Impl;

namespace felis {

static constexpr size_t kPromiseRoutineHeader = sizeof(go::Routine);
static_assert(kPromiseRoutineHeader % 8 == 0); // Has to be aligned!

PieceRoutine *PieceRoutine::CreateFromCapture(size_t capture_len)
{
  auto r = (PieceRoutine *) BasePieceCollection::Alloc(sizeof(PieceRoutine));
  r->capture_len = capture_len;
  r->capture_data = (uint8_t *) BasePieceCollection::Alloc(util::Align(capture_len));
  r->sched_key = 0;
  r->level = 0;
  r->affinity = std::numeric_limits<uint64_t>::max();

  r->callback = nullptr;
  r->next = nullptr;
  std::fill(r->__padding__, r->__padding__ + 8, 0);
  return r;
}

PieceRoutine *
PieceRoutine::CreateFromPacket(uint8_t *p, size_t packet_len)
{
  auto r = (PieceRoutine *) BasePieceCollection::Alloc(sizeof(PieceRoutine));
  auto result_len = r->DecodeNode(p, packet_len);
  abort_if(result_len != packet_len,
           "DecodeNode() consumes {} but passed in {} bytes",
           result_len, packet_len);
  return r;
}

size_t PieceRoutine::NodeSize() const
{
  size_t s = util::Align(sizeof(PieceRoutine))
             + util::Align(capture_len)
             + 8;

  for (size_t i = 0; next && i < next->nr_handlers; i++) {
    auto *child = next->routine(i);
    s += child->NodeSize();
  }

  return s;
}

uint8_t *PieceRoutine::EncodeNode(uint8_t *p)
{
  memcpy(p, this, sizeof(PieceRoutine));
  auto node = (PieceRoutine *) p;
  p += util::Align(sizeof(PieceRoutine));

  memcpy(p, capture_data, capture_len);
  p += util::Align(capture_len);

  size_t nr_children = next ? next->nr_handlers : 0;
  memcpy(p, &nr_children, 8);
  p += 8;

  for (size_t i = 0; next && i < next->nr_handlers; i++) {
    auto *child = next->routine(i);
    p = child->EncodeNode(p);
  }
  return p;
}

size_t PieceRoutine::DecodeNode(uint8_t *p, size_t len)
{
  uint8_t *orig_p = p;
  size_t off = util::Align(sizeof(PieceRoutine));
  memcpy(this, p, off);
  p += off;

  off = util::Align(capture_len);
  capture_data = (uint8_t *) BasePieceCollection::Alloc(off);
  memcpy(capture_data, p, off);
  p += off;

  next = nullptr;

  size_t nr_children = 0;
  memcpy(&nr_children, p, 8);
  p += 8;

  if (nr_children > 0) {
    next = new BasePieceCollection(nr_children);
    for (int i = 0; i < nr_children; i++) {
      auto child = (PieceRoutine *) BasePieceCollection::Alloc(sizeof(PieceRoutine));
      off = child->DecodeNode(p, len - (p - orig_p));
      p += off;
      next->Add(child);
    }
  }
  return p - orig_p;
}

size_t BasePieceCollection::g_nr_threads = 0;

BasePieceCollection::BasePieceCollection(int limit)
    : limit(limit), nr_handlers(0), extra_handlers(nullptr)
{
  if (limit > kInlineLimit) {
    extra_handlers = (PieceRoutine **)Alloc(util::Align(
        sizeof(PieceRoutine *) * (limit - kInlineLimit), CACHE_LINE_SIZE));
  }
}

void *BasePieceCollection::operator new(std::size_t size)
{
  return BasePieceCollection::Alloc(size);
}

void *BasePieceCollection::Alloc(size_t size)
{
  return util::Impl<PromiseAllocationService>().Alloc(size);
}

void BasePieceCollection::AssignSchedulingKey(uint64_t key)
{
  for (int i = 0; i < nr_handlers; i++) {
    auto *child = routine(i);
    if (child->sched_key == 0)
      child->sched_key = key;
    if (child->next)
      child->next->AssignSchedulingKey(key);
  }
}

void BasePieceCollection::AssignAffinity(uint64_t aff)
{
  for (int i = 0; i < nr_handlers; i++) {
    auto *child = routine(i);
    if (child->affinity == std::numeric_limits<uint64_t>::max())
      child->affinity = aff;
    if (child->next)
      child->next->AssignAffinity(aff);
  }
}

void BasePieceCollection::Add(PieceRoutine *child)
{
  abort_if(nr_handlers >= limit,
           "nr_handlers {} exceeding limits!", nr_handlers);
  routine(nr_handlers++) = child;
}

void BasePieceCollection::Complete()
{
  auto &transport = util::Impl<PromiseRoutineTransportService>();
  for (size_t i = 0; i < nr_handlers; i++) {
    auto r = routine(i);
    transport.TransportPromiseRoutine(r);
  }
}

void BasePieceCollection::ExecutionRoutine::AddToReadyQueue(go::Scheduler::Queue *q, bool next_ready)
{
  auto p = q;
  while (p->next != q) {
    if (!((Routine *) p->next)->is_urgent())
      break;
    p = p->next;
  }
  Add(p);
}

void BasePieceCollection::ExecutionRoutine::Run()
{
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  auto &transport = util::Impl<PromiseRoutineTransportService>();

  int core_id = scheduler()->thread_pool_id() - 1;
  trace(TRACE_EXEC_ROUTINE "new ExecutionRoutine up and running on {}", core_id);

  PieceRoutine *next_r;
  bool give_up = false;
  // BasePromise::ExecutionRoutine *next_state = nullptr;
  go::Scheduler *sched = scheduler();

  auto should_pop = PromiseRoutineDispatchService::GenericDispatchPeekListener(
      [&next_r, &give_up, sched]
      (PieceRoutine *r, BasePieceCollection::ExecutionRoutine *state) -> bool {
        if (state != nullptr) {
          if (state->is_detached()) {
            trace(TRACE_EXEC_ROUTINE "Wakeup Coroutine {}", (void *) state);
            state->Init();
            sched->WakeUp(state);
          } else {
            trace(TRACE_EXEC_ROUTINE "Found a sleeping Coroutine, but it's already awaken.");
          }
          give_up = true;
          return false;
        }
        give_up = false;
        next_r = r;
        // next_state = state;
        return true;
      });


  unsigned long cnt = 0x01F;
  bool hasTxn, hasPiece;
  PriorityTxn *txn;

  do {
    while ((hasTxn = svc.Peek(core_id, txn)) | (hasPiece = svc.Peek(core_id, should_pop))) {
      // running priority: pieces from priority txn > issue of priority txn > pieces from batched txn
      if (hasTxn && !hasPiece) {
        txn->Run();
        svc.Complete(core_id);
        continue;
      }

      // Periodic flush
      cnt++;
      if ((cnt & 0x01F) == 0) {
        transport.PeriodicIO(core_id);
      }

      auto rt = next_r;
      if (rt->sched_key != 0)
        debug(TRACE_EXEC_ROUTINE "Run {} sid {}", (void *) rt, rt->sched_key);

      bool pieceFromPriTxn = PriorityTxnService::isPriorityTxn(rt->sched_key);
      if (hasTxn && !pieceFromPriTxn) {
        // if piece is from batched txn, priority txn issue runs before piece exec
        txn->Run();
        svc.Complete(core_id);
      }

      if (NodeConfiguration::g_priority_txn) {
        util::Instance<PriorityTxnService>().UpdateProgress(core_id, rt->sched_key);
      }

      auto tsc = __rdtsc();
      rt->callback(rt);
      auto diff = (__rdtsc() - tsc) / 2200;
      if (rt->sched_key != 0)
        felis::probes::PieceTime{diff, rt->sched_key}();
      svc.Complete(core_id);

      if (hasTxn && pieceFromPriTxn) {
        // priority txn issue runs after piece exec
        txn->Run();
        svc.Complete(core_id);
      }
    }
  } while (!give_up && svc.IsReady(core_id) && transport.PeriodicIO(core_id));

  trace(TRACE_EXEC_ROUTINE "Coroutine Exit on core {} give up {}", core_id, give_up);
}

bool BasePieceCollection::ExecutionRoutine::Preempt()
{
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  int core_id = scheduler()->thread_pool_id() - 1;
  bool spawn = true;

  if (svc.Preempt(core_id, this)) {
 sleep:
    if (spawn) {
      sched->WakeUp(new ExecutionRoutine());
    }
    trace(TRACE_EXEC_ROUTINE "Sleep. Spawning a new coroutine = {}.", spawn);
    sched->RunNext(go::Scheduler::SleepState);

    spawn = true;
    auto should_pop = PromiseRoutineDispatchService::GenericDispatchPeekListener(
        [this, &spawn]
        (PieceRoutine *, BasePieceCollection::ExecutionRoutine *state) -> bool {
          if (state == this)
            return true;
          if (state != nullptr) {
            trace(TRACE_EXEC_ROUTINE "Unfinished encoutered, no spawn.");
            if (state->is_detached()) {
              state->Init();
              sched->WakeUp(state);
            }
            spawn = false;
          } else {
            trace(TRACE_EXEC_ROUTINE "Spawning because I saw a new piece");
          }
          return false;
        });

    trace(TRACE_EXEC_ROUTINE "Just got up!");
    if (!svc.Peek(core_id, should_pop))
      goto sleep;

    set_busy_poll(false);
    return true;
  }
  return false;
}

void BasePieceCollection::QueueRoutine(PieceRoutine **routines, size_t nr_routines, int core_id)
{
  util::Impl<PromiseRoutineDispatchService>().Add(core_id, routines, nr_routines);
}

void BasePieceCollection::FlushScheduler()
{
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  for (int i = 0; i < g_nr_threads; i++) {
    if (!svc.IsRunning(i)) {
      go::GetSchedulerFromPool(i + 1)->WakeUp(new ExecutionRoutine());
    }
  }
}

}
