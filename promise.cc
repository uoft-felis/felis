#include "promise.h"
#include "epoch.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

#include <queue>
#include "util.h"
#include "opts.h"
#include "mem.h"

using util::Instance;
using util::Impl;

namespace felis {

static constexpr size_t kPromiseRoutineHeader = sizeof(go::Routine);
static_assert(kPromiseRoutineHeader % 8 == 0); // Has to be aligned!

PromiseRoutine *PromiseRoutine::CreateFromCapture(size_t capture_len)
{
  auto r = (PromiseRoutine *) BasePromise::Alloc(sizeof(PromiseRoutine));
  r->capture_len = capture_len;
  r->capture_data = (uint8_t *) BasePromise::Alloc(util::Align(capture_len));
  r->sched_key = 0;
  r->level = 0;
  r->affinity = std::numeric_limits<uint64_t>::max();

  r->callback = nullptr;
  r->node_func = nullptr;
  r->next = nullptr;
  return r;
}

PromiseRoutineWithInput
PromiseRoutine::CreateFromPacket(uint8_t *p, size_t packet_len)
{
  uint16_t len;
  memcpy(&len, p, 2);

  uint16_t aligned_len = util::Align(2 + len);
  auto *input_ptr = (uint8_t *) BasePromise::Alloc(aligned_len);
  memcpy(input_ptr, p + 2, aligned_len - 2);

  p += aligned_len;

  auto r = (PromiseRoutine *) BasePromise::Alloc(sizeof(PromiseRoutine));
  auto result_len = r->DecodeNode(p, packet_len - aligned_len);
  abort_if(result_len != packet_len - aligned_len,
           "DecodeNode() consumes {} but passed in {} bytes",
           result_len, packet_len - aligned_len);
  VarStr input(len, 0, input_ptr);
  return std::make_tuple(r, input);
}

size_t PromiseRoutine::TreeSize(const VarStr &input) const
{
  return util::Align(2 + input.len) + NodeSize();
}

void PromiseRoutine::EncodeTree(uint8_t *p, const VarStr &input)
{
  // Format: input data are placed before the tree root. Every tree node
  // addressess are aligned to 8 bytes. (C standards)
  uint8_t *start = p;
  memcpy(p, &input.len, 2);
  memcpy(p + 2, input.data, input.len);
  p += util::Align(2 + input.len);
  EncodeNode(p);
}

size_t PromiseRoutine::NodeSize() const
{
  size_t s = util::Align(sizeof(PromiseRoutine))
             + util::Align(capture_len)
             + 8;

  for (size_t i = 0; next && i < next->nr_handlers; i++) {
    auto *child = next->routine(i);
    s += child->NodeSize();
  }

  return s;
}

uint8_t *PromiseRoutine::EncodeNode(uint8_t *p)
{
  memcpy(p, this, sizeof(PromiseRoutine));
  auto node = (PromiseRoutine *) p;
  p += util::Align(sizeof(PromiseRoutine));

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

size_t PromiseRoutine::DecodeNode(uint8_t *p, size_t len)
{
  uint8_t *orig_p = p;
  size_t off = util::Align(sizeof(PromiseRoutine));
  memcpy(this, p, off);
  p += off;

  off = util::Align(capture_len);
  capture_data = (uint8_t *) BasePromise::Alloc(off);
  memcpy(capture_data, p, off);
  p += off;

  next = nullptr;

  size_t nr_children = 0;
  memcpy(&nr_children, p, 8);
  p += 8;

  if (nr_children > 0) {
    next = new BasePromise(nr_children);
    for (int i = 0; i < nr_children; i++) {
      auto child = (PromiseRoutine *) BasePromise::Alloc(sizeof(PromiseRoutine));
      off = child->DecodeNode(p, len - (p - orig_p));
      p += off;
      next->Add(child);
    }
  }
  return p - orig_p;
}

size_t BasePromise::g_nr_threads = 0;

BasePromise::BasePromise(int limit)
    : limit(limit), nr_handlers(0), extra_handlers(nullptr)
{
  if (limit > kInlineLimit) {
    extra_handlers = (PromiseRoutine **)Alloc(util::Align(
        sizeof(PromiseRoutine *) * (limit - kInlineLimit), CACHE_LINE_SIZE));
  }
}

void *BasePromise::operator new(std::size_t size)
{
  return BasePromise::Alloc(size);
}

void *BasePromise::Alloc(size_t size)
{
  return util::Impl<PromiseAllocationService>().Alloc(size);
}

void BasePromise::AssignSchedulingKey(uint64_t key)
{
  for (int i = 0; i < nr_handlers; i++) {
    auto *child = routine(i);
    if (child->sched_key == 0)
      child->sched_key = key;
    if (child->next)
      child->next->AssignSchedulingKey(key);
  }
}

void BasePromise::AssignAffinity(uint64_t aff)
{
  for (int i = 0; i < nr_handlers; i++) {
    auto *child = routine(i);
    if (child->affinity == std::numeric_limits<uint64_t>::max())
      child->affinity = aff;
    if (child->next)
      child->next->AssignAffinity(aff);
  }
}

void BasePromise::Add(PromiseRoutine *child)
{
  abort_if(nr_handlers >= kMaxHandlersLimit,
           "nr_handlers {} exceeding limits!", nr_handlers);
  routine(nr_handlers++) = child;
}

void BasePromise::Complete(const VarStr &in)
{
  auto &transport = util::Impl<PromiseRoutineTransportService>();
  for (size_t i = 0; i < nr_handlers; i++) {
    auto r = routine(i);

    // This is a dynamically dispatched piece.
    if (r->node_id == 255) {
      auto real_node = r->node_func(r, in);
      auto max_node = transport.GetNumberOfNodes();
      VarStr bubble(0, 0, (uint8_t *) PromiseRoutine::kBubblePointer);
      for (r->node_id = 1; r->node_id <= max_node; r->node_id++) {
        if (r->node_id == real_node) {
          transport.TransportPromiseRoutine(r, in);
        } else {
          transport.TransportPromiseRoutine(r, bubble);
        }
      }
    } else {
      transport.TransportPromiseRoutine(r, in);
    }
  }

  // Recursively complete all bubbles.
  if (in.data == (uint8_t *) PromiseRoutine::kBubblePointer) {
    for (size_t i = 0; i < nr_handlers; i++) {
      auto r = routine(i);
      if (r->next) r->next->Complete(in);
    }
  }
}

void BasePromise::ExecutionRoutine::RunPromiseRoutine(PromiseRoutine *r, const VarStr &in)
{
  INIT_ROUTINE_BRK(8192);
  r->callback((PromiseRoutine *) r, in);
}

void BasePromise::ExecutionRoutine::AddToReadyQueue(go::Scheduler::Queue *q, bool next_ready)
{
  auto p = q;
  while (p->next != q) {
    if (!((Routine *) p->next)->is_urgent())
      break;
    p = p->next;
  }
  Add(p);
}

void BasePromise::ExecutionRoutine::Run()
{
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  auto &transport = util::Impl<PromiseRoutineTransportService>();

  int core_id = scheduler()->thread_pool_id() - 1;
  if (!svc.IsReady(core_id))
    return;

  trace(TRACE_EXEC_ROUTINE "new ExecutionRoutine up and running on {}", core_id);

  PromiseRoutineWithInput next_r;
  // BasePromise::ExecutionRoutine *next_state = nullptr;
  go::Scheduler *sched = scheduler();

  auto should_pop = PromiseRoutineDispatchService::GenericDispatchPeekListener(
      [&next_r, sched]
      (PromiseRoutineWithInput r, BasePromise::ExecutionRoutine *state) -> bool {
        if (state != nullptr) {
          if (state->is_detached()) {
            trace(TRACE_EXEC_ROUTINE "Wakeup Coroutine {}", (void *) state);
            state->Init();
            sched->WakeUp(state);
          } else {
            trace(TRACE_EXEC_ROUTINE "Found a sleeping Coroutine, but it's already awaken.");
          }
          return false;
        }
        next_r = r;
        // next_state = state;
        return true;
      });


  unsigned long cnt = 0x0FF;
  unsigned long last_io = 0;
  while (svc.Peek(core_id, should_pop)) {

    // Periodic flush
    cnt++;
    if ((cnt & 0x0FF) == 0) {
      unsigned long now = __rdtsc();
      if (now - last_io > 60000) {
        last_io = now;
        transport.PeriodicIO(core_id);
      }
    }

    auto [rt, in] = next_r;
    if (rt->sched_key != 0)
      debug(TRACE_EXEC_ROUTINE "Run {} sid {}", (void *) rt, rt->sched_key);

    RunPromiseRoutine(rt, in);
    svc.Complete(core_id);
  }
  trace(TRACE_EXEC_ROUTINE "Coroutine Exit");
}

bool BasePromise::ExecutionRoutine::Preempt(bool force)
{
  // TODO:
  //
  // `force` should be deprecated? It means I'll preempt no matter what, even if
  // the scheduler tell me not to. This is why, under this setting, no matter
  // what we'll always need to spawn a new routine. However, I doubt if we ever
  // need anything like this?
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  int core_id = scheduler()->thread_pool_id() - 1;
  bool spawn = !force;

  if (svc.Preempt(core_id, force, this)) {
 sleep:
    if (spawn) {
      sched->WakeUp(new ExecutionRoutine());
    }
    trace(TRACE_EXEC_ROUTINE "Sleep. Spawning a new coroutine = {}. force = {}", spawn, force);
    sched->RunNext(go::Scheduler::SleepState);

    spawn = true;
    auto should_pop = PromiseRoutineDispatchService::GenericDispatchPeekListener(
        [this, &spawn]
        (PromiseRoutineWithInput r, BasePromise::ExecutionRoutine *state) -> bool {
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

void BasePromise::QueueRoutine(felis::PromiseRoutineWithInput *routines, size_t nr_routines,
                               int source_idx, int thread, bool batch)
{
  util::Impl<PromiseRoutineDispatchService>().Add(thread - 1, routines, nr_routines);
}

void BasePromise::FlushScheduler()
{
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  for (int i = 1; i <= g_nr_threads; i++) {
    if (!svc.IsRunning(i - 1))
      go::GetSchedulerFromPool(i)->WakeUp(new ExecutionRoutine());
  }
}

}
