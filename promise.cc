#include "promise.h"
#include "epoch.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"
#include <queue>
#include "util.h"
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
  r->next = nullptr;
  return r;
}

std::tuple<PromiseRoutine *, VarStr>
PromiseRoutine::CreateFromPacket(go::TcpInputChannel *in, size_t packet_len)
{
  uint16_t len;
  in->Read(&len, 2);

  uint16_t aligned_len = util::Align(2 + len);
  auto *input_ptr = (uint8_t *) BasePromise::Alloc(aligned_len);
  in->Read(input_ptr, aligned_len - 2);

  auto r = (PromiseRoutine *) BasePromise::Alloc(sizeof(PromiseRoutine));
  r->DecodeNode(in);
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

void PromiseRoutine::DecodeNode(go::TcpInputChannel *in)
{
  in->Read(this, util::Align(sizeof(PromiseRoutine)));

  capture_data = (uint8_t *) BasePromise::Alloc(util::Align(capture_len));
  in->Read(capture_data, util::Align(capture_len));

  next = nullptr;

  size_t nr_children = 0;
  in->Read(&nr_children, 8);

  if (nr_children > 0) {
    next = new BasePromise(nr_children);
    for (int i = 0; i < nr_children; i++) {
      auto child = (PromiseRoutine *) BasePromise::Alloc(sizeof(PromiseRoutine));
      child->DecodeNode(in);
      next->Add(child);
    }
  }
}

PromiseProc::~PromiseProc()
{
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
    child->sched_key = key;
    if (child->next)
      child->next->AssignSchedulingKey(key);
  }
}

void BasePromise::AssignSequence(uint64_t seq)
{
  for (int i = 0; i < nr_handlers; i++) {
    auto *child = routine(i);
    child->seq = seq;
    if (child->next)
      child->next->AssignSequence(seq);
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
    transport.TransportPromiseRoutine(r, in);
  }
  if (in.data == (uint8_t *) PromiseRoutine::kBubblePointer) {
    for (size_t i = 0; i < nr_handlers; i++) {
      auto r = routine(i);
      if (r->next) r->next->Complete(in);
    }
  }
}

static std::unique_ptr<util::CacheAligned<std::atomic_ulong>[]> batch_counts;

void BasePromise::InitializeSourceCount(int nr_sources, size_t nr_threads)
{
  g_nr_threads = nr_threads;
  batch_counts.reset(new util::CacheAligned<std::atomic_ulong>[g_nr_threads + 1]);
  for (int i = 0; i < g_nr_threads; i++) {
    batch_counts[i].elem = 0;
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
  int core_id = scheduler()->thread_pool_id() - 1;
  // logger->info("new ExecutionRoutine up and running on {}", core_id);

  PromiseRoutineWithInput next_r;
  // BasePromise::ExecutionRoutine *next_state = nullptr;
  go::Scheduler *sched = scheduler();

  auto should_pop = PromiseRoutineDispatchService::GenericDispatchPeekListener(
      [&next_r, sched]
      (PromiseRoutineWithInput r, BasePromise::ExecutionRoutine *state) -> bool {
        if (state != nullptr) {
          if (state->is_detached()) {
            state->Init();
            sched->WakeUp(state);
          }
          return false;
        }
        next_r = r;
        // next_state = state;
        return true;
      });

  while (svc.Peek(core_id, should_pop)) {
    auto [rt, in] = next_r;
    RunPromiseRoutine(rt, in);
    svc.Complete(core_id);
  }
}

bool BasePromise::ExecutionRoutine::Preempt(bool force)
{
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  int core_id = scheduler()->thread_pool_id() - 1;
  bool spawn = !force;

  if (svc.Preempt(core_id, force)) {
    set_busy_poll(true);
    // logger->info("Initial sleep. Spawning a new coroutine. force = {}", force);
 sleep:
    if (spawn)
      sched->WakeUp(new ExecutionRoutine());
    sched->RunNext(go::Scheduler::SleepState);

    auto should_pop = PromiseRoutineDispatchService::GenericDispatchPeekListener(
        [this, &spawn]
        (PromiseRoutineWithInput r, BasePromise::ExecutionRoutine *state) -> bool {
          if (state == this)
            return true;
          if (state != nullptr) {
            if (state->is_detached()) {
              state->Init();
              sched->WakeUp(state);
            }
            spawn = false;
          } else {
            logger->info("Spawning because I saw a new piece");
          }
          return false;
        });

    spawn = true;
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
