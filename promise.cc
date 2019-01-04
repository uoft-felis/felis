#include "promise.h"
#include "epoch.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"
#include <queue>
#include "util.h"
#include "mem.h"
#include "iface.h"

using util::Instance;
using util::Impl;

namespace felis {

static constexpr size_t kPromiseRoutineHeader = sizeof(go::Routine);
static_assert(kPromiseRoutineHeader % 8 == 0); // Has to be aligned!

PromiseRoutine *PromiseRoutine::CreateFromCapture(size_t capture_len)
{
  PromiseRoutine *r = new BasePromise::ExecutionRoutine();
  r->capture_len = capture_len;
  r->capture_data = (uint8_t *) BasePromise::Alloc(util::Align(capture_len, 8));
  r->sched_key = 0;
  r->next = nullptr;
  return r;
}

PromiseRoutine *PromiseRoutine::CreateFromPacket(go::TcpInputChannel *in, size_t packet_len)
{
  uint16_t len;
  in->Read(&len, 2);

  uint16_t aligned_len = util::Align(2 + len, 8);
  auto *input_ptr = (uint8_t *) BasePromise::Alloc(aligned_len);
  in->Read(input_ptr, aligned_len - 2);

  auto r = (PromiseRoutine *) new BasePromise::ExecutionRoutine();
  r->DecodeNode(in);
  r->input.data = input_ptr;
  return r;
}

size_t PromiseRoutine::TreeSize() const
{
  return util::Align(2 + input.len, 8) + NodeSize();
}

void PromiseRoutine::EncodeTree(uint8_t *p)
{
  // Format: input data are placed before the tree root. Every tree node
  // addressess are aligned to 8 bytes. (C standards)
  uint8_t *start = p;
  memcpy(p, &input.len, 2);
  memcpy(p + 2, input.data, input.len);
  p += util::Align(2 + input.len, 8);
  EncodeNode(p);
}

size_t PromiseRoutine::NodeSize() const
{
  size_t s = util::Align(sizeof(PromiseRoutine), 8)
             + util::Align(capture_len, 8)
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
  p += util::Align(sizeof(PromiseRoutine), 8);

  memcpy(p, capture_data, capture_len);
  p += util::Align(capture_len, 8);

  size_t nr_children = next ? next->nr_handlers : 0;
  memcpy(p, &nr_children, 8);
  p += 8;

  if (next) {
    for (size_t i = 0; i < next->nr_handlers; i++) {
      auto *child = next->routine(i);
      p = child->EncodeNode(p);
    }
  }
  return p;
}

void PromiseRoutine::DecodeNode(go::TcpInputChannel *in)
{
  in->Read(this, util::Align(sizeof(PromiseRoutine), 8));

  capture_data = (uint8_t *) BasePromise::Alloc(util::Align(capture_len, 8));
  in->Read(capture_data, util::Align(capture_len, 8));

  next = nullptr;

  size_t nr_children = 0;
  in->Read(&nr_children, 8);

  if (nr_children > 0) {
    next = new BasePromise(nr_children);
    for (int i = 0; i < nr_children; i++) {
      PromiseRoutine *child = new BasePromise::ExecutionRoutine();
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
   if (child->next) child->next->AssignSchedulingKey(key);
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

  __builtin_prefetch(extra_handlers);

  for (size_t i = 0; i < nr_handlers; i++) {
    auto r = routine(i);
    r->input = in;
    transport.TransportPromiseRoutine(r);
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

void BasePromise::ExecutionRoutine::Run()
{
  {
    INIT_ROUTINE_BRK(4096);
    this->callback((PromiseRoutine *) this);
  }
  util::Impl<PromiseRoutineDispatchService>().Complete(
      scheduler()->thread_pool_id() - 1);
}

void BasePromise::ExecutionRoutine::AddToReadyQueue(go::Scheduler::Queue *q, bool next_ready)
{
  util::Impl<PromiseRoutineDispatchService>().Dispatch(
      scheduler()->thread_pool_id() - 1, this, q);
}

void BasePromise::ExecutionRoutine::OnRemoveFromReadyQueue()
{
  util::Impl<PromiseRoutineDispatchService>().Detach(
        scheduler()->thread_pool_id() - 1, this);
}

void BasePromise::QueueRoutine(felis::PromiseRoutine **routines, size_t nr_routines,
                               int source_idx, int thread, bool batch)
{
  go::Routine *grt[nr_routines];
  for (size_t i = 0; i < nr_routines; i++) {
    auto r = (ExecutionRoutine *) routines[i];
    grt[i] = r;
    if (i == nr_routines - 1)
      r->set_busy_poll(true);
  }
  go::GetSchedulerFromPool(thread)
      ->WakeUp(grt, nr_routines, batch);
}

void BasePromise::FlushScheduler()
{
  for (int i = 1; i <= g_nr_threads; i++) {
    go::GetSchedulerFromPool(i)->WakeUp();
  }
}

}
