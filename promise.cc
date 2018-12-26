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

PromiseRoutine *PromiseRoutine::CreateFromCapture(size_t capture_len)
{
  auto *p = (uint8_t *) BasePromise::Alloc(
      util::Align(sizeof(PromiseRoutine) + capture_len, CACHE_LINE_SIZE));
  auto *r = (PromiseRoutine *) p;
  r->capture_len = capture_len;
  r->capture_data = p + sizeof(PromiseRoutine);
  r->sched_key = 0;
  return r;
}

PromiseRoutine *PromiseRoutine::CreateFromPacket(uint8_t *p, size_t packet_len)
{
  uint16_t len;
  memcpy(&len, p, 2);
  uint8_t * input_ptr = p + 2;
  p += util::Align(2 + len, 8);

  auto r = (PromiseRoutine *) p;
  r->DecodeNode(p);
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
    auto *child = next->handlers[i];
    s += child->NodeSize();
  }

  return s;
}

uint8_t *PromiseRoutine::EncodeNode(uint8_t *p)
{
  memcpy(p, this, sizeof(PromiseRoutine));
  auto node = (PromiseRoutine *) p;
  p += util::Align(sizeof(PromiseRoutine), 8);
  node->capture_data = p;
  node->next = nullptr;

  memcpy(p, capture_data, capture_len);
  p += util::Align(capture_len, 8);

  size_t nr_children = next ? next->nr_handlers : 0;
  memcpy(p, &nr_children, 8);
  p += 8;

  if (next) {
    for (size_t i = 0; i < next->nr_handlers; i++) {
      auto *child = next->handlers[i];
      p = child->EncodeNode(p);
    }
  }
  return p;
}

uint8_t *PromiseRoutine::DecodeNode(uint8_t *p)
{
  p += util::Align(sizeof(PromiseRoutine), 8);
  capture_data = p;
  p += util::Align(capture_len, 8);
  next = nullptr;

  size_t nr_children = 0;
  memcpy(&nr_children, p, 8);
  p += 8;
  if (nr_children > 0) {
    next = new BasePromise(nr_children);
    for (int i = 0; i < nr_children; i++) {
      auto child = (PromiseRoutine *) p;
      p = child->DecodeNode(p);
      next->Add(child);
    }
  }
  return p;
}

PromiseProc::~PromiseProc()
{
}

size_t BasePromise::g_nr_threads = 0;

BasePromise::BasePromise(size_t limit)
    : limit(limit), nr_handlers(0)
{
  if (limit <= kInlineLimit)
    handlers = inline_handlers;
  else
    handlers = (PromiseRoutine **) Alloc(
        util::Align(sizeof(PromiseRoutine *) * limit, CACHE_LINE_SIZE));
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
  for (size_t i = 0; i < nr_handlers; i++) {
    auto *child = handlers[i];
    child->sched_key = key;
   if (child->next) child->next->AssignSchedulingKey(key);
  }
}

void BasePromise::Add(PromiseRoutine *child)
{
  abort_if(nr_handlers == kMaxHandlersLimit,
           "nr_handlers {} exceeding limits!", nr_handlers);
  handlers[nr_handlers++] = child;
}

void BasePromise::Complete(const VarStr &in)
{
  auto &transport = util::Impl<PromiseRoutineTransportService>();
  for (size_t i = 0; i < nr_handlers; i++) {
    auto routine = handlers[i];
    routine->input = in;
    transport.TransportPromiseRoutine(routine);
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

void *BasePromise::ExecutionRoutine::operator new(std::size_t size)
{
  return BasePromise::Alloc(size);
}

void BasePromise::ExecutionRoutine::Run()
{
  {
    INIT_ROUTINE_BRK(4096);
    r->callback(r);
  }
  auto cmp = EpochClient::g_workload_client->completion_object();
  cmp->Complete();
}

void BasePromise::QueueRoutine(felis::PromiseRoutine **routines, size_t nr_routines,
                               int source_idx, int thread, bool batch)
{
  go::Routine *grt[nr_routines];
  for (size_t i = 0; i < nr_routines; i++) {
    auto r = new ExecutionRoutine(routines[i]);
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
