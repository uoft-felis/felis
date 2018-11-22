#include "promise.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"
#include <queue>
#include "util.h"

using util::Instance;
using util::Impl;

namespace felis {

PromiseRoutinePool *PromiseRoutinePool::Create(size_t size)
{
  PromiseRoutinePool *p = (PromiseRoutinePool *) malloc(sizeof(PromiseRoutinePool) + size);
  p->mem_size = size;
  p->refcnt = 0;
  p->input_ptr = nullptr;
  return p;
}

void PromiseRoutine::UnRef()
{
  delete next;
  next = nullptr;

  if (!pool->IsManaging(input.data))
    free((void *) input.data);

  if (pool->refcnt.fetch_sub(1) == 1) {
    free(pool); // which also free this
  }
}

void PromiseRoutine::UnRefRecursively()
{
  if (next) {
    for (auto child: next->handlers) {
      child->UnRefRecursively();
    }
  }
  UnRef();
}

PromiseRoutine *PromiseRoutine::CreateWithDedicatePool(size_t capture_len)
{
  auto *p = PromiseRoutinePool::Create(sizeof(PromiseRoutine) + capture_len);
  auto *r = (PromiseRoutine *) p->mem;
  r->capture_len = capture_len;
  r->capture_data = p->mem + sizeof(PromiseRoutine);
  r->pool = p;
  r->Ref();
  return r;
}

PromiseRoutine *PromiseRoutine::CreateFromBufferedPool(PromiseRoutinePool *rpool)
{
  uint8_t *p = rpool->mem;
  uint16_t len;
  memcpy(&len, p, 2);
  rpool->input_ptr = p + 2;
  p += util::Align(2 + len, 8);

  auto r = (PromiseRoutine *) p;
  r->DecodeTree(rpool);
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

void PromiseRoutine::DecodeTree(PromiseRoutinePool *rpool)
{
  // input = VarStr(input.len, input.region_id, rpool->input_ptr);
  input.data = rpool->input_ptr;
  DecodeNode((uint8_t *) this, rpool);
}

size_t PromiseRoutine::NodeSize() const
{
  size_t s = util::Align(sizeof(PromiseRoutine), 8)
             + util::Align(capture_len, 8)
             + 8;
  if (next) {
    for (auto child: next->handlers) {
      s += child->NodeSize();
    }
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
  node->pool = nullptr;

  memcpy(p, capture_data, capture_len);
  p += util::Align(capture_len, 8);

  size_t nr_children = next ? next->handlers.size() : 0;
  memcpy(p, &nr_children, 8);
  p += 8;

  if (next) {
    for (auto child: next->handlers) {
      p = child->EncodeNode(p);
    }
  }
  return p;
}

uint8_t *PromiseRoutine::DecodeNode(uint8_t *p, PromiseRoutinePool *rpool)
{
  p += util::Align(sizeof(PromiseRoutine), 8);
  capture_data = p;
  p += util::Align(capture_len, 8);
  next = nullptr;
  pool = rpool;
  Ref();
  size_t nr_children = 0;
  memcpy(&nr_children, p, 8);
  p += 8;
  if (nr_children > 0) {
    next = new BasePromise();
    for (int i = 0; i < nr_children; i++) {
      auto child = (PromiseRoutine *) p;
      p = child->DecodeNode(p, rpool);
      next->handlers.push_back(child);
    }
  }
  return p;
}

size_t BasePromise::g_nr_threads = 0;
bool BasePromise::g_enable_batching = false;

static size_t g_cur_thread = 1;

void BasePromise::Complete(const VarStr &in)
{
  auto &transport = util::Impl<PromiseRoutineTransportService>();
  for (auto routine: handlers) {
    routine->input = in;

    if (routine->node_id == -1) {
      routine->node_id = routine->placement(routine);
    }
    if (routine->node_id < 0) std::abort();

    transport.TransportPromiseRoutine(routine);
  }
}

struct PromiseSourceDesc {
  std::atomic_long finish_count;
  go::BufferChannel *wait_channel;

  PromiseSourceDesc(long cnt, go::BufferChannel *wait_channel)
      : finish_count(cnt), wait_channel(wait_channel) {
  }

  PromiseSourceDesc(const PromiseSourceDesc &rhs) {
    // Not thread safe
    wait_channel = rhs.wait_channel;
    finish_count.store(rhs.finish_count.load());
  }

  ~PromiseSourceDesc() {}

  void WaitLocalBarrier();
  static void BroadcastLocalBarrier();
};

static std::vector<PromiseSourceDesc> g_sources;
static std::unique_ptr<util::CacheAligned<std::atomic_ulong>[]> batch_counts;

void PromiseSourceDesc::WaitLocalBarrier()
{
  uint8_t done[g_sources.size()];
  wait_channel->Read(done, g_sources.size());
}

void PromiseSourceDesc::BroadcastLocalBarrier()
{
  for (auto &desc: g_sources) {
    uint8_t done = 0;
    desc.wait_channel->Write(&done, 1);
  }
}

void BasePromise::InitializeSourceCount(int nr_sources, size_t nr_threads)
{
  for (int i = 0; i < nr_sources; i++) {
    g_sources.emplace_back(1, new go::BufferChannel(nr_sources));
  }
  g_nr_threads = nr_threads;
  batch_counts.reset(new util::CacheAligned<std::atomic_ulong>[g_nr_threads + 1]);
  for (int i = 0; i < g_nr_threads; i++) {
    batch_counts[i].elem = 0;
  }
}

void BasePromise::QueueRoutine(felis::PromiseRoutine *r, int source_idx, int thread)
{
  auto &desc = g_sources[source_idx];
  /*
  if (r == nullptr) {
    if (desc.finish_count.fetch_sub(1) == 1) {
      desc.BroadcastLocalBarrier();
    }
    desc.WaitLocalBarrier();
    desc.finish_count.fetch_add(1);
    return;
  }
  */
  // desc.finish_count.fetch_add(1);
  bool would_batch = g_enable_batching;
  if (g_enable_batching
      && batch_counts[thread]->fetch_add(1) % 65536 == 65535) {
    would_batch = false;
  }
  go::GetSchedulerFromPool(thread)->WakeUp(
      go::Make(
          [r, &desc]() {
            {
              INIT_ROUTINE_BRK(4096);
              r->callback(r);
            }
            /*
            if (desc.finish_count.fetch_sub(1) == 1) {
              desc.BroadcastLocalBarrier();
            }
            */
          }),
      would_batch);
}

}
