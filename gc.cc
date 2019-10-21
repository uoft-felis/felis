#include "gc.h"
#include "util.h"
#include "log.h"
#include "vhandle.h"
#include "index.h"
#include "node_config.h"
#include "epoch.h"

#include "literals.h"

namespace felis {

mem::ParallelPool GC::g_block_pool;

void GC::InitPool()
{
  g_block_pool = mem::ParallelPool(
      mem::VhandlePool, GarbageBlock::kBlockSize, 128_M / GarbageBlock::kBlockSize);
}

GC::LocalCollector &GC::local_collector()
{
  return local_cls[go::Scheduler::CurrentThreadPoolId() - 1];
}

static const int kGCEveryEpoch = 8;

void GC::AddVHandle(VHandle *handle, uint64_t epoch_nr)
{
  if (epoch_nr == 0)
    return;

  // Either there's nothing to collect, or it's already in the GC queue.
  if (handle->last_gc_mark_epoch != 0 &&
      (handle->last_gc_mark_epoch / kGCEveryEpoch) == (epoch_nr / kGCEveryEpoch))
    return;
  handle->last_gc_mark_epoch = epoch_nr;

  auto &cls = local_collector();
  if (cls.pending == nullptr || cls.pending->nr_handles == GarbageBlock::kMaxNrBlocks) {
    auto b = new GarbageBlock();
    b->processing_next = b->next = cls.pending;
    cls.pending = b;
  }
  cls.pending->handles[cls.pending->nr_handles++] = handle;
}

void GC::PrepareGC()
{
  if (util::Instance<EpochManager>().current_epoch_nr() % kGCEveryEpoch != 0)
    return;

  auto &cls = local_collector();
  GarbageBlock *tail = cls.pending;
  if (!tail) return;
  while (tail->next) tail = tail->next;

  tail->next = cls.processing;
  cls.processing = cls.pending;

  tail->processing_next = processing_queue.load();
  while (!processing_queue.compare_exchange_strong(tail->processing_next, cls.pending))
    _mm_pause();

  cls.pending = nullptr;
}

void GC::FinalizeGC()
{
  if (util::Instance<EpochManager>().current_epoch_nr() % kGCEveryEpoch != 0)
    return;

  auto &cls = local_collector();
  GarbageBlock *next = nullptr;
  for (auto b = cls.processing; b != nullptr; b = next) {
    next = b->next;
    delete b;
  }
  cls.processing = nullptr;
}

void GC::RunGC()
{
  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  if (cur_epoch_nr % kGCEveryEpoch != 0)
    return;

  GarbageBlock *b = processing_queue.load();
  size_t nr = 0, nrb = 0;
  while (true) {
    while (!b || !processing_queue.compare_exchange_strong(b, b->processing_next)) {
      if (!b) {
        // logger->info("GC done {} rows {} blks", nr, nrb);
        return;
      }

      _mm_pause();
    }

    for (auto i = 0; i < b->nr_handles; i++) Process(b->handles[i], cur_epoch_nr);
    nrb++;
    nr += b->nr_handles;
    b = b->processing_next;
  }
}

void GC::Process(VHandle *handle, uint64_t cur_epoch_nr)
{
  util::MCSSpinLock::QNode qnode;
  handle->lock.Lock(&qnode);
  Collect(handle, cur_epoch_nr);
  handle->lock.Unlock(&qnode);
}

void GC::Collect(VHandle *handle, uint64_t cur_epoch_nr)
{
  auto *versions = handle->versions;
  uintptr_t *objects = handle->versions + handle->capacity;
  int i = 0;
  while (i < handle->size - 1 && (versions[i + 1] >> 32) < cur_epoch_nr) {
    i++;
  }
  for (auto j = 0; j < i; j++) {
    delete (VarStr *) objects[j];
  }
  std::move(objects + i, objects + handle->size, objects);
  std::move(versions + i, versions + handle->size, versions);
  handle->size -= i;
  handle->latest_version.fetch_sub(i);

  if (is_trace_enabled(TRACE_GC)) {
    fmt::memory_buffer buf;
    for (auto j = 0; j < handle->size; j++) {
      fmt::format_to(buf, "{}->0x{:x} ", versions[j], objects[j]);
    }
    trace(TRACE_GC "GC on row {} {}", (void *) handle,
          std::string_view(buf.begin(), buf.size()));
  }
}

}

namespace util {

using namespace felis;

static GC g_gc;

GC *InstanceInit<GC>::instance = &g_gc;

}
