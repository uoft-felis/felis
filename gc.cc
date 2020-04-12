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
unsigned int GC::g_gc_every_epoch = 0;

void GC::InitPool()
{
  g_block_pool = mem::ParallelPool(
      mem::VhandlePool, GarbageBlock::kBlockSize, 128_M / GarbageBlock::kBlockSize);
  g_gc_every_epoch = 800000 / EpochClient::g_txn_per_epoch;
}

GC::LocalCollector &GC::local_collector()
{
  return local_cls[go::Scheduler::CurrentThreadPoolId() - 1];
}

void GC::AddVHandle(VHandle *handle, uint64_t epoch_nr)
{
  if (epoch_nr == 0)
    return;

  // Because our GC is sloppy, in the extreme case, the row can run out of
  // versions if it's too long. We need to force a garbage collection in this
  // case.
  if (handle->nr_versions() >= 1_M - 64) {
    // We limit the collect to 16K because we are still holding a lock!
    if (Collect(handle, epoch_nr, 16_K) < 16_K)
      return;
  }

  // Either there's nothing to collect, or it's already in the GC queue.
  if (handle->last_gc_mark_epoch != 0 &&
      (handle->last_gc_mark_epoch / g_gc_every_epoch) == (epoch_nr / g_gc_every_epoch))
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
  if (util::Instance<EpochManager>().current_epoch_nr() % g_gc_every_epoch != 0)
    return;

  auto &cls = local_collector();
  GarbageBlock *tail = nullptr;

  // Let's put the left over at the head of the processing queue
  if ((tail = cls.left_over) != nullptr) {
    while (tail->next) tail = tail->next;
    tail->processing_next = tail->next = cls.pending;
    cls.pending = cls.left_over;
  }

  if (cls.pending) {
    if (tail == nullptr) tail = cls.pending;
    while (tail->next) tail = tail->next;
    tail->processing_next = tail->next = cls.processing;
    cls.processing = cls.pending;
  } else {
    return;
  }

  logger->info("GC processing {} pending {} left_over {}", (void *) cls.processing, (void *) cls.pending, (void *) cls.left_over);
  tail->processing_next = processing_queue.load();
  while (!processing_queue.compare_exchange_strong(tail->processing_next, cls.processing))
    _mm_pause();

  cls.left_over = nullptr;
  cls.pending = nullptr;
}

void GC::FinalizeGC()
{
  if (util::Instance<EpochManager>().current_epoch_nr() % g_gc_every_epoch != 0)
    return;

  auto &cls = local_collector();
  GarbageBlock *next = nullptr;
  for (auto b = cls.processing; b != nullptr; b = next) {
    next = b->next;
    if (b->nr_handles == 0) {
      delete b;
    } else {
      b->processing_next = b->next = cls.left_over;
      cls.left_over = b;
    }
  }
  cls.processing = nullptr;
}

void GC::RunGC()
{
  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  if (cur_epoch_nr % g_gc_every_epoch != 0)
    return;

  size_t nr = 0, nrb = 0;
  nr_gc_collecting.fetch_add(1);
  GarbageBlock *b = processing_queue.load();
  while (true) {
    while (!b || !processing_queue.compare_exchange_strong(b, b->processing_next)) {
      if (!b) {
        nr_gc_collecting.fetch_sub(1);
        logger->info("GC done {} rows {} blks", nr, nrb);
        return;
      }

      _mm_pause();
    }

    size_t last = 0;
    size_t i = 0;
    while (i < b->nr_handles) {
      auto nr_processed = Process(b->handles[i], cur_epoch_nr, 16_K);
      if (nr_processed < 16_K) {
        i++;
        continue;
      }

      last += nr_processed;
      // Too much work for this block, am I the straggler?
      if (last < 16_K) continue;
      if (processing_queue.load() == nullptr) {
        // Fuck you are the straggler
        logger->info("GC unfinished {} rows {} blks, last {}", nr + i, nrb, last);
        std::move(b->handles.begin() + i, b->handles.begin() + b->nr_handles, b->handles.begin());
        b->nr_handles -= i;

        nr_gc_collecting.fetch_sub(1);
        return;
      }
      last = 0;
    }

    nrb++;
    nr += b->nr_handles;
    b->nr_handles = 0;
    b = b->processing_next;
  }
}

size_t GC::Process(VHandle *handle, uint64_t cur_epoch_nr, size_t limit)
{
  util::MCSSpinLock::QNode qnode;
  handle->lock.Lock(&qnode);
  size_t n = Collect(handle, cur_epoch_nr, limit);
  handle->lock.Unlock(&qnode);
  return n;
}

size_t GC::Collect(VHandle *handle, uint64_t cur_epoch_nr, size_t limit)
{
  auto *versions = handle->versions;
  uintptr_t *objects = handle->versions + handle->capacity;
  int i = 0;
  while (i < handle->size - 1 && i <= limit && (versions[i + 1] >> 32) < cur_epoch_nr) {
    i++;
  }
  while (i > 0 && objects[i] == kIgnoreValue) {
    i--; // if last version is ignore, then we gc less versions
  }
  if (i == 0) return 0;

  for (auto j = 0; j < i; j++) {
    if (objects[j] == kIgnoreValue) continue;
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
  return i;
}

}

namespace util {

using namespace felis;

static GC g_gc;

GC *InstanceInit<GC>::instance = &g_gc;

}
