#include "gc.h"
#include "util.h"
#include "log.h"
#include "vhandle.h"
#include "index.h"
#include "node_config.h"
#include "epoch.h"

#include "literals.h"

namespace felis {

struct GarbageBlock : public util::GenericListNode<GarbageBlock> {
  static constexpr size_t kBlockSize = 512;
  static constexpr int kMaxNrRows = kBlockSize / 8 - 4;
  std::array<VHandle *, kMaxNrRows> rows;
  int alloc_core;
  int q_idx;
  uint64_t bitmap;

  void Prefetch() {
    for (int i = 0; i < kMaxNrRows; i++) {
      __builtin_prefetch(rows[i]);
    }
  }
};

static_assert(sizeof(GarbageBlock) == GarbageBlock::kBlockSize, "Block doesn't match block size?");

struct GarbageBlockSlab {
  static constexpr auto kPreallocPerCore = 8192U;
  util::MCSSpinLock lock;
  int core_id;
  util::GenericListNode<GarbageBlock> free;
  util::GenericListNode<GarbageBlock> half[128];
  util::GenericListNode<GarbageBlock> full[128];

  GarbageBlockSlab(int core_id);

  uint64_t Add(VHandle *row, int q_idx);
  void Remove(GarbageBlock *blk, int idx);
};

GarbageBlockSlab::GarbageBlockSlab(int core_id)
    : core_id(core_id)
{
  auto blks = (GarbageBlock *) mem::MemMapAlloc(
      mem::VhandlePool, GarbageBlock::kBlockSize * kPreallocPerCore, core_id / mem::kNrCorePerNode);

  for (size_t i = 0; i < 128; i++) {
    half[i].Initialize();
    full[i].Initialize();
  }
  free.Initialize();
  for (int i = kPreallocPerCore - 1; i >= 0; i--) {
    auto blk = &blks[i];
    blk->Initialize();
    blk->InsertAfter(&free);
  }
}

uint64_t GarbageBlockSlab::Add(VHandle *row, int q_idx)
{
  auto half_queue = &half[q_idx];
  auto full_queue = &full[q_idx];
  int idx = 0;
  GarbageBlock *blk = nullptr;
  util::MCSSpinLock::QNode qnode;
  lock.Acquire(&qnode);
  if (!half_queue->empty()) {
    blk = half_queue->next->object();
    idx = __builtin_ffsll(~blk->bitmap) - 1;
    abort_if(idx < 0 || idx >= GarbageBlock::kBlockSize, "inconsistent garbage block slab!");
    blk->bitmap |= 1ULL << idx;
    if (__builtin_popcountll(blk->bitmap) == GarbageBlock::kMaxNrRows - 1) {
      blk->Remove();
      blk->InsertAfter(full_queue);
    }
  } else {
    abort_if(free.empty(), "no more blocks!");
    blk = free.next->object();
    blk->Remove();
    blk->InsertAfter(half_queue);
    blk->alloc_core = core_id;
    blk->q_idx = q_idx;
    blk->bitmap = 1;
    idx = 0;
  }
  blk->rows[idx] = row;
  lock.Release(&qnode);

  return (uint64_t) &blk->rows[idx];
}

void GarbageBlockSlab::Remove(GarbageBlock *blk, int idx)
{
  auto half_queue = &half[blk->q_idx];
  util::MCSSpinLock::QNode qnode;

  lock.Acquire(&qnode);
  if (__builtin_popcountll(blk->bitmap) == GarbageBlock::kMaxNrRows) {
    blk->Remove();
    blk->InsertAfter(half_queue);
  } else if (__builtin_popcountll(blk->bitmap) == 1) {
    blk->Remove();
    blk->InsertAfter(&free);
  }

  blk->rows[idx] = nullptr;
  blk->bitmap &= ~(1ULL << idx);
  lock.Release(&qnode);
}

uint64_t GC::AddRow(VHandle *row, uint64_t epoch_nr)
{
  abort_if(epoch_nr == 0, "Should not even detect garbage during loader");
  int q_idx = epoch_nr % g_gc_every_epoch;
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  return g_slabs[core_id]->Add(row, q_idx);
}

void GC::RemoveRow(VHandle *row, uint64_t gc_handle)
{
  uint64_t base = GarbageBlock::kBlockSize * (gc_handle / GarbageBlock::kBlockSize);
  int idx = (gc_handle - base - sizeof(util::GenericListNode<GarbageBlock>)) / 8;
  auto blk = (GarbageBlock *) base;
  return g_slabs[blk->alloc_core]->Remove(blk, idx);
}

unsigned int GC::g_gc_every_epoch = 0;
std::array<GarbageBlockSlab *, NodeConfiguration::kMaxNrThreads> GC::g_slabs;

void GC::InitPool()
{
  g_gc_every_epoch = 2000000 / EpochClient::g_txn_per_epoch;
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    g_slabs[i] = new GarbageBlockSlab(i);
  }
}

void GC::PrepareGCForAllCores()
{
  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();

  for (auto core_id = 0; core_id < NodeConfiguration::g_nr_threads; core_id++) {
    auto &slab = g_slabs[core_id];
    int q_idx = cur_epoch_nr % g_gc_every_epoch;
    auto *full_queue = &slab->full[q_idx];
    auto *half_queue = &slab->half[q_idx];

    GarbageBlock *new_head = nullptr;
    util::GenericListNode<GarbageBlock> *tail_node = nullptr;

    if (!full_queue->empty() && !half_queue->empty()) {
      new_head = full_queue->next->object();
      full_queue->prev->next = half_queue->next;
      tail_node = half_queue->prev;
    } else if (full_queue->empty() && !half_queue->empty()) {
      new_head = half_queue->next->object();
      tail_node = half_queue->prev;
    } else if (!full_queue->empty() && half_queue->empty()) {
      new_head = full_queue->next->object();
      tail_node = full_queue->prev;
    } else {
      return;
    }

    GarbageBlock *tail_next = collect_head;
    // do {
    // tail_node->next = tail_next;
    // } while (!collect_head.compare_exchange_strong(tail_next, new_head));
    tail_node->next = tail_next;
    collect_head = new_head;

    full_queue->Initialize();
    half_queue->Initialize();
  }
}

void GC::RunGC()
{
  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  int q_idx = cur_epoch_nr % g_gc_every_epoch;

  size_t nr = 0, nrb = 0, nr_bytes = 0;

  GarbageBlock *b = collect_head.load();
  while (true) {
    // logger->info("GC block {}", (void *) b);
    while (!b || !collect_head.compare_exchange_strong(b, b->next->object())) {
      if (!b) {
        logger->info("GC done {} rows {} blks freed {}K", nr, nrb,
                     nr_bytes >> 10);
        return;
      }
    }

    size_t i = 0;
    // After processing this block, we always need to put it back into the slab!
    // util::MCSSpinLock::QNode qnode;
    auto slab = g_slabs[b->alloc_core];
    b->Initialize();

    while (b->bitmap != 0) {
      i = __builtin_ffsll(b->bitmap) - 1;
      // logger->info("Found {} bitmap {:x}", i, b->bitmap);
      auto nr_processed = Process(b->rows[i], cur_epoch_nr, 16_K, &nr_bytes);
      if (nr_processed < 16_K) {
        b->rows[i]->gc_handle = 0;
        b->bitmap &= ~(1ULL << i);
        nr++;
        continue;
      }

      // Too much work for this block, am I the straggler?
      if (collect_head == nullptr) {
        // Add this block back into the slab.
        // slab->lock.Acquire(&qnode);
        if (__builtin_popcountll(b->bitmap) == GarbageBlock::kMaxNrRows) {
          b->InsertAfter(&slab->full[q_idx]);
        } else {
          b->InsertAfter(&slab->half[q_idx]);
        }
        // slab->lock.Release(&qnode);

        logger->info("GC unfinished {} rows {} blks, freed {}K", nr + 1, nrb,
                     nr_bytes >> 10);
        return;
      }
    }
    // Mark this block free
    // slab->lock.Acquire(&qnode);
    b->InsertAfter(&g_slabs[b->alloc_core]->free);
    // slab->lock.Release(&qnode);

    nrb++;
    b = b->next->object();
  }
}

size_t GC::Process(VHandle *handle, uint64_t cur_epoch_nr, size_t limit, size_t *nr_bytes)
{
  util::MCSSpinLock::QNode qnode;
  handle->lock.Lock(&qnode);
  size_t n = Collect(handle, cur_epoch_nr, limit, nr_bytes);
  handle->lock.Unlock(&qnode);
  return n;
}

size_t GC::Collect(VHandle *handle, uint64_t cur_epoch_nr, size_t limit, size_t *nr_bytes)
{
  auto *versions = handle->versions;
  uintptr_t *objects = handle->versions + handle->capacity;
  int i = 0;
  while (i < handle->size - 1 && i <= limit && (versions[i + 1] >> 32) < cur_epoch_nr) {
    i++;
  }
  if (i == 0) return 0;

  if (is_trace_enabled(TRACE_GC)) {
    fmt::memory_buffer buf;
    for (auto j = 0; j < handle->size; j++) {
      fmt::format_to(buf, "{}->0x{:x} ", versions[j], objects[j]);
    }
    trace(TRACE_GC "BeforeGC on row {} {}, i {}", (void *) handle,
          std::string_view(buf.begin(), buf.size()), i);
  }

  for (auto j = 0; j < i; j++) {
    auto p = (VarStr *) objects[j];
    if (IsDataGarbage(handle, p)) {
      *nr_bytes += p->len;
      delete p;
    }
  }
  std::move(objects + i, objects + handle->size, objects);
  std::move(versions + i, versions + handle->size, versions);
  handle->size -= i;
  handle->cur_start -= i;
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

bool GC::IsDataGarbage(VHandle *row, VarStr *data)
{
  if (data == nullptr) return false;
  auto p = (uint8_t *) data;
  if (p > (uint8_t *) row && p < (uint8_t *) row + 256) {
    abort_if(!row->is_inlined(), "??? row {} p {}", (void *) row, (void *) p);
    return false;
  }
  return true;
}

}

namespace util {

using namespace felis;

static GC g_gc;

GC *InstanceInit<GC>::instance = &g_gc;

}
