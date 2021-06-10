#include "gc.h"
#include "util/linklist.h"
#include "util/locks.h"
#include "log.h"
#include "vhandle.h"
#include "index_info.h"
#include "index.h"
#include "node_config.h"
#include "epoch.h"

#include "literals.h"

namespace felis {

struct GarbageBlock : public util::GenericListNode<GarbageBlock> {
  static constexpr size_t kBlockSize = 512;
  static constexpr int kMaxNrRows = kBlockSize / 8 - 4;
  std::array<IndexInfo *, kMaxNrRows> rows;
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
  static constexpr size_t kPreallocPerCore = 64_K;
  static constexpr size_t kNrQueue = 200;
  util::MCSSpinLock lock;
  int core_id;
  util::GenericListNode<GarbageBlock> free;
  util::GenericListNode<GarbageBlock> half[kNrQueue];
  util::GenericListNode<GarbageBlock> full[kNrQueue];

  GarbageBlockSlab(int core_id);

  uint64_t Add(IndexInfo *row, int q_idx);
  void Remove(GarbageBlock *blk, int idx);
};

GarbageBlockSlab::GarbageBlockSlab(int core_id)
    : core_id(core_id)
{
  auto blks = (GarbageBlock *) mem::AllocMemory(
      mem::IndexInfoPool, GarbageBlock::kBlockSize * kPreallocPerCore, core_id / mem::kNrCorePerNode);

  for (size_t i = 0; i < kNrQueue; i++) {
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

uint64_t GarbageBlockSlab::Add(IndexInfo *row, int q_idx) {
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
  abort_if(blk == nullptr, "WHY?");
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

uint64_t GC::AddRow(IndexInfo *row, uint64_t epoch_nr) {
  abort_if(epoch_nr == 0, "Should not even detect garbage during loader");
  int q_idx = epoch_nr % g_gc_every_epoch;
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  return g_slabs[core_id]->Add(row, q_idx);
}

void GC::RemoveRow(IndexInfo *row, uint64_t gc_handle) {
  uint64_t base = GarbageBlock::kBlockSize * (gc_handle / GarbageBlock::kBlockSize);
  int idx = (gc_handle - base - sizeof(util::GenericListNode<GarbageBlock>)) / 8;
  auto blk = (GarbageBlock *) base;
  return g_slabs[blk->alloc_core]->Remove(blk, idx);
}

unsigned int GC::g_gc_every_epoch = 0;
bool GC::g_lazy = false;
std::array<GarbageBlockSlab *, NodeConfiguration::kMaxNrThreads> GC::g_slabs;

void GC::InitPool()
{
  abort_if(g_gc_every_epoch >= GarbageBlockSlab::kNrQueue,
           "g_gc_every_epoch {} >= {}", g_gc_every_epoch, GarbageBlockSlab::kNrQueue);

  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    g_slabs[i] = new GarbageBlockSlab(i);
  }
}

void GC::PrepareGCForAllCores()
{
  if (g_lazy)
    return;

  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();

  for (auto core_id = 0; core_id < NodeConfiguration::g_nr_threads; core_id++) {
    auto &slab = g_slabs[core_id];
    int q_idx = (cur_epoch_nr + 1) % g_gc_every_epoch;
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

    // This function runs during epoch boundary, we don't need atomic access to
    // the head.
    //
    // do {
    //   tail_node->next = tail_next;
    // } while (!collect_head.compare_exchange_strong(tail_next, new_head));

    tail_node->next = tail_next;
    collect_head = new_head;

    full_queue->Initialize();
    half_queue->Initialize();
  }
}

void GC::RunGC()
{
  // TODO: add memory pressure detection.
  if (g_lazy)
    return;

  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  int q_idx = (cur_epoch_nr + 1) % g_gc_every_epoch;

  auto &s = stats[go::Scheduler::CurrentThreadPoolId() - 1];

  GarbageBlock *b = collect_head.load();
  while (true) {
    // logger->info("GC block {}", (void *) b);
    while (!b || !collect_head.compare_exchange_strong(b, b->next->object())) {
      if (!b) {
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
      // abort_if((uint64_t) &b->rows[i] != b->rows[i]->gc_handle.load(),
      //          "gc_handle {:x} i {} blk {}", b->rows[i]->gc_handle.load(), i, (void *) b);

      auto old = s.nr_bytes;
      auto nr_processed = Process(b->rows[i], cur_epoch_nr, 16_K);
      
      if (nr_processed < 16_K) {
        // b->rows[i]->gc_handle = 0;
        b->bitmap &= ~(1ULL << i);
        s.nr_rows++;
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

        s.nr_rows++;
        s.straggler = true;
        return;
      }
    }
    // Mark this block free
    // slab->lock.Acquire(&qnode);
    b->InsertAfter(&g_slabs[b->alloc_core]->free);
    // slab->lock.Release(&qnode);

    s.nr_blocks++;
    b = b->next->object();
  }
}

void GC::RunPmemGC()
{
  // TODO: add memory pressure detection.
  if (g_lazy)
    return;

  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  int q_idx = (cur_epoch_nr + 1) % g_gc_every_epoch;

  auto &s = stats[go::Scheduler::CurrentThreadPoolId() - 1];

  GarbageBlock *b = collect_head.load();
  while (true) {
    // logger->info("GC block {}", (void *) b);
    while (!b || !collect_head.compare_exchange_strong(b, b->next->object())) {
      if (!b) {
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
      // abort_if((uint64_t) &b->rows[i] != b->rows[i]->gc_handle.load(),
      //          "gc_handle {:x} i {} blk {}", b->rows[i]->gc_handle.load(), i,
      //          (void *) b);

      auto old = s.nr_bytes;
      auto nr_processed = ProcessPmem(b->rows[i], cur_epoch_nr, 16_K);
      // shirley: after we remove the hard limit, don't need this check anymore.
      // shirley: don't require limit on gc. clean everything
      // if (nr_processed < 16_K) {
        // b->rows[i]->gc_handle = 0;
        b->bitmap &= ~(1ULL << i);
        s.nr_rows++;
        continue;
      // }

      // shirley: dont care if we're straggler. clean everything
    }
    // Mark this block free
    // slab->lock.Acquire(&qnode);
    b->InsertAfter(&g_slabs[b->alloc_core]->free);
    // slab->lock.Release(&qnode);

    s.nr_blocks++;
    b = b->next->object();
  }
}

size_t GC::Process(IndexInfo *handle, uint64_t cur_epoch_nr, size_t limit) {
  util::MCSSpinLock::QNode qnode;
  handle->lock.Lock(&qnode);
  size_t n = Collect(handle, cur_epoch_nr, limit);
  handle->lock.Unlock(&qnode);
  return n;
}

size_t GC::ProcessPmem(IndexInfo *handle, uint64_t cur_epoch_nr, size_t limit) {
  util::MCSSpinLock::QNode qnode;
  handle->lock.Lock(&qnode);
  size_t n = CollectPmem(handle, cur_epoch_nr, limit);
  handle->lock.Unlock(&qnode);
  return n;
}

bool GC::FreeIfGarbage(IndexInfo *row, VarStr *p, VarStr *next)
{
  auto &s = stats[go::Scheduler::CurrentThreadPoolId() - 1];
  bool deleted = false;

  if (IsDataGarbage(row, p)) {
    s.nr_bytes += p->length();
    deleted = true;
    delete p;
  }

  return deleted;
}

size_t GC::Collect(IndexInfo *handle, uint64_t cur_epoch_nr, size_t limit) {
  printf("shouldn't reach old implementation of GC::Collect\n");
  std::abort();
  auto *versions = handle->versions;
  uintptr_t *objects = handle->versions + handle->capacity_get(handle->versions);
  int i = 0;
  while (i < handle->size_get(handle->versions) - 1 && i < limit && (versions[i + 1] >> 32) < cur_epoch_nr) {
    i++;
  }
  if (i == 0) return 0;

  if (is_trace_enabled(TRACE_GC)) {
    trace(TRACE_GC "BeforeGC on row {} {}, i {}", (void *) handle, handle->ToString(), i);
  }

  for (auto j = 0; j < i; j++) {
    auto p = (VarStr *) objects[j];
    auto next = (VarStr *) objects[j + 1];
    FreeIfGarbage(handle, p, next);
  }

  std::move(objects + i, objects + handle->size_get(handle->versions), objects);
  std::move(versions + i, versions + handle->size_get(handle->versions), versions);
  handle->size_set(handle->versions, handle->size_get(versions) - i);
  //handle->cur_start -= i;
  handle->latest_version_ptr(handle->versions)->fetch_sub(i);

  if (is_trace_enabled(TRACE_GC)) {
    trace(TRACE_GC "GC on row {} {}", (void *) handle, handle->ToString());
  }
  return i;
}

size_t GC::CollectPmem(IndexInfo *handle, uint64_t cur_epoch_nr, size_t limit) {
  //auto *versions = handle->versions;
  //uintptr_t *objects = handle->versions + handle->capacity;
  //int i = handle->size; //shirley: don't need this return value. just return 1.

  //printf("RunPmemGC: cleaning vhandle %p\n", handle);
  handle->versions = nullptr;
  //handle->size = 0; //shirley: removed bc size is stored in versions
  //handle->cur_start = 0;
  //handle->latest_version = -1; //shirley: removed bc latest version is stored in versions

  VHandle *vhandle = handle->vhandle;

  // shirley: free ptr1 if it's not from inline
  vhandle->FreePtr1();
  // auto ptr1 = handle->GetInlinePtr(felis::SortedArrayVHandle::SidType1);
  // if (ptr1 < (unsigned char *)handle ||
  //     ptr1 >= (unsigned char *)handle + 256) {
  //       delete ((VarStr *)ptr1);
  // }
  // shirley: copy sid2, ptr2 to sid1, ptr1
  vhandle->Copy2To1();
  // //get sid2 and ptr2
  // auto sid2 = handle->GetInlineSid(felis::SortedArrayVHandle::SidType2); 
  // auto ptr2 = handle->GetInlinePtr(felis::SortedArrayVHandle::SidType2);
  // //set sid1 and ptr1
  // handle->SetInlineSid(felis::SortedArrayVHandle::SidType1, sid2);
  // handle->SetInlinePtr(felis::SortedArrayVHandle::SidType1, ptr2);
  // shirley: reset sid2, ptr2 (not needed in ECE1724 design)
  vhandle->ResetSid2();

  //shirley pmem: flush cache after GC
  // _mm_clwb((char *)vhandle);
  // _mm_clwb(((char *)vhandle) + 64);
  // _mm_clwb(((char *)vhandle) + 128);
  // _mm_clwb(((char *)vhandle) + 192);

  return 1;
}

// shirley: this function is not used for new design
bool GC::IsDataGarbage(IndexInfo *row, VarStr *data) {
  if (data == nullptr) return false;
  auto p = (uint8_t *) data;
  // shirley: this doesn't make sense in new design. ignore.
  if (p > (uint8_t *) row && p < (uint8_t *) row + 256) {
    // shirley: removed. always inlined
    // abort_if(!row->is_inlined(), "??? row {} p {}", (void *) row, (void *) p);
    return false;
  }
  return true;
}

void GC::PrintStats()
{
  fmt::memory_buffer buf;
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto &s = stats[i];
    fmt::format_to(buf, " {}({}{})={}K",
                   s.nr_rows, s.nr_blocks, s.straggler ? "*" : "",
                   s.nr_bytes >> 10);
  }
  logger->info("GC: {}", std::string_view(buf.data(), buf.size()));
}

}

namespace util {

using namespace felis;

static GC g_gc;

GC *InstanceInit<GC>::instance = &g_gc;

}
