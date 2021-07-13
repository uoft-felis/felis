#include "gc_dram.h"
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

struct GarbageBlockDram : public util::GenericListNode<GarbageBlockDram> {
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

static_assert(sizeof(GarbageBlockDram) == GarbageBlockDram::kBlockSize, "Block doesn't match block size?");

struct GarbageBlockSlabDram {
  static constexpr size_t kPreallocPerCore = 192_K;
  static constexpr size_t kNrQueue = 200;
  util::MCSSpinLock lock;
  int core_id;
  util::GenericListNode<GarbageBlockDram> free;
  util::GenericListNode<GarbageBlockDram> half[kNrQueue];
  util::GenericListNode<GarbageBlockDram> full[kNrQueue];

  GarbageBlockSlabDram(int core_id);

  uint64_t Add(IndexInfo *row, int q_idx);
  void Remove(GarbageBlockDram *blk, int idx);
};

GarbageBlockSlabDram::GarbageBlockSlabDram(int core_id)
    : core_id(core_id)
{
  auto blks = (GarbageBlockDram *) mem::AllocMemory(
      mem::VhandlePool, GarbageBlockDram::kBlockSize * kPreallocPerCore, core_id / mem::kNrCorePerNode);

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

uint64_t GarbageBlockSlabDram::Add(IndexInfo *row, int q_idx) {
  auto half_queue = &half[q_idx];
  auto full_queue = &full[q_idx];
  int idx = 0;
  GarbageBlockDram *blk = nullptr;
  util::MCSSpinLock::QNode qnode;
  lock.Acquire(&qnode);

  if (!half_queue->empty()) {
    blk = half_queue->next->object();
    idx = __builtin_ffsll(~blk->bitmap) - 1;
    abort_if(idx < 0 || idx >= GarbageBlockDram::kBlockSize, "inconsistent garbage block slab!");
    blk->bitmap |= 1ULL << idx;
    if (__builtin_popcountll(blk->bitmap) == GarbageBlockDram::kMaxNrRows - 1) {
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

void GarbageBlockSlabDram::Remove(GarbageBlockDram *blk, int idx)
{
  auto half_queue = &half[blk->q_idx];
  util::MCSSpinLock::QNode qnode;

  lock.Acquire(&qnode);
  if (__builtin_popcountll(blk->bitmap) == GarbageBlockDram::kMaxNrRows) {
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

uint64_t GC_Dram::AddRow(IndexInfo *row, uint64_t epoch_nr) {
  // shirley: note if we're adding to GC after insert, then remove this abort? double check if it causes other problems.
  // abort_if(epoch_nr == 0, "Should not even detect garbage during loader");
  int q_idx = epoch_nr % g_gc_every_epoch;
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  return g_slabs[core_id]->Add(row, q_idx);
}

// shirley: this shouldn't be used.
// void GC_Dram::RemoveRow(IndexInfo *row, uint64_t gc_handle) {
//   uint64_t base = GarbageBlockDram::kBlockSize * (gc_handle / GarbageBlockDram::kBlockSize);
//   int idx = (gc_handle - base - sizeof(util::GenericListNode<GarbageBlockDram>)) / 8;
//   auto blk = (GarbageBlockDram *) base;
//   return g_slabs[blk->alloc_core]->Remove(blk, idx);
// }

unsigned int GC_Dram::g_gc_every_epoch = 0;
bool GC_Dram::g_lazy = false;
std::array<GarbageBlockSlabDram *, NodeConfiguration::kMaxNrThreads> GC_Dram::g_slabs;

void GC_Dram::InitPool()
{
  abort_if(g_gc_every_epoch >= GarbageBlockSlabDram::kNrQueue,
           "g_gc_every_epoch {} >= {}", g_gc_every_epoch, GarbageBlockSlabDram::kNrQueue);

  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    g_slabs[i] = new GarbageBlockSlabDram(i);
  }
}

void GC_Dram::PrepareGCForAllCores()
{
  if (g_lazy)
    return;

  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();

  for (auto core_id = 0; core_id < NodeConfiguration::g_nr_threads; core_id++) {
    auto &slab = g_slabs[core_id];
    int q_idx = (cur_epoch_nr + 1) % g_gc_every_epoch;
    auto *full_queue = &slab->full[q_idx];
    auto *half_queue = &slab->half[q_idx];

    GarbageBlockDram *new_head = nullptr;
    util::GenericListNode<GarbageBlockDram> *tail_node = nullptr;

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
      // shirley debug: changing this to continue (I think continue is correct, not return?)
      continue; 
      // return; // shirley: shouldn't this be continue?
    }

    GarbageBlockDram *tail_next = collect_head;

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

void GC_Dram::RunGC()
{
  // TODO: add memory pressure detection.
  if (g_lazy)
    return;

  auto cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  // shirley: this is not used.
  // int q_idx = (cur_epoch_nr + 1) % g_gc_every_epoch;

  auto &s = stats[go::Scheduler::CurrentThreadPoolId() - 1];

  GarbageBlockDram *b = collect_head.load();
  while (true) {
    // logger->info("GC block {}", (void *) b);
    while (!b || !collect_head.compare_exchange_strong(b, b->next->object())) {
      if (!b) {
        return;
      }
    }

    // shirley: get next block before initializing b or inserting to free list
    auto b_next = b->next->object();

    size_t i = 0;
    // After processing this block, we always need to put it back into the slab!
    util::MCSSpinLock::QNode qnode;
    auto slab = g_slabs[b->alloc_core];
    b->Initialize();

    while (b->bitmap != 0) {
      i = __builtin_ffsll(b->bitmap) - 1;
      // logger->info("Found {} bitmap {:x}", i, b->bitmap);
      // abort_if((uint64_t) &b->rows[i] != b->rows[i]->gc_handle.load(),
      //          "gc_handle {:x} i {} blk {}", b->rows[i]->gc_handle.load(), i,
      //          (void *) b);

      auto old = s.nr_bytes;
      auto nr_processed = Process(b->rows[i], cur_epoch_nr, 16_K);
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
    // shirley note: must acquire lock for correct behavior (somehow it was working before)
    slab->lock.Acquire(&qnode);
    b->InsertAfter(&g_slabs[b->alloc_core]->free);
    slab->lock.Release(&qnode);

    s.nr_blocks++;
    // shirley: don't think it's right to set to b->next here bc already initialized & inserted to free list
    b = b_next;// b->next->object();
  }
}

size_t GC_Dram::Process(IndexInfo *handle, uint64_t cur_epoch_nr, size_t limit) {
  util::MCSSpinLock::QNode qnode;
  handle->lock.Lock(&qnode);
  size_t n = Collect(handle, cur_epoch_nr, limit);
  handle->lock.Unlock(&qnode);
  return n;
}

size_t GC_Dram::Collect(IndexInfo *handle, uint64_t cur_epoch_nr, size_t limit) {

  // shirley todo: detect prev_X and current_X epoch, skip if already cleaned based on memory pressure

  if (!(handle->dram_version)) return 0;
  if (handle->dram_version->ep_num > (cur_epoch_nr - g_gc_every_epoch + 1)) {
    // shirley: this row has been accessed more recently.
    // GC_Dram::AddRow(handle, cur_epoch_nr);
    GC_Dram::AddRow(handle, handle->dram_version->ep_num);
    return 0;
  }
  // shirley: free dram cache
  VarStr *dram_ver = (VarStr *)(handle->dram_version->val);
  if (dram_ver) {
    auto var_reg_id = dram_ver->get_region_id();
    auto var_len = dram_ver->length();
    // shirley debug: something wrong if we free the dram val.
    mem::GetDataRegion().Free(dram_ver, var_reg_id, sizeof(VarStr) + var_len);
  }
  else{
    // felis::probes::NumUnwrittenDramCache{1}();
  }
  mem::GetDataRegion().Free(handle->dram_version, handle->dram_version->this_coreid, sizeof(DramVersion));
  handle->dram_version = nullptr;

  return 1;
}



void GC_Dram::PrintStats()
{
  fmt::memory_buffer buf;
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto &s = stats[i];
    fmt::format_to(buf, " {}({}{})={}K",
                   s.nr_rows, s.nr_blocks, s.straggler ? "*" : "",
                   s.nr_bytes >> 10);
  }
  logger->info("GC_Dram: {}", std::string_view(buf.data(), buf.size()));
}

}

namespace util {

using namespace felis;

static GC_Dram g_gc_dram;

GC_Dram *InstanceInit<GC_Dram>::instance = &g_gc_dram;

}
