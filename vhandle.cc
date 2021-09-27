#include <cstdlib>
#include <cstdint>

#include "util/arch.h"
#include "util/objects.h"
#include "util/lowerbound.h"
#include "log.h"
#include "vhandle.h"
#include "index_info.h"
#include "node_config.h"
#include "epoch.h"
#include "gc.h"
#include "gc_dram.h"

#include "opts.h"
#include "contention_manager.h"

#include "literals.h"

namespace felis {

// shirley: moved sync into index_info
// bool VHandleSyncService::g_lock_elision = false;

// VHandleSyncService &BaseVHandle::sync()
// {
//   return util::Impl<VHandleSyncService>();
// }

SortedArrayVHandle::SortedArrayVHandle()
{
  // probes::NumVHandlesTotal{1}();
  // shirley: capacity = 0 if we initialize versions to nullptr?
  // shirley: actually, initial capacity field doesn't matter because
  // we set capacity when we allocate version array
  // so just leave it at 4.
  // capacity = 4; // shirley: removed setting capacity for new design bc inlined to version array

  // value_mark = 0;
  // size = 0; // shirley: removed setting size for new design bc inlined to version array
  //cur_start = 0;
  nr_ondsplt = 0;
  //shirley TODO: we don't need this inline_used variable with our design. we have our own flags/bitmaps.
  //inline_used = 0xFF; // disabled, 0x0F at most when enabled.

  // abort_if(mem::ParallelPool::CurrentAffinity() >= 256,
  //         "Too many cores, we need a larger vhandle");

  this_coreid = mem::ParallelPool::CurrentAffinity();
  cont_affinity = -1;

  // shirley: versions should be initialized to null
  // versions = nullptr;
  // shirley: old: alloc versions externally from data region or inline
  // versions = (uint64_t *) mem::GetDataRegion().Alloc(2 * 4 * sizeof(uint64_t)); 
  // versions = (uint64_t *) mem::GetTransientPool().Alloc(2 * capacity * sizeof(uint64_t)); 
  // versions = (uint64_t *) ((uint8_t *) this + 64);
  //latest_version.store(-1); //shirley: remove setting latest_version for new design bc inlined to version array
}


SortedArrayVHandle *SortedArrayVHandle::New()
{
  //shirley: this function shouldn't be called bc we configured all tables to use inline
  return new (pool.Alloc()) SortedArrayVHandle();
}

// shirley NOTE: there's a place in hashtable_index_impl.cc that directly 
// allocates new vhandle without going through these functions.
SortedArrayVHandle *SortedArrayVHandle::NewInline()
{
  auto r = new (inline_pool.Alloc(true)) SortedArrayVHandle(); // shirley: set exception to true for inline_pool.Alloc
  // auto r = new (inline_pool.Alloc(false)) SortedArrayVHandle();
  //shirley TODO: we don't use inline_used variable in our design. we have our own flags/bitmaps
  //r->inline_used = 0;

  // shirley: we can't modify the vhandle layout because for hashtable_index tables,
  // they store a HashEntry within the vhandle at vhandle+96. 
  // size of HashEntry is 32 bytes. So we only have 128 bytes at the end for mini-heap

  return r;
}

//shirley TODO: we can comment out the pool bc all vhandles should be inlined
mem::ParallelSlabPool BaseVHandle::pool;
mem::ParallelBrkWFree BaseVHandle::inline_pool;
// mem::ParallelSlabPool BaseVHandle::inline_pool;

void BaseVHandle::InitPool()
{
  // shirley todo: use ParallelBrkWFree pool, let each core have 2G? 2147483648
  size_t VHandlePoolSize = ((size_t)1024)*1024*1024*2; // 2 GB for now
  // shirley TODO: pool should be removed, only need inline_pool
  // shirley pmem: when on pmem machine, set to true. when on our machines, set to false

  pool = mem::ParallelSlabPool(mem::VhandlePool, kSize, 4, false);
  // shirley: changed to parallel brk w free pool. also don't register
  void *fixed_mmap_addr = nullptr;
  inline_pool = mem::ParallelBrkWFree(
      mem::VhandlePool, mem::VhandleFreelistPool, fixed_mmap_addr,
      VHandlePoolSize, kInlinedSize, false, Options::kRecovery);
  // inline_pool = mem::ParallelSlabPool(mem::VhandlePool, kInlinedSize, 4, false);
  pool.Register();
  // inline_pool.Register();
}

void BaseVHandle::PersistPoolOffsets(bool first_slot) {
  inline_pool.persistOffsets(first_slot);
}

}
