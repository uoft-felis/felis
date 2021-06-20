#include <cstdint>
#include <cstdlib>

#include "epoch.h"
#include "gc.h"
#include "log.h"
#include "node_config.h"
#include "util/arch.h"
#include "util/lowerbound.h"
#include "util/objects.h"
#include "index_info.h"

#include "contention_manager.h"
#include "opts.h"

#include "literals.h"

namespace felis {

bool VHandleSyncService::g_lock_elision = false;

VHandleSyncService &BaseIndexInfo::sync() {
  return util::Impl<VHandleSyncService>();
}

IndexInfo::IndexInfo() {
//   nr_ondsplt = 0;
//   this_coreid = mem::ParallelPool::CurrentAffinity();
//   cont_affinity = -1;

  // shirley: versions should be initialized to null
  versions = nullptr;
  // shirley note: need to make sure we only allocate vhandle once (here or in index.cc)
  vhandle = (VHandle *) VHandle::NewInline();

#ifndef NDEBUG
  // we are in debug build
  vhandle = (VHandle *)((int64_t)vhandle | 0x8000000000000000);
#endif

  // shirley: old: alloc versions externally from data region or inline
  // versions = (uint64_t *) mem::GetDataRegion().Alloc(2 * 4 *
  // sizeof(uint64_t)); versions = (uint64_t *) mem::GetTransientPool().Alloc(2
  // * capacity * sizeof(uint64_t)); versions = (uint64_t *) ((uint8_t *) this +
  // 64);
  // latest_version.store(-1); //shirley: remove setting latest_version for new
  // design bc inlined to version array
}

bool IndexInfo::ShouldScanSkip(uint64_t sid) {
  auto epoch_of_txn = sid >> 32;
  util::MCSSpinLock::QNode qnode;
  lock.Acquire(&qnode);
  bool skip = (first_version() >= sid);
  lock.Release(&qnode);
  return skip;
}

static uint64_t *EnlargePair64Array(IndexInfo *row, uint64_t *old_p,
                                    unsigned int old_cap, int old_regionid,
                                    unsigned int new_cap) {
  const size_t old_len = old_cap * sizeof(uint64_t);
  const size_t new_len = new_cap * sizeof(uint64_t);

  // shirley: grab new mem for version array from transient pool
  auto new_p = (uint64_t *)mem::GetTransientPool().Alloc(
      BaseIndexInfo::VerArrayInfoSize + 2 * new_len);
  if (!new_p) {
    return nullptr;
  }

  // shirley: copy header info in version array
  // shirley todo: set new header accordingly. here or outside this function?
  std::copy(old_p,
            (uint64_t *)((uint8_t *)old_p + BaseIndexInfo::VerArrayInfoSize),
            new_p);

  // memcpy(new_p, old_p, old_cap * sizeof(uint64_t));
  std::copy(IndexInfo::versions_ptr(old_p),
            IndexInfo::versions_ptr(old_p) + old_cap,
            IndexInfo::versions_ptr(new_p));
  // memcpy((uint8_t *) new_p + new_len, (uint8_t *) old_p + old_len, old_cap *
  // sizeof(uint64_t));
  std::copy(IndexInfo::versions_ptr(old_p) + old_cap,
            IndexInfo::versions_ptr(old_p) + 2 * old_cap,
            IndexInfo::versions_ptr(new_p) + new_cap);
  // shirley: after using parallel break pool, don't need the free here (cleaned
  // at end of epoch)
  // if ((uint8_t *) old_p - (uint8_t *) row != 64)
  //   mem::GetDataRegion().Free(old_p, old_regionid, 2 * old_len);
  return new_p;
}

std::string IndexInfo::ToString() const {
  fmt::memory_buffer buf;
  auto objects = versions_ptr(versions) + capacity_get(versions);
  for (auto j = 0u; j < size_get(versions); j++) {
    fmt::format_to(buf, "{}->0x{:x} ", versions_ptr(versions)[j], objects[j]);
  }
  return std::string(buf.begin(), buf.size());
}

void IndexInfo::IncreaseSize(int delta, uint64_t epoch_nr) {
  // shirley: remove minor (?) garbage collection for now
  // auto &gc = util::Instance<GC>();
  // auto handle = gc_handle.load(std::memory_order_relaxed);
  // auto latest = latest_version.load(std::memory_order_relaxed);
  // if (size + delta > capacity && capacity >= 512_K) {
  //   gc.Collect((VHandle *) this, epoch_nr, 16_K);
  //   latest = latest_version.load(std::memory_order_relaxed);
  // }

  // logger->info("MOMO in increaseSize calling myprobe");
  // probes::VersionSizeArray{size, delta}();
  // logger->info("MOMO in increaseSize done calling myprobe");

  size_set(versions, size_get(versions) + delta); // size += delta;

  if (unlikely(size_get(versions) > capacity_get(versions))) {
    auto current_regionid = mem::ParallelPool::CurrentAffinity();
    auto new_cap = std::max(
        8U, 1U << (32 - __builtin_clz((unsigned int)size_get(versions))));
    auto new_versions =
        EnlargePair64Array(this, versions, capacity_get(versions), 0, new_cap);

    probes::VHandleExpand{(void *)this, capacity_get(versions), new_cap}();

    abort_if(new_versions == nullptr,
             "Memory allocation failure, second ver epoch {}",
             versions_ptr(versions)[1] >> 32);
    /*
    if (EpochClient::g_workload_client) {
      auto &lm =
    EpochClient::g_workload_client->get_execution_locality_manager();
      lm.PlanLoad(alloc_by_regionid, -1 * (long) size);
      lm.PlanLoad(current_regionid, (long) size);
    }
    */

    versions = new_versions;
    capacity_set(versions, new_cap);
    // alloc_by_regionid = current_regionid;
  }

  auto objects = versions_ptr(versions) + capacity_get(versions);

  std::fill(objects + size_get(versions) - delta, objects + size_get(versions),
            kPendingValue);

  // SHIRLEY: remove minor (?) garbage collection for now
  /*
  if (cur_start != latest + 1) {
    cur_start = latest + 1;
    size_t nr_bytes = 0;
    bool garbage_left = latest >= 16_K;

    if (handle) {
      if (latest > 0)
        gc.Collect((VHandle *) this, epoch_nr, std::min<size_t>(16_K, latest));
      gc.RemoveRow((VHandle *) this, handle);
      gc_handle.store(0, std::memory_order_relaxed);
    }

    latest = latest_version.load();

    if (!garbage_left && latest >= 0
        && GC::IsDataGarbage((VHandle *) this, (VarStr *) objects[latest]))
      garbage_left = true;

    if (garbage_left)
      gc_handle.store(gc.AddRow((VHandle *) this, epoch_nr),
  std::memory_order_relaxed);
  }
  */
}

void IndexInfo::AppendNewVersionNoLock(uint64_t sid, uint64_t epoch_nr,
                                                int ondemand_split_weight) {
  // shirley todo: we need this if we want contention manager optimizations. for
  // now disable. if (ondemand_split_weight) nr_ondsplt +=
  // ondemand_split_weight;

  // append this version at the end of version array
  IncreaseSize(1, epoch_nr);
  BookNewVersionNoLock(sid, size_get(versions) - 1);

#if 0
  // find the location this version is supposed to be
  uint64_t last = versions[size - 1];
  int i = std::lower_bound(versions, versions + size - 1, last) - versions;

  // move versions by 1 to make room, and put this version back
  memmove(&versions[i + 1], &versions[i], sizeof(uint64_t) * (size - i - 1));
  versions[i] = last;
#endif

  AbsorbNewVersionNoLock(size_get(versions) - 1, 0);
}

unsigned int IndexInfo::AbsorbNewVersionNoLock(unsigned int end,
                                               unsigned int extra_shift) {
  if (end == 0) {
    versions_ptr(versions)[extra_shift] = versions_ptr(versions)[0];
    return 0;
  }

  uint64_t last = versions_ptr(versions)[end];
  unsigned int mark = (end - 1) & ~(0x03FF);
  // int i = std::lower_bound(versions + mark, versions + end, last) - versions;

  int i = util::FastLowerBound(versions_ptr(versions) + mark,
                               versions_ptr(versions) + end, last) -
          versions_ptr(versions);
  if (i == mark)
    i = std::lower_bound(versions_ptr(versions), versions_ptr(versions) + mark,
                         last) -
        versions_ptr(versions);

  std::move(versions_ptr(versions) + i, versions_ptr(versions) + end,
            versions_ptr(versions) + i + 1 + extra_shift);
  probes::VHandleAbsorb{this, (int)end - i}();

  versions_ptr(versions)[i + extra_shift] = last;

  return i;
}

// Insert a new version into the version array, with value pending.
void IndexInfo::AppendNewVersion(uint64_t sid, uint64_t epoch_nr,
                                 int ondemand_split_weight) {
  probes::VHandleAppend{this, sid, 0}();

  if (likely(!VHandleSyncService::g_lock_elision)) {
    util::MCSSpinLock::QNode qnode;
    VersionBufferHandle handle;

    if (sid == 0)
      goto slowpath;
    // shirley todo: removed for now, add back if using contention manager
    // if (Options::kVHandleBatchAppend) {
    //   printf("AppendNewVersion: in batch append!\n");
    //   // shirley: this is turned off by default
    //   if (buf_pos.load(std::memory_order_acquire) == -1
    //       //&& size - cur_start < EpochClient::g_splitting_threshold
    //       && lock.TryLock(&qnode)) {
    //     AppendNewVersionNoLock(sid, epoch_nr, ondemand_split_weight);
    //     lock.Unlock(&qnode);
    //     return;
    //   }

    //   handle =
    //       util::Instance<ContentionManager>().GetOrInstall((VHandle *)this);
    //   if (handle.prealloc_ptr) {
    //     handle.Append((VHandle *)this, sid, epoch_nr, ondemand_split_weight);
    //     return;
    //   }
    // } else if (Options::kOnDemandSplitting) {
    //   printf("AppendNewVersion: in on demand splitting!\n");
    //   // shirley: this is turned off by default
    //   // Even if batch append is off, we still create a buf_pos for splitting.
    //   if (buf_pos.load(std::memory_order_acquire) == -1
    //       //&& size - cur_start >= EpochClient::g_splitting_threshold
    //   )
    //     util::Instance<ContentionManager>().GetOrInstall((VHandle *)this);
    // }

  slowpath:
    lock.Lock(&qnode);
    probes::VHandleAppendSlowPath{this}();
    // shirley: this is used by default
    // shirley: if versions is nullptr, allocate new version array, copy sid1 &
    // ptr1, set capacity = 4, size = 1, latest_version = 0, cur_start = 0
    auto current_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
    if (versions_ep != current_epoch_nr) {
      // printf("AppendNewVersion: version array is null. SHOULDN'T REACH
      // HERE!\n"); printf("AppendNewVersion: trying to allocate new version
      // array\n"); shirley: initial size 64 is ok. (fits 4 versions). note:
      // this is without info in ver_array.
      // shirley: use transient pool for version array
      int initial_cap = 2;
      versions_ep = current_epoch_nr;
      versions = (uint64_t *)mem::GetTransientPool().Alloc(
          VerArrayInfoSize + 2 * initial_cap * sizeof(uint64_t));
      capacity_set(versions, initial_cap);
      size_set(versions, 0);
      // cur_start = 0;
      latest_version_set(versions, -1);

      // now add initial version (just set sid=0, ptr2/1) to the new version array
      versions_ptr(versions)[0] = 0; //it's okay to set sid to 0 here.
#ifndef NDEBUG
      // we are in debug build
      VHandle *vhandle = vhandle_ptr();//(VHandle *)((int64_t)vhandle & 0x7FFFFFFFFFFFFFFF);
#endif

      // shirley: try getting initial version from dram cache
      if (dram_version){
        VarStr *init_val = (VarStr *)mem::GetTransientPool().Alloc(VarStr::NewSize(dram_version->val->length()));
        std::memcpy(init_val, dram_version->val, VarStr::NewSize(dram_version->val->length()));
        versions_ptr(versions)[initial_cap] = (uint64_t)init_val;
        mem::GetDataRegion().Free(dram_version->val, 
                                  init_val->get_region_id(), 
                                  VarStr::NewSize(init_val->length()));
      }
      else{
        auto ptr2 = vhandle->GetInlinePtr(felis::SortedArrayVHandle::SidType2);
        if (ptr2){
          // auto sid2 = vhandle->GetInlineSid(felis::SortedArrayVHandle::SidType2);
          versions_ptr(versions)[initial_cap] = (uint64_t)ptr2;
        }
        else{
          // auto sid1 = vhandle->GetInlineSid(felis::SortedArrayVHandle::SidType1);
          auto ptr1 = vhandle->GetInlinePtr(felis::SortedArrayVHandle::SidType1);
          versions_ptr(versions)[initial_cap] = (uint64_t)ptr1;
        }
        dram_version = (DramVersion*) mem::GetDataRegion().Alloc(sizeof(DramVersion));
      }
      
      size_set(versions, 1);
      latest_version_set(versions, 0);
      // shirley debug: do we need to init to 0? I don't think so?
      // versions[1] = 0;
      // versions[2] = 0;
      // versions[3] = 0;
      // versions[5] = 0;
      // versions[6] = 0;
      // versions[7] = 0;

      // shirley: add row to GC bc it's appended to (major GC method)
      // auto &gc = util::Instance<GC>();
      // auto gchandle = gc.AddRow((IndexInfo *)this, epoch_nr);
      // gc_handle.store(gchandle, std::memory_order_relaxed);

      // shirley: this was the old gc add row.
      // gc_handle.store(gc.AddRow((VHandle *) this, epoch_nr),
      // std::memory_order_relaxed);
    }
    AppendNewVersionNoLock(sid, epoch_nr, ondemand_split_weight);
    // shirley: should remove this flush bc only flushing 64 bytes. It's gonna
    // invalidate the cacheline.
    // // _mm_clwb((char *)this); // shirley: flush cache bc we modified some
    // info in vhandle.
    lock.Unlock(&qnode);
  } else {
    AppendNewVersionNoLock(sid, epoch_nr, ondemand_split_weight);
  }
}

volatile uintptr_t *IndexInfo::WithVersion(uint64_t sid, int &pos) {
  assert(size_get(versions) > 0);

  // shirley TODO: how can we prefetch in our new design?
  // Prefetch_vhandle();

  uint64_t *p = versions_ptr(versions);
  uint64_t *start = versions_ptr(versions); //+ cur_start;
  uint64_t *end = versions_ptr(versions) + size_get(versions);
  int latest = latest_version_get(versions);

  if (latest >= 0 && sid > versions_ptr(versions)[latest]) {
    start = versions_ptr(versions) + latest + 1;
    if (start >= versions_ptr(versions) + size_get(versions) || sid <= *start) {
      p = start;
      goto found;
    }
  } else if (latest >= 0) {
    end = versions_ptr(versions) + latest;
    if (*(end - 1) < sid) {
      p = end;
      goto found;
    }
  }

  p = std::lower_bound(start, end, sid);
  if (p == versions_ptr(versions)) {
    logger->critical(
        "ReadWithVersion() {} cannot found for sid {} start is {} begin is {}",
        (void *)this, sid, *start, *versions_ptr(versions));
    return nullptr;
  }
found:
  auto objects = versions_ptr(versions) + capacity_get(versions);

  // A not very useful read-own-write implementation...
  /*
  if (*p == sid
      && (objects[p - versions] >> 56) != 0xFE) {
    return &objects[pos];
  }
  */

  pos = --p - versions_ptr(versions);
  return &objects[pos];
}

// Heart of multi-versioning: reading from its preceding version.
//   for instance, in versions we have 5, 10, 18, and sid = 13
//   then we would like it to read version 10 (so pos = 1)
//   but if sid = 4, then we don't have the value for it to read, then returns
//   nullptr
VarStr *IndexInfo::ReadWithVersion(uint64_t sid) {
#ifndef NDEBUG
  // we are in debug build
  VHandle *vhandle = vhandle_ptr();//(VHandle *)((int64_t)vhandle & 0x7FFFFFFFFFFFFFFF);
#endif
  // shirley: if versions is nullptr, read from sid1/sid2
  auto current_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  if (versions_ep != current_epoch_nr)  {
    if (dram_version){
      dram_version->ep_num = current_epoch_nr;
      return dram_version->val;
    }
    else{
      util::MCSSpinLock::QNode qnode;
      lock.Lock(&qnode);
      if (!dram_version){
        DramVersion *temp_dram_version = (DramVersion*) mem::GetDataRegion().Alloc(sizeof(DramVersion));
        temp_dram_version->ep_num = current_epoch_nr;
        // printf("ReadWithVersion try read from inline\n");
        auto ptr2 = vhandle->GetInlinePtr(felis::SortedArrayVHandle::SidType2);
        // shirley: don't need to compare sid with sid2 bc sid2 should definitely be smaller.
        if (ptr2 /*&& (sid >= vhandle->GetInlineSid(SortedArrayVHandle::SidType2))*/){
          // VarStr *ptr2 = (VarStr *)(vhandle->GetInlinePtr(SortedArrayVHandle::SidType2));
          // return (VarStr *) ptr2;
          temp_dram_version->val = (VarStr*) mem::GetDataRegion().Alloc(VarStr::NewSize(((VarStr*)ptr2)->length()));
          std::memcpy(temp_dram_version->val, ptr2, VarStr::NewSize(((VarStr*)ptr2)->length()));
          temp_dram_version->val->set_region_id(mem::ParallelPool::CurrentAffinity());
        }
        // shirley: don't need to compare sid with sid1 bc sid1 should definitely be smaller.
        else /*if (sid >= vhandle->GetInlineSid(SortedArrayVHandle::SidType1))*/ {
          VarStr *ptr1 = (VarStr *)(vhandle->GetInlinePtr(SortedArrayVHandle::SidType1));
          // return ptr1;
          temp_dram_version->val = (VarStr*) mem::GetDataRegion().Alloc(VarStr::NewSize(ptr1->length()));
          std::memcpy(temp_dram_version->val, ptr1, VarStr::NewSize(ptr1->length()));
          temp_dram_version->val->set_region_id(mem::ParallelPool::CurrentAffinity());
        }
        dram_version = temp_dram_version;
      }
      lock.Unlock(&qnode);
      return dram_version->val;
    }
    
    // printf("ReadWithVersion: !versions, returning nullptr! sid = %u, sid1 =
    // %u\n", sid, GetInlineSid(sid1)); std::abort();
    // return nullptr;

    // shirley: for minor GC approach we need to check sid2 before sid1. But, we don't
    // need to sync().WaitForData bc if versions is null or out of date epoch,
    // then the data is already written in prev epoch. If it was updated in curr
    // epoch then we won't go into this code (i.e. there won't be null versions
    // or out-dated versions ptr).
  }
  // printf("ReadWithVersion: reading from version array!\n");

  int pos;
  volatile uintptr_t *addr = WithVersion(sid, pos);
  if (!addr) {
    // printf("ReadWithVersion: reading from version array FAILED!\n");
    return nullptr;
  }

  sync().WaitForData(addr, sid, versions_ptr(versions)[pos], (void *)vhandle);

  return (VarStr *)*addr;
}

// Read the exact version. version_idx is the version offset in the array, not
// serial id
VarStr *IndexInfo::ReadExactVersion(unsigned int version_idx) {
  assert(size_get(versions) > 0);
  assert(size_get(versions) < capacity_get(versions));
  assert(version_idx < size_get(versions));
  // shirley TODO: assert: we shouldn't reach this function unless we're running
  // granola / pwv / data migration if not, need to handle if versions is null
  // assert(0);

  auto objects = versions_ptr(versions) + capacity_get(versions);
  volatile uintptr_t *addr = &objects[version_idx];
  assert(addr);

  return (VarStr *)*addr;
}

bool IndexInfo::WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr) {
  if (!versions) {
    printf("WriteWithVersions versions is null!!!\n");
    std::abort();
  }
  // shirley TODO: how to use prefetch for our new design?
  // Prefetch_vhandle();
  
  // Finding the exact location
  int pos = latest_version_get(versions);
  uint64_t *it = versions_ptr(versions) + pos + 1;
  if (*it != sid) {
    it = std::lower_bound(versions_ptr(versions) /*+ cur_start*/,
                          versions_ptr(versions) + size_get(versions), sid);
    if (unlikely(it == versions_ptr(versions) + size_get(versions) ||
                 *it != sid)) {
      // sid is greater than all the versions, or the located lower_bound isn't
      // sid version
      logger->critical("Diverging outcomes on {}! sid {} pos {}/{}",
                       (void *)this, sid, it - versions_ptr(versions),
                       size_get(versions));
    // shirley todo: removed this log for now bc not using contention manager
    //   logger->critical("bufpos {}", buf_pos.load());
      fmt::memory_buffer buffer;
      for (int i = 0; i < size_get(versions); i++) {
        fmt::format_to(buffer, "{}{} ", versions_ptr(versions)[i],
                       i == it - versions_ptr(versions) ? "*" : "");
      }
      logger->critical("Versions: {}",
                       std::string_view(buffer.data(), buffer.size()));
      std::abort();
      return false;
    }
  }

  auto objects = versions_ptr(versions) + capacity_get(versions);
  volatile uintptr_t *addr = &objects[it - versions_ptr(versions)];

  // Writing to exact location
  sync().OfferData(addr, (uintptr_t)obj);

  int latest = it - versions_ptr(versions);
  probes::VersionWrite{this, latest, epoch_nr}();
  while (latest > pos) {
    if (latest_version_ptr(versions)->compare_exchange_strong(pos, latest))
      break;
  }

  if (latest == size_get(versions) - 1) {
    // shirley todo: we need these two if we want contention manager
    // optimizations. for now disable. nr_ondsplt = 0; cont_affinity = -1;
  } else {
    // SHIRLEY: this is marking the row for GC which will be performed later?
    /*
    if (GC::IsDataGarbage((VHandle *) this, obj) && gc_handle == 0) {
      auto &gc = util::Instance<GC>();
      auto gchdl = gc.AddRow((VHandle *) this, epoch_nr);
      uint64_t old = 0;
      if (!gc_handle.compare_exchange_strong(old, gchdl)) {
        gc.RemoveRow((VHandle *) this, gchdl);
      }
    }
    */
  }
  return true;
}

bool IndexInfo::WriteExactVersion(unsigned int version_idx, VarStr *obj,
                                  uint64_t epoch_nr) {
  abort_if(version_idx >= size_get(versions),
           "WriteExactVersion overflowed {} >= {}", version_idx,
           size_get(versions));
  // TODO: GC?

  volatile uintptr_t *addr =
      versions_ptr(versions) + capacity_get(versions) + version_idx;
  // sync().OfferData(addr, (uintptr_t) obj);
  *addr = (uintptr_t)obj;

  probes::VersionWrite{this}();

  int ver = latest_version_get(versions);
  while (version_idx > ver) {
    if (latest_version_ptr(versions)->compare_exchange_strong(ver, version_idx))
      break;
  }
  return true;
}

IndexInfo *IndexInfo::New() {
  return new (pool.Alloc()) IndexInfo();
}

mem::ParallelSlabPool BaseIndexInfo::pool;

void BaseIndexInfo::InitPool() {
  pool = mem::ParallelSlabPool(mem::IndexInfoPool, kIndexInfoSize, 4, false);
  pool.Register();
}

} // namespace felis
