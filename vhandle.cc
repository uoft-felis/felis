#include <cstdlib>
#include <cstdint>

#include "util/arch.h"
#include "util/objects.h"
#include "util/lowerbound.h"
#include "log.h"
#include "vhandle.h"
#include "node_config.h"
#include "epoch.h"
#include "gc.h"

#include "opts.h"
#include "contention_manager.h"

#include "priority.h"
#include "literals.h"

namespace felis {

bool VHandleSyncService::g_lock_elision = false;

VHandleSyncService &BaseVHandle::sync()
{
  return util::Impl<VHandleSyncService>();
}

SortedArrayVHandle::SortedArrayVHandle()
{
  capacity = 4;
  // value_mark = 0;
  size = 0;
  cur_start = 0;
  nr_ondsplt = 0;
  inline_used = 0xFF; // disabled, 0x0F at most when enabled.

  // abort_if(mem::ParallelPool::CurrentAffinity() >= 256,
  //         "Too many cores, we need a larger vhandle");

  this_coreid = alloc_by_regionid = mem::ParallelPool::CurrentAffinity();
  extra_vhandle = nullptr;
  cont_affinity = -1;

  // versions = (uint64_t *) mem::GetDataRegion().Alloc(2 * capacity * sizeof(uint64_t));
  versions = (uint64_t *) ((uint8_t *) this + 64);
  latest_version.store(-1);
}

bool SortedArrayVHandle::ShouldScanSkip(uint64_t sid)
{
  auto epoch_of_txn = sid >> 32;
  util::MCSSpinLock::QNode qnode;
  lock.Acquire(&qnode);
  bool skip = (first_version() >= sid);
  lock.Release(&qnode);
  return skip;
}

static uint64_t *EnlargePair64Array(SortedArrayVHandle *row,
                                    uint64_t *old_p, unsigned int old_cap, int old_regionid,
                                    unsigned int new_cap)
{
  const size_t old_len = old_cap * sizeof(uint64_t);
  const size_t new_len = new_cap * sizeof(uint64_t);

  auto new_p = (uint64_t *) mem::GetDataRegion().Alloc(2 * new_len);
  if (!new_p) {
    return nullptr;
  }
  // memcpy(new_p, old_p, old_cap * sizeof(uint64_t));
  std::copy(old_p, old_p + old_cap, new_p);
  // memcpy((uint8_t *) new_p + new_len, (uint8_t *) old_p + old_len, old_cap * sizeof(uint64_t));
  std::copy(old_p + old_cap, old_p + 2 * old_cap, new_p + new_cap);
  if ((uint8_t *) old_p - (uint8_t *) row != 64)
    mem::GetDataRegion().Free(old_p, old_regionid, 2 * old_len);
  return new_p;
}

std::string SortedArrayVHandle::ToString() const
{
  fmt::memory_buffer buf;
  auto objects = versions + capacity;
  for (auto j = 0u; j < size; j++) {
    fmt::format_to(buf, "{}->0x{:x} ", versions[j], objects[j]);
  }
  return std::string(buf.begin(), buf.size());
}

void SortedArrayVHandle::IncreaseSize(int delta, uint64_t epoch_nr)
{
  auto &gc = util::Instance<GC>();
  auto handle = gc_handle.load(std::memory_order_relaxed);
  auto latest = latest_version.load(std::memory_order_relaxed);
  if (size + delta > capacity && capacity >= 512_K) {
    gc.Collect((VHandle *) this, epoch_nr, 16_K);
    latest = latest_version.load(std::memory_order_relaxed);
  }

  size += delta;

  if (unlikely(size > capacity)) {
    auto current_regionid = mem::ParallelPool::CurrentAffinity();
    auto new_cap = std::max(8U, 1U << (32 - __builtin_clz((unsigned int) size)));
    auto new_versions = EnlargePair64Array(this, versions, capacity, alloc_by_regionid, new_cap);

    probes::VHandleExpand{(void *) this, capacity, new_cap}();

    abort_if(new_versions == nullptr,
             "Memory allocation failure, second ver epoch {}",
             versions[1] >> 32);
    /*
    if (EpochClient::g_workload_client) {
      auto &lm = EpochClient::g_workload_client->get_execution_locality_manager();
      lm.PlanLoad(alloc_by_regionid, -1 * (long) size);
      lm.PlanLoad(current_regionid, (long) size);
    }
    */

    versions = new_versions;
    capacity = new_cap;
    alloc_by_regionid = current_regionid;
  }

  auto objects = versions + capacity;

  std::fill(objects + size - delta,
            objects + size,
            kPendingValue);

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
      gc_handle.store(gc.AddRow((VHandle *) this, epoch_nr), std::memory_order_relaxed);
  }
}

void SortedArrayVHandle::AppendNewVersionNoLock(uint64_t sid, uint64_t epoch_nr, int ondemand_split_weight)
{
  if (!NodeConfiguration::g_priority_txn || !PriorityTxnService::g_row_rts)
    if (ondemand_split_weight) nr_ondsplt += ondemand_split_weight;

  // append this version at the end of version array
  IncreaseSize(1, epoch_nr);
  BookNewVersionNoLock(sid, size - 1);

#if 0
  // find the location this version is supposed to be
  uint64_t last = versions[size - 1];
  int i = std::lower_bound(versions, versions + size - 1, last) - versions;

  // move versions by 1 to make room, and put this version back
  memmove(&versions[i + 1], &versions[i], sizeof(uint64_t) * (size - i - 1));
  versions[i] = last;
#endif

  AbsorbNewVersionNoLock(size - 1, 0);
}

unsigned int SortedArrayVHandle::AbsorbNewVersionNoLock(unsigned int end, unsigned int extra_shift)
{
  if (end == 0) {
    versions[extra_shift] = versions[0];
    return 0;
  }

  uint64_t last = versions[end];
  unsigned int mark = (end - 1) & ~(0x03FF);
  // int i = std::lower_bound(versions + mark, versions + end, last) - versions;

  int i = util::FastLowerBound(versions + mark, versions + end, last) - versions;
  if (i == mark)
    i = std::lower_bound(versions, versions + mark, last) - versions;

  std::move(versions + i, versions + end, versions + i + 1 + extra_shift);
  probes::VHandleAbsorb{this, (int) end - i}();

  versions[i + extra_shift] = last;

  return i;
}

// Insert a new version into the version array, with value pending.
void SortedArrayVHandle::AppendNewVersion(uint64_t sid, uint64_t epoch_nr, int ondemand_split_weight)
{
  probes::VHandleAppend{this, sid, alloc_by_regionid}();

  if (likely(!VHandleSyncService::g_lock_elision)) {
    util::MCSSpinLock::QNode qnode;
    VersionBufferHandle handle;

    if (sid == 0) goto slowpath;
    if (Options::kVHandleBatchAppend) {
      if (buf_pos.load(std::memory_order_acquire) == -1
          && size - cur_start < EpochClient::g_splitting_threshold
          && lock.TryLock(&qnode)) {
        AppendNewVersionNoLock(sid, epoch_nr, ondemand_split_weight);
        lock.Unlock(&qnode);
        return;
      }

      handle = util::Instance<ContentionManager>().GetOrInstall((VHandle *) this);
      if (handle.prealloc_ptr) {
        handle.Append((VHandle *) this, sid, epoch_nr, ondemand_split_weight);
        return;
      }
    } else if (Options::kOnDemandSplitting) {
      // Even if batch append is off, we still create a buf_pos for splitting.
      if (buf_pos.load(std::memory_order_acquire) == -1
          && size - cur_start >= EpochClient::g_splitting_threshold)
        util::Instance<ContentionManager>().GetOrInstall((VHandle *) this);
    }

 slowpath:
    lock.Lock(&qnode);
    probes::VHandleAppendSlowPath{this}();
    AppendNewVersionNoLock(sid, epoch_nr, ondemand_split_weight);
    lock.Unlock(&qnode);
  } else {
    AppendNewVersionNoLock(sid, epoch_nr, ondemand_split_weight);
  }
}

bool SortedArrayVHandle::AppendNewPriorityVersion(uint64_t sid)
{
  auto old = extra_vhandle.load();
  if (old == nullptr) {
    // did not exist, allocate
    auto temp = new ExtraVHandle();
    auto succ = extra_vhandle.compare_exchange_strong(old, temp);
    if (succ)
      old = temp;
    else {
      delete temp; // somebody else allocated and CASed their ptr first, just use that
      old = extra_vhandle.load(); // all to avoid an extra atomic load
    }
  }
  return old->AppendNewPriorityVersion(sid);
}

volatile uintptr_t *SortedArrayVHandle::WithVersion(uint64_t sid, int &pos)
{
  if (size == 0) return nullptr;

  if (inline_used != 0xFF) __builtin_prefetch((uint8_t *) this + 128);

  uint64_t *p = versions;
  uint64_t *start = versions + cur_start;
  uint64_t *end = versions + size;
  int latest = latest_version.load();

  if (latest >= 0 && sid > versions[latest]) {
    start = versions + latest + 1;
    if (start >= versions + size || sid <= *start) {
      p = start;
      goto found;
    }
  } else if (latest >= 0) {
    end = versions + latest;
    if (*(end - 1) < sid) {
      p = end;
      goto found;
    }
  }

  p = std::lower_bound(start, end, sid);
  if (p == versions) {
    return nullptr;
  }
found:
  auto objects = versions + capacity;

  // A not very useful read-own-write implementation...
  /*
  if (*p == sid
      && (objects[p - versions] >> 56) != 0xFE) {
    return &objects[pos];
  }
  */

  pos = --p - versions;
  return &objects[pos];
}

// Heart of multi-versioning: reading from its preceding version.
//   for instance, in versions we have 5, 10, 18, and sid = 13
//   then we would like it to read version 10 (so pos = 1)
//   but if sid = 4, then we don't have the value for it to read, then returns nullptr
VarStr *SortedArrayVHandle::ReadWithVersion(uint64_t sid)
{
  abort_if(sid == -1, "sid == -1");
  int pos;
  volatile uintptr_t *addr = WithVersion(sid, pos);

  // MVTO: mark row read timestamp
  if (PriorityTxnService::g_row_rts) {
    uint64_t new_rts_64 = sid >> 8;
    abort_if(new_rts_64 > 0xFFFFFFFF, "too many epochs ({}) for RowRTS", sid >> 32);
    unsigned int new_rts = new_rts_64;
    unsigned int old = this->nr_ondsplt.load();
    while (new_rts > old) {
      if (this->nr_ondsplt.compare_exchange_strong(old, new_rts))
        break;
    }
  }

  // check extra array. If the version in the extra array is closer to the sid than the
  // version in this original version array, read from that instead.
  uint64_t ver = (addr == nullptr) ? 0 : versions[pos];
  auto extra = extra_vhandle.load();
  VarStr* extra_result = extra ? extra->ReadWithVersion(sid, ver, this) : nullptr;
  if (extra_result)
    return extra_result;

  if (!addr) {
    logger->critical("ReadWithVersion() {} cannot find for sid {} begin is {}",
                     (void *) this, sid, *versions);
    return nullptr;
  }

  // mark read bit
  uintptr_t oldval = *addr;
  if (PriorityTxnService::g_read_bit && !(oldval & kReadBitMask)) {
    uintptr_t newval = oldval | kReadBitMask;
    while (!(oldval & kReadBitMask)) {
      uintptr_t val = __sync_val_compare_and_swap(addr, oldval, newval);
      if (val == oldval) break;
      oldval = val;
      newval = oldval | kReadBitMask;
    }
  }

  sync().WaitForData(addr, sid, versions[pos], (void *) this);

  uintptr_t varstr_ptr = *addr & ~kReadBitMask;
  return (VarStr *) varstr_ptr;
}

// Read the exact version. version_idx is the version offset in the array, not serial id
VarStr *SortedArrayVHandle::ReadExactVersion(unsigned int version_idx)
{
  assert(size > 0);
  assert(size < capacity);
  assert(version_idx < size);

  auto objects = versions + capacity;
  volatile uintptr_t *addr = &objects[version_idx];
  assert(addr);

  // mark read bit
  uintptr_t varstr_ptr = *addr;
  if (PriorityTxnService::g_read_bit && !(varstr_ptr & kReadBitMask)) {
    *addr = varstr_ptr | kReadBitMask;
  }
  varstr_ptr = varstr_ptr & ~kReadBitMask;

  return (VarStr *) varstr_ptr;
}

// return true if sid's previous version has been read
bool SortedArrayVHandle::CheckReadBit(uint64_t sid) {
  abort_if(sid == -1, "sid == -1");
  abort_if(!PriorityTxnService::g_read_bit, "CheckReadBit() is called when read bit is off");
  int pos;
  volatile uintptr_t *addr = WithVersion(sid, pos);

  uint64_t ver = (addr == nullptr) ? 0 : versions[pos];
  auto extra = extra_vhandle.load();
  bool is_in = false;
  bool extra_result = extra ? extra->CheckReadBit(sid, ver, this, is_in) : false;
  if (is_in) // if the previous version is in Extra VHandle, just use the result
    return extra_result;

  if (!addr)
    return false; // no previous version exists, no one could have read it, our prepare is good

  if (*addr == kPendingValue)
    return false; // if it's not written, it couldn't be read
  if (PriorityTxnService::g_last_version_patch && pos == this->size - 1 && (*addr & kReadBitMask)) {
    // last version is read
    uint64_t rts = ((uint64_t)(this->GetRowRTS()) << 8) + 1;
    auto wts = sid;
    return wts <= rts;
  }
  return *addr & kReadBitMask;
}

uint32_t SortedArrayVHandle::GetRowRTS()
{
  abort_if(!PriorityTxnService::g_row_rts, "GetRowRTS() is called when Row RTS is off");
  return nr_ondsplt.load(std::memory_order_seq_cst);
}

/** @brief Find a SID that, starting from this SID, all of the versions of this
    row has not been read.
    @param min search lower bound (SID returned cannot be smaller than min).
    @return SID found. */
uint64_t SortedArrayVHandle::SIDBackwardSearch(uint64_t min)
{
  // eg:    extra vhandle         6r            11      14r      16  18
  //        original vhandle  5r      7  8  10      13       15          19
  //        min = 12
  // 1. search in extra, extra = 16
  // 2. search in original, original = 12 (because 10 < min, use min as result)
  // 3. return the larger of the two, 16
  //    TODO: combine 1&2, allowing it to find 15
  abort_if(!PriorityTxnService::g_read_bit, "SIDBackwardSearch() is called when read bit is off");
  uint64_t original = 0, extra = 0;

  auto handle = extra_vhandle.load();
  if (handle) extra = handle->FindUnreadVersionLowerBound(min);
  original = this->FindUnreadVersionLowerBound(min);

  if (original == 0 && extra == 0)
    return util::Instance<PriorityTxnService>().GetMaxProgress();
  if (original == 0) return extra;
  if (extra == 0) return original;
  return (original > extra) ? original : extra;
}

/** @brief Find the lower bound of the last consecutive unread versions.
    @param min search lower bound (SID returned can only be larger than min).
    @return the SID of the version.
    if answer found is smaller than min, return min;
    if all versions are read, return 0. */
uint64_t SortedArrayVHandle::FindUnreadVersionLowerBound(uint64_t min)
{
  // searching backwards. use max_prog to help speed up finding the last read bit version
  uint64_t max_prog = util::Instance<PriorityTxnService>().GetMaxProgress();
  int upper_pos;
  volatile uintptr_t *ptr_obj = WithVersion(max_prog, upper_pos);
  if (ptr_obj == nullptr)
    return 0;
  if (*ptr_obj & kReadBitMask) // all the versions are read
    return 0;
  volatile uintptr_t *ptr_ver = &versions[upper_pos];
  if (*ptr_ver <= min) // answer found is smaller than min
    return min;

  while (!(*ptr_obj & kReadBitMask) && ptr_ver != versions) {
    if (*ptr_ver <= min) // answer found is smaller than min
      return min; // check this before moving the ptr!!!
    ptr_ver--;
    ptr_obj--;
  } // this while will at least happen once

  if (ptr_ver == versions && !(*ptr_obj & kReadBitMask))
    return versions[0];

  return *(ptr_ver + 1);
}

/** @brief Find the first SID > min that is unread.
    @param min search lower bound (if answer found is smaller than min, return min)
    @return SID found.*/
uint64_t SortedArrayVHandle::SIDForwardSearch(uint64_t min)
{
  abort_if(!PriorityTxnService::g_read_bit, "SIDForwardSearch() is called when read bit is off");
  uint64_t original = 0, extra = 0;

  original = this->FindFirstUnreadVersion(min);
  auto handle = extra_vhandle.load();
  if (handle) extra = handle->FindFirstUnreadVersion(min);

  if (original == 0 && extra == 0)
  {
    if (PriorityTxnService::g_last_version_patch) {
      uint64_t rts = (this->nr_ondsplt.load() + 1) << 8;
      if (rts < min)
        return min;
      return rts;
    }
    return util::Instance<PriorityTxnService>().GetMaxProgress();
  }
  if (original == 0) return extra;
  if (extra == 0) return original;
  return (original < extra) ? original : extra;
}

// if no unread version can be found in the range of [min, core_prog) can be found, return 0.
uint64_t SortedArrayVHandle::FindFirstUnreadVersion(uint64_t min)
{
  int lower_pos;
  volatile uintptr_t *ptr_obj = WithVersion(min, lower_pos);
  if (ptr_obj == nullptr)
    return min;

  volatile uintptr_t *ptr_ver = &versions[lower_pos]; // could be smaller than min

  while (*ptr_obj & kReadBitMask) {
    ptr_ver++;
    ptr_obj++;
    if (ptr_ver - versions >= size) // search out of bound, all versions are read
      return 0;
    int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    uint64_t core_prog = util::Instance<PriorityTxnService>().GetProgress(core_id);
    if (*ptr_ver >= core_prog)
      return 0; // since giving out SID is apprxiamte, using core_prog as boundary is enough
  }

  if (*ptr_ver <= min)
    return min;
  return *ptr_ver;
}

bool SortedArrayVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr)
{
  if (PriorityTxnService::isPriorityTxn(sid)) {
    // go to extra array to find version
    auto extra = extra_vhandle.load();
    if (extra && (extra->WriteWithVersion(sid, obj)))
      return true;
    logger->critical("priority write failed, sid {}, extra {}", sid, (void*)extra);
    std::abort();
  }

  if (inline_used != 0xFF) __builtin_prefetch((uint8_t *) this + 128);
  // Finding the exact location
  int pos = latest_version.load();
  uint64_t *it = versions + pos + 1;
  if (*it != sid) {
    it = std::lower_bound(versions + cur_start, versions + size, sid);
    if (unlikely(it == versions + size || *it != sid)) {
      // sid is greater than all the versions, or the located lower_bound isn't sid version
      logger->critical("Diverging outcomes on {}! sid {} pos {}/{}", (void *) this,
                       sid, it - versions, size);
      logger->critical("bufpos {}", buf_pos.load());
      fmt::memory_buffer buffer;
      for (int i = 0; i < size; i++) {
        fmt::format_to(buffer, "{}{} ", versions[i], i == it - versions ? "*" : "");
      }
      logger->critical("Versions: {}", std::string_view(buffer.data(), buffer.size()));
      std::abort();
      return false;
    }
  }

  auto objects = versions + capacity;
  volatile uintptr_t *addr = &objects[it - versions];

  // Writing to exact location
  sync().OfferData(addr, (uintptr_t) obj);

  int latest = it - versions;
  probes::VersionWrite{this, latest, epoch_nr}();
  while (latest > pos) {
    if (latest_version.compare_exchange_strong(pos, latest))
      break;
  }

  if (latest == size - 1) {
    if (!NodeConfiguration::g_priority_txn || !PriorityTxnService::g_row_rts)
      nr_ondsplt = 0;
    cont_affinity = -1;
  } else {
    if (GC::IsDataGarbage((VHandle *) this, obj) && gc_handle == 0) {
      auto &gc = util::Instance<GC>();
      auto gchdl = gc.AddRow((VHandle *) this, epoch_nr);
      uint64_t old = 0;
      if (!gc_handle.compare_exchange_strong(old, gchdl)) {
        gc.RemoveRow((VHandle *) this, gchdl);
      }
    }
  }
  return true;
}

bool SortedArrayVHandle::WriteExactVersion(unsigned int version_idx, VarStr *obj, uint64_t epoch_nr)
{
  abort_if(version_idx >= size, "WriteExactVersion overflowed {} >= {}", version_idx, size);
  // TODO: GC?

  volatile uintptr_t *addr = versions + capacity + version_idx;
  // sync().OfferData(addr, (uintptr_t) obj);
  *addr = (uintptr_t) obj;

  probes::VersionWrite{this}();

  int ver = latest_version.load();
  while (version_idx > ver) {
    if (latest_version.compare_exchange_strong(ver, version_idx))
      break;
  }
  return true;
}

#if 0
void SortedArrayVHandle::GarbageCollect()
{
  auto objects = versions + capacity;
  if (size < 2) return;

  for (int i = 0; i < size - 1; i++) {
    VarStr *o = (VarStr *) objects[i];
    o = (VarStr *) ((uintptr_t)o & ~kReadBitMask);
    delete o;
  }

  versions[0] = versions[size - 1];
  objects[0] = objects[size - 1];
  latest_version.fetch_sub(size - 1);
  size = 1;
}
#endif

SortedArrayVHandle *SortedArrayVHandle::New()
{
  return new (pool.Alloc()) SortedArrayVHandle();
}

SortedArrayVHandle *SortedArrayVHandle::NewInline()
{
  auto r = new (inline_pool.Alloc()) SortedArrayVHandle();
  r->inline_used = 0;
  return r;
}

mem::ParallelSlabPool BaseVHandle::pool;
mem::ParallelSlabPool LinkedListExtraVHandle::Entry::pool;
mem::ParallelSlabPool BaseVHandle::inline_pool;

void BaseVHandle::InitPool()
{
  pool = mem::ParallelSlabPool(mem::VhandlePool, kSize, 4);
  inline_pool = mem::ParallelSlabPool(mem::VhandlePool, kInlinedSize, 4);
  pool.Register();
  inline_pool.Register();
}

#ifdef ARRAY_EXTRA_VHANDLE
ArrayExtraVHandle::ArrayExtraVHandle()
{
  capacity = 4;
  size.store(0);
  this_coreid = alloc_by_regionid = mem::ParallelPool::CurrentAffinity();

  versions = (uint64_t *) mem::GetDataRegion().Alloc(2 * capacity * sizeof(uint64_t));
}

bool ArrayExtraVHandle::AppendNewVersion(uint64_t sid)
{
  util::MCSSpinLock::QNode qnode;
  lock.Lock(&qnode);

  if (size > 0 && versions[size - 1] >= sid) {
    lock.Unlock(&qnode);
    return false;
  }

  size++;
  if (unlikely(size >= capacity)) {
    auto current_regionid = mem::ParallelPool::CurrentAffinity();
    auto new_cap = 1U << (32 - __builtin_clz((unsigned int) size));
    // ensure the pending reads doesn't stuck on old pending address
    auto objects = versions + capacity;
    for (int i = 0; i < size; ++i) {
      if (util::Impl<VHandleSyncService>().IsPendingVal(objects[i])) {
        util::Impl<VHandleSyncService>().OfferData(&objects[i], kRetryValue);
      }
    }
    auto new_versions = EnlargePair64Array(versions, capacity, alloc_by_regionid, new_cap);
    if (new_versions == nullptr) {
      logger->critical("Memory allocation failure for extra array");
      std::abort();
    }
    auto new_objects = new_versions + new_cap;
    for (int i = 0; i < size; ++i) {
      if (new_objects[i] == kRetryValue) {
        new_objects[i] = kPendingValue;
      }
    }
    versions = new_versions;
    capacity = new_cap;
    alloc_by_regionid = current_regionid;
  }
  std::fill(versions + capacity + size - 1, versions + capacity + size, kPendingValue);

  versions[size - 1] = sid;

  // no need to insertion sort, extra array is always appending in the back
  // TODO: GC

  // trace(TRACE_PRIORITY "txn sid {} - append on extra Vhandle {:p}, current size {}", sid, (void*)this, size.load());
  lock.Unlock(&qnode);
  return true;
}

// sid: txn's sid
// ver: the version we found in the original version array
// return: if the version in extra array is closer, the VarStr we read; else, nullptr
VarStr *ArrayExtraVHandle::ReadWithVersion(uint64_t sid, uint64_t ver, SortedArrayVHandle* handle)
{
  util::MCSSpinLock::QNode qnode;
  lock.Lock(&qnode);

  uint64_t *p = versions;
  uint64_t *start = versions;
  uint64_t *end = versions + size;

  p = std::lower_bound(start, end, sid);
  if (p == versions) {
    lock.Unlock(&qnode);
    return nullptr;
  }

  int pos = --p - versions;
  auto ver_extra = versions[pos];
  if (ver_extra > ver) {
    auto addr = versions + capacity + pos;
    lock.Unlock(&qnode);
    util::Impl<VHandleSyncService>().WaitForData(addr, sid, ver_extra, (void*) this);
    auto varstr_ptr = *addr;
    if (VHandleSyncService::IsIgnoreVal(varstr_ptr))
      return handle->ReadWithVersion(ver_extra);
    else if (varstr_ptr == kRetryValue) {
      // trace(TRACE_PRIORITY "RETRY ACTUALLY HAPPENED! sid {} pos {} ver {} old versions {:p}", sid, pos, ver_extra, (void *)versions);
      return this->ReadWithVersion(sid, ver, handle);
    }
    else
      return (VarStr *) varstr_ptr;
  } else {
    lock.Unlock(&qnode);
    return nullptr;
  }
}

bool ArrayExtraVHandle::WriteWithVersion(uint64_t sid, VarStr *obj)
{
  util::MCSSpinLock::QNode qnode;
  lock.Lock(&qnode);

  // Finding the exact location
  auto it = std::lower_bound(versions, versions + size, sid);
  if (unlikely(it == versions + size || *it != sid)) {
    // sid is greater than all the versions, or the located lower_bound isn't sid version
    logger->critical("Diverging outcomes on extra array {}! sid {} pos {}/{}", (void *) this,
                     sid, it - versions, size);
    std::stringstream ss;
    for (int i = 0; i < size; i++) {
      ss << versions[i] << ' ';
    }
    logger->critical("Versions: {}", ss.str());
    lock.Unlock(&qnode);
    return false;
  }

  auto objects = versions + capacity;
  volatile uintptr_t *addr = &objects[it - versions];

  // Writing to exact location
  util::Impl<VHandleSyncService>().OfferData(addr, (uintptr_t) obj);

  lock.Unlock(&qnode);
  return true;
}
#else
LinkedListExtraVHandle::LinkedListExtraVHandle()
    : head(nullptr), size(0)
{
  this_coreid = alloc_by_regionid = mem::ParallelPool::CurrentAffinity();
}
bool LinkedListExtraVHandle::IsLockLess(void) {
  return PriorityTxnService::g_lockless_append;
}

bool LinkedListExtraVHandle::AppendNewPriorityVersion(uint64_t sid)
{
  if (!IsLockLess()) {
    Entry *n = new Entry(sid, kPendingValue, mem::ParallelPool::CurrentAffinity());
    util::MCSSpinLock::QNode qnode;
    lock.Lock(&qnode);
    Entry dummy(LONG_MAX, kPendingValue, 0);
    dummy.next = head;

    Entry *cur = &dummy;
    while (cur && cur->next && cur->next->version > sid)
      cur = cur->next;
    n->next = cur->next;
    cur->next = n;
    head = dummy.next;

    lock.Unlock(&qnode);
    return true;
  }

  Entry *old, *n = new Entry(sid, kPendingValue, mem::ParallelPool::CurrentAffinity());
  do {
    old = head.load();
    if (old && old->version >= sid) {
      delete n;
      return false;
    }
    n->next = old;
  } while (!head.compare_exchange_strong(old, n));

  size++;
  return true;
}

// return: if the version in extra array is closer, the VarStr we read; else, nullptr
VarStr *LinkedListExtraVHandle::ReadWithVersion(uint64_t sid, uint64_t ver, SortedArrayVHandle* handle)
{
  util::MCSSpinLock::QNode qnode;
  if (!IsLockLess())
    lock.Acquire(&qnode);

  Entry *p = head;
  while (p && ((p->version >= sid) || (p->version < sid && VHandleSyncService::IsIgnoreVal(p->object))))
    p = p->next;

  if (!IsLockLess()) lock.Release(&qnode);
  if (!p)
    return nullptr;

  abort_if(p->version >= sid, "p->version >= sid, {} >= {}", p->version, sid);
  auto ver_extra = p->version;
  if (ver_extra < ver)
    return nullptr;
  volatile uintptr_t *addr = &p->object;

  // mark read bit
  uintptr_t oldval = *addr;
  if (PriorityTxnService::g_read_bit && !(oldval & kReadBitMask)) {
    uintptr_t newval = oldval | kReadBitMask;
    while (!(oldval & kReadBitMask)) {
      uintptr_t val = __sync_val_compare_and_swap(addr, oldval, newval);
      if (val == oldval) break;
      oldval = val;
      newval = oldval | kReadBitMask;
    }
  }

  util::Impl<VHandleSyncService>().WaitForData(addr, sid, ver_extra, (void *) this);
  auto varstr_ptr = *addr & ~kReadBitMask;
  if (VHandleSyncService::IsIgnoreVal(varstr_ptr))
    return handle->ReadWithVersion(ver_extra);

  return (VarStr *) varstr_ptr;
}

// bool& is_in:  true if the previous version is indeed in the extra array
// return value: true if read bit is set
bool LinkedListExtraVHandle::CheckReadBit(uint64_t sid, uint64_t ver, SortedArrayVHandle* handle, bool& is_in) {
  abort_if(!PriorityTxnService::g_read_bit, "ExtraVHandle CheckReadBit() is called when read bit is off");
  util::MCSSpinLock::QNode qnode;
  if (!IsLockLess())
    lock.Acquire(&qnode);
  is_in = false;
  Entry *p = head;
  while (p && ((p->version >= sid) || (p->version < sid && VHandleSyncService::IsIgnoreVal(p->object))))
    p = p->next;

  if (!IsLockLess()) lock.Release(&qnode);
  if (!p)
    return false;

  auto ver_extra = p->version;
  if (ver_extra < ver)
    return false;

  is_in = true;
  auto varstr_ptr = p->object;
  if (varstr_ptr == kPendingValue)
    return false; // if it's not written, it couldn't be read
  if (PriorityTxnService::g_last_version_patch && p == head && (varstr_ptr & kReadBitMask)) {
    // last version is read
    uint64_t rts = ((uint64_t)(handle->GetRowRTS()) << 8) + 1;
    auto wts = sid;
    return wts <= rts;
  }
  return varstr_ptr & kReadBitMask;
}

/** @brief Find the lower bound of the last consecutive unread versions.
    For example: 6r <- 11 <- 14r <- 16 <- 18, return 16.
    @param min search lower bound
    @return the SID of the version.
    if answer found is smaller than min, return min;
    if all versions are read, return 0. */
uint64_t LinkedListExtraVHandle::FindUnreadVersionLowerBound(uint64_t min)
{
  Entry dummy(0, 0, 0);
  dummy.next = head;
  Entry *cur = &dummy;
  while (cur->next && !(cur->next->object & kReadBitMask)) {
    cur = cur->next;
    if (cur->version <= min)
      return min;
  }
  if (cur == &dummy)
    return 0;
  return cur->version;
}

// if no unread version can be found in the range of [min, core_prog) can be found, return 0.
uint64_t LinkedListExtraVHandle::FindFirstUnreadVersion(uint64_t min)
{
  util::MCSSpinLock::QNode qnode;
  if (!IsLockLess())
    lock.Acquire(&qnode);
  Entry dummy(0, 0, 0);
  dummy.next = head;
  Entry *cur = &dummy;
  uint64_t last_unread = ~0; // last unread SID in the linked list
  while (cur->next && cur->next->version >= min) {
    // tricky, since linked list SID is in descending order
    cur = cur->next;
    if (!(cur->object & kReadBitMask)) {
      abort_if(last_unread < cur->version, "what? last_unread < cur->version");
      last_unread = cur->version;
    }
  }

  if (cur->next && cur->next->version < min && !(cur->next->object & kReadBitMask))
  {
    if (!IsLockLess()) lock.Release(&qnode);
    return min; // special case: if the version before min is unread, we can use min
  }
  if (!IsLockLess()) lock.Release(&qnode);
  if (last_unread == ~0)
    return 0;
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  uint64_t core_prog = util::Instance<PriorityTxnService>().GetProgress(core_id);
  if (last_unread >= core_prog)
    return 0;
  return last_unread;
}

bool LinkedListExtraVHandle::WriteWithVersion(uint64_t sid, VarStr *obj)
{
  util::MCSSpinLock::QNode qnode;
  if (!IsLockLess())
    lock.Acquire(&qnode);

  Entry *p = head;
  while (p && p->version > sid)
    p = p->next;

  if (!p || p->version != sid) {
    logger->critical("Diverging outcomes! sid {}", sid);
    std::stringstream ss;
    Entry *p = head;
    while (p) {
      ss << "{" << std::dec << p->version << "(hex" << std::hex << p->version <<"), 0x" << std::hex << p->object << "}->";
      p = p->next;
    }
    logger->critical("Extra Linked list: {}nullptr", ss.str());
    if (!IsLockLess()) lock.Release(&qnode);
    return false;
  }

  if (!IsLockLess()) lock.Release(&qnode);
  volatile uintptr_t *addr = &p->object;
  util::Impl<VHandleSyncService>().OfferData(addr, (uintptr_t) obj);
  return true;
}

void LinkedListExtraVHandle::GarbageCollect()
{
  // GC all but one version: largest version with actual value (not ignore)
  Entry *cur = this->head.load();
  while (cur) {
    if (!VHandleSyncService::IsIgnoreVal(cur->object)) {
      // found the version to keep, start GC
      Entry *front = head, *behind = cur->next;
      while (front && front != cur) {
        if (!VHandleSyncService::IsIgnoreVal(front->object)) {
          VarStr *o = (VarStr *) front->object;
          o = (VarStr *) ((uintptr_t)o & ~kReadBitMask);
          delete o;
        }
        Entry *temp = front->next;
        delete front;
        front = temp;
      }
      while (behind) {
        if (!VHandleSyncService::IsIgnoreVal(behind->object)) {
          VarStr *o = (VarStr *) behind->object;
          o = (VarStr *) ((uintptr_t)o & ~kReadBitMask);
          delete o;
        }
        Entry *temp = behind->next;
        delete behind;
        behind = temp;
      }
      cur->next = nullptr;
      this->head = cur;
      this->size = 1;
      return;
    } else {
      // keep trying to find the version to keep
      cur = cur->next;
    }
  }

  // if it reaches here, it means cur == nullptr, all Entry are ignore value
  cur = this->head.load();
  while (cur) {
    Entry *temp = cur->next;
    delete cur;
    cur = temp;
  }
  this->head = nullptr;
  this->size = 0;
}

#endif

#ifdef LL_REPLAY

LinkListVHandle::LinkListVHandle()
    : this_coreid(mem::CurrentAllocAffinity()), lock(false), head(nullptr), size(0)
{
}

mem::Pool *LinkListVHandle::Entry::pools;

void LinkListVHandle::Entry::InitPools()
{
  pools = InitPerCorePool(32, 16 << 20);
}

bool LinkListVHandle::AppendNewVersion(uint64_t sid, uint64_t epoch_nr)
{
  bool expected = false;
  if (!lock.compare_exchange_strong(expected, true)) {
    return false;
  }

  gc_rule(*this, sid, epoch_nr);

  Entry **p = &head;
  Entry *cur = head;
  Entry *n = nullptr;
  while (cur) {
    if (cur->version < sid) break;
    if (cur->version == sid) goto done;
    p = &cur->next;
    cur = cur->next;
  }
  n = new Entry {cur, sid, kPendingValue, mem::CurrentAllocAffinity()};
  *p = n;
  size++;
done:
  lock.store(false);
  return true;
}

VarStr *LinkListVHandle::ReadWithVersion(uint64_t sid)
{
  Entry *p = head;
  int search_count = 1;
  while (p && p->version >= sid) {
    search_count++;
    p = p->next;
  }

  // PROBE2(felis, linklist_search_read, search_count, size);

  if (!p) return nullptr;

  volatile uintptr_t *addr = &p->object;
  WaitForData(addr, sid, p->version, (void *) this);
  return (VarStr *) *addr;
}

bool LinkListVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run)
{
  assert(this);
  Entry *p = head;
  int search_count = 1;
  while (p && p->version != sid) {
    search_count++;
    p = p->next;
  }
  // PROBE2(felis, linklist_search_write, search_count, size);
  if (!p) {
    logger->critical("Diverging outcomes! sid {}", sid);
    return false;
  }
  if (dry_run) return true;

  volatile uintptr_t *addr = &p->object;
  sync.OfferData(addr, (uintptr_t) obj);
  return true;
}

void LinkListVHandle::GarbageCollect()
{
  Entry *p = head;
  Entry **pp = &head;
  if (!p || p->next == nullptr) return;

  while (p && p->version >= gc_rule.min_of_epoch) {
    pp = &p->next;
    p = p->next;
  }

  if (!p) return;

  *pp = nullptr; // cut of the link list
  while (p) {
    Entry *next = p->next;
    VarStr *o = (VarStr *) p->object;
    delete o;
    delete p;
    p = next;
    size--;
  }
}

#endif

#ifdef CALVIN_REPLAY

CalvinVHandle::CalvinVHandle()
    : lock(false), pos(0)
{
  this_coreid = alloc_by_coreid = mem::CurrentAllocAffinity();
  auto &region = mem::GetThreadLocalRegion(alloc_by_coreid);
  size = 0;
  capacity = 4;

  accesses = (uint64_t *) region.Alloc(capacity * sizeof(uint64_t));
  obj = nullptr;
}

bool CalvinVHandle::AppendNewVersion(uint64_t sid, uint64_t epoch_nr)
{
  return AppendNewAccess(sid, epoch_nr);
}

bool CalvinVHandle::AppendNewAccess(uint64_t sid, uint64_t epoch_nr, bool is_read)
{
  bool expected = false;
  if (!lock.compare_exchange_strong(expected, true))
    return false;

  gc_rule(*this, sid, epoch_nr);

  size++;
  EnsureSpace();

  uint64_t access_turn = sid << 1;
  if (!is_read) access_turn |= 1;

  uint64_t last = accesses[size - 1] = access_turn;
  int i = size - 1;
  while (i > 0 && accesses[i - 1] > last) i--;
  memmove(&accesses[i + 1], &accesses[i], (size - i - 1) * sizeof(uint64_t));
  accesses[i] = last;
  lock.store(false);
  return true;
}

void CalvinVHandle::EnsureSpace()
{
  if (unlikely(size == capacity)) {
    auto current_coreid = mem::CurrentAllocAffinity();
    auto old_accesses = accesses;
    auto old_capacity = capacity;
    capacity *= 2;
    accesses = (uint64_t *) mem::GetThreadLocalRegion(current_coreid).Alloc(capacity * sizeof(uint64_t));
    memcpy(accesses, old_accesses, old_capacity * sizeof(uint64_t));
    mem::GetThreadLocalRegion(alloc_by_coreid).Free(old_accesses, old_capacity * sizeof(uint64_t));
    alloc_by_coreid = current_coreid;
  }
}

uint64_t CalvinVHandle::WaitForTurn(uint64_t sid)
{
  // if (pos.load(std::memory_order_acquire) >= size) std::abort();

  /*
   * Because Calvin enforces R-R conflicts, we may get into deadlocks if we do
   * not switch to another txn. A simple scenario is:
   *
   * T1: t24, t19
   * T2: t20 t24
   *
   * Both t24 and t19 read some key, but, because calvin enforces R-R conflicts
   * too, t24 will need to wait for t19. This is unnecessary of course, but
   * allow arbitrary R-R ordering in Calvin can be expensive.
   *
   * Both dolly and Calvin are safe with W-W conflict because the both SSN and
   * SSI guarantee writes are "fresh". Commit back in time cannot happen between
   * W-W conflict.
   *
   * In princple we do not need to worry about R-W conflicts either, because the
   * dependency graph is tree.
   *
   * Solution for R-R conflicts in Calvin is simple: spin for a while and switch
   * to the next txn.
   */
  uint64_t switch_counter = 0;
  while (true) {
    uint64_t turn = accesses[pos.load()];
    if ((turn >> 1) == sid) {
      return turn;
    }
#if 0
    if (++switch_counter >= 10000000UL) {
      // fputs("So...Hmm, switching cuz I've spinned enough", stderr);
      go::Scheduler::Current()->RunNext(go::Scheduler::NextReadyState);
      switch_counter = 0;
    }
#endif
    asm volatile("pause": : :"memory");
  }
}

bool CalvinVHandle::PeekForTurn(uint64_t sid)
{
  // Binary search over the entire accesses array
  auto it = std::lower_bound((uint64_t *) accesses,
                             (uint64_t *) accesses + size,
                             sid << 1);
  if (it == accesses + size) return false;
  if ((*it) >> 1 == sid) return true;
  /*
  if ((accesses[0] >> 1) < sid) {
    std::abort();
  }
  */
  return false;
}

bool CalvinVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run)
{
  uint64_t turn = WaitForTurn(sid);

  if (!dry_run) {
    delete this->obj;
    this->obj = obj;
    if (obj == nullptr && pos.load() == size - 1) {
      // delete the garbage
    }
    pos.fetch_add(1);
  }
  return true;
}

VarStr *CalvinVHandle::ReadWithVersion(uint64_t sid)
{
  // Need for scan
  if (!PeekForTurn(sid))
    return nullptr;

  uint64_t turn = WaitForTurn(sid);
  VarStr *o = obj;
  if ((turn & 0x01) == 0) {    // I only need to read this record, so advance the pos. Otherwise, I do not
    // need to advance the pos, because I will write to this later.
    pos.fetch_add(1);
  }
  return o;
}

VarStr *CalvinVHandle::DirectRead()
{
  return obj;
}

void CalvinVHandle::GarbageCollect()
{
#if 0
  if (size < 2) return;

  auto it = std::lower_bound(accesses, accesses + size, gc_rule.min_of_epoch);
  memmove(accesses, it, (it - accesses) * sizeof(uint64_t));
  size -= it - accesses;
  pos.fetch_sub(it - accesses, std::memory_order_release);
  return;
#endif
  // completely clear the handle?
  size = 0;
  pos.store(0);
}

#endif

}
