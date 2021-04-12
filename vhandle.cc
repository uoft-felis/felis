#include <cstdlib>
#include <cstdint>

#include "util.h"
#include "log.h"
#include "vhandle.h"
#include "node_config.h"
#include "gc.h"

#include "opts.h"
#include "vhandle_batchappender.h"

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
  this_coreid = alloc_by_regionid = mem::ParallelPool::CurrentAffinity();
  extra_vhandle = nullptr;

  versions = (uint64_t *) mem::GetDataRegion().Alloc(2 * capacity * sizeof(uint64_t));
  latest_version.store(0);
}

bool SortedArrayVHandle::ShouldScanSkip(uint64_t sid)
{
  auto epoch_of_txn = sid >> 32;
  if (last_gc_mark_epoch > 0 && last_gc_mark_epoch < epoch_of_txn)
    return false;
  util::MCSSpinLock::QNode qnode;
  lock.Acquire(&qnode);
  bool skip = (first_version() >= sid);
  lock.Release(&qnode);
  return skip;
}

static uint64_t *EnlargePair64Array(uint64_t *old_p, unsigned int old_cap, int old_regionid,
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
  mem::GetDataRegion().Free(old_p, old_regionid, 2 * old_len);
  return new_p;
}

void SortedArrayVHandle::EnsureSpace()
{
  if (unlikely(size >= capacity)) {
    auto current_regionid = mem::ParallelPool::CurrentAffinity();
    auto new_cap = 1U << (32 - __builtin_clz((unsigned int) size));
    auto new_versions = EnlargePair64Array(versions, capacity, alloc_by_regionid, new_cap);
    if (new_versions == nullptr) {
      logger->critical("Memory allocation failure, last GC {}, second ver epoch {}", last_gc_mark_epoch, versions[1] >> 32);
      std::abort();
    }
    versions = new_versions;
    capacity = new_cap;
    alloc_by_regionid = current_regionid;
  }
}

void SortedArrayVHandle::AppendNewVersionNoLock(uint64_t sid, uint64_t epoch_nr)
{
  // append this version at the end of version array
  IncreaseSize(1);
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

  // We don't need to move the values, because they are all kPendingValue in
  // this epoch anyway. Of course, this is assuming sid will never smaller than
  // the minimum sid of this epoch. In felis, we can assume this. However if we
  // were to replay Ermia, we couldn't.

  util::Instance<GC>().AddVHandle((VHandle *) this, epoch_nr);
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
  versions[i + extra_shift] = last;

  return i;
}

// Insert a new version into the version array, with value pending.
// return value only has meaning for priority txns (bcz it's inserting during execution)
bool SortedArrayVHandle::AppendNewVersion(uint64_t sid, uint64_t epoch_nr, bool priority)
{
  if (likely(!VHandleSyncService::g_lock_elision)) {
    util::MCSSpinLock::QNode qnode;
    VersionBufferHandle handle;

    if (priority) {
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
      return old->AppendNewVersion(sid);
    }

    if (sid == 0) goto slowpath;
    if (!Options::kVHandleBatchAppend) goto slowpath;

    if (buf_pos.load(std::memory_order_acquire) == -1
        && lock.TryLock(&qnode)) {
      AppendNewVersionNoLock(sid, epoch_nr);
      lock.Unlock(&qnode);
      return true;
    }

    handle = util::Instance<BatchAppender>().GetOrInstall((VHandle *) this);
    if (handle.prealloc_ptr) {
      handle.Append((VHandle *) this, sid, epoch_nr);
      return true;
    }

 slowpath:
    lock.Lock(&qnode);
    AppendNewVersionNoLock(sid, epoch_nr);
    lock.Unlock(&qnode);
  } else {
    AppendNewVersionNoLock(sid, epoch_nr);
  }
  return true;
}

volatile uintptr_t *SortedArrayVHandle::WithVersion(uint64_t sid, int &pos)
{
  if (size == 0) return nullptr;

  uint64_t *p = versions;
  uint64_t *start = versions;
  uint64_t *end = versions + size;
  unsigned int latest = latest_version.load();

  if (sid > versions[latest]) {
    start = versions + latest + 1;
    if (start >= versions + size || sid <= *start) {
      p = start;
      goto found;
    }
  } else {
    end = versions + latest + 1;
  }

  p = std::lower_bound(start, end, sid);
  if (p == versions) {
    return nullptr;
  }
found:
  pos = --p - versions;
  auto objects = versions + capacity;
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

  sync().WaitForData(addr, sid, versions[pos], (void *) this);

  // mark read bit
  uintptr_t varstr_ptr = *addr;
  if (PriorityTxnService::g_read_bit && !(varstr_ptr & kReadBitMask)) {
    *addr = varstr_ptr | kReadBitMask;
  }
  varstr_ptr = varstr_ptr & ~kReadBitMask;

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
    return true;

  if (*addr == kPendingValue)
    return false; // if it's not written, it couldn't be read
  return *addr & kReadBitMask;
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
    return util::Instance<PriorityTxnService>().GetMaxProgress();
  if (original == 0) return extra;
  if (extra == 0) return original;
  return (original < extra) ? original : extra;
}

uint64_t SortedArrayVHandle::FindFirstUnreadVersion(uint64_t min)
{
  int lower_pos;
  volatile uintptr_t *ptr_obj = WithVersion(min, lower_pos);
  if (ptr_obj == nullptr)
    return min;

  volatile uintptr_t *ptr_ver = &versions[lower_pos];

  while (*ptr_obj & kReadBitMask) {
    ptr_ver++;
    ptr_obj++;
    uint64_t max_prog = util::Instance<PriorityTxnService>().GetMaxProgress();
    if (ptr_ver - versions >= size) // search out of bound, all versions are read
      return 0;
    if (*ptr_ver >= max_prog)
      return max_prog;
  }

  if (*ptr_ver <= min)
    return min;
  return *ptr_ver;
}

// for priority txn's delete, append new version for delete, and mark versions
// behind it as kDeletePendingValue
bool SortedArrayVHandle::InitDelete(uint64_t sid) {
  if (!AppendNewVersion(sid, sid >> 32, true))
    return false;

  // mark versions in original array
  int pos;
  WithVersion(sid, pos);
  uintptr_t *objects = versions + capacity;
  for (++pos; pos < size; ++pos) {
    if (!sync().IsPendingVal(objects[pos]))
      return false;
    sync().OfferData(objects + pos, (uintptr_t)kDeletePendingValue);
    if (!sync().IsDeletePendingVal(objects[pos]))
      return false;
  }
  // mark versions in extra array
  auto extra = extra_vhandle.load();
  if (extra == nullptr)
    return true;
  return extra->InitDelete(sid);
}

void SortedArrayVHandle::RevertInitDelete(uint64_t sid) {
  int pos;
  WithVersion(sid, pos);
  uintptr_t *objects = versions + capacity;
  for (++pos; pos < size; ++pos) {
    if (sync().IsDeletePendingVal(objects[pos]))
      sync().OfferData(objects + pos, (uintptr_t)kPendingValue);
  }
  auto extra = extra_vhandle.load();
  if (extra)
    extra->RevertInitDelete(sid);
}

void SortedArrayVHandle::PriorityDelete(uint64_t sid) {
  int pos;
  WithVersion(sid, pos);
  uintptr_t *objects = versions + capacity;
  for (++pos; pos < size; ++pos) {
    sync().OfferData(objects + pos, (uintptr_t)nullptr);
  }
  auto extra = extra_vhandle.load();
  if (extra)
    extra->PriorityDelete(sid);
}

bool SortedArrayVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr)
{
  // Finding the exact location
  auto it = std::lower_bound(versions, versions + size, sid);
  if (unlikely(it == versions + size || *it != sid)) {
    auto extra = extra_vhandle.load();
    if (extra) {
      // go to extra array to find version. if still not found, doomed
      if (extra->WriteWithVersion(sid, obj))
        return true;
    }
    // sid is greater than all the versions, or the located lower_bound isn't sid version
    logger->critical("Diverging outcomes on {}! sid {} pos {}/{}", (void *) this,
                     sid, it - versions, size);
    logger->critical("bufpos {}", buf_pos.load());
    std::stringstream ss;
    for (int i = 0; i < size; i++) {
      ss << versions[i] << ' ';
    }
    logger->critical("Versions: {}", ss.str());
    std::abort();
    return false;
  }

  auto objects = versions + capacity;
  volatile uintptr_t *addr = &objects[it - versions];

  // Writing to exact location
  sync().OfferData(addr, (uintptr_t) obj);

  unsigned int ver = latest_version.load();
  unsigned int latest = it - versions;
  while (latest > ver) {
    if (latest_version.compare_exchange_strong(ver, latest))
      break;
  }
  return true;
}

bool SortedArrayVHandle::WriteExactVersion(unsigned int version_idx, VarStr *obj, uint64_t epoch_nr)
{
  abort_if(version_idx >= size, "WriteExactVersion overflowed {} >= {}", version_idx, size);

  volatile uintptr_t *addr = versions + capacity + version_idx;
  // sync().OfferData(addr, (uintptr_t) obj);
  *addr = (uintptr_t) obj;

  unsigned int ver = latest_version.load();
  while (version_idx > ver) {
    if (latest_version.compare_exchange_strong(ver, version_idx))
      break;
  }
  return true;
}

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

void *SortedArrayVHandle::operator new(size_t nr_bytes)
{
  return pool.Alloc();
}

mem::ParallelSlabPool BaseVHandle::pool;

void BaseVHandle::InitPool()
{
  pool = mem::ParallelSlabPool(mem::VhandlePool, 64, 4);
  pool.Register();
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

  // debug(TRACE_PRIORITY "txn sid {} - append on extra Vhandle {:p}, current size {}", sid, (void*)this, size.load());
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
    if (varstr_ptr == kIgnoreValue)
      return handle->ReadWithVersion(ver_extra);
    else if (varstr_ptr == kRetryValue) {
      // debug(TRACE_PRIORITY "RETRY ACTUALLY HAPPENED! sid {} pos {} ver {} old versions {:p}", sid, pos, ver_extra, (void *)versions);
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

bool LinkedListExtraVHandle::AppendNewVersion(uint64_t sid)
{
  Entry *old, *n = new Entry(sid, kPendingValue, mem::ParallelPool::CurrentAffinity());
  bool recycle = false;
  do {
    recycle = false;
    old = head.load();
    if (old && old->version >= sid) {
      delete n;
      return false;
    }
    n->next = old;
    if (old && old->object == kIgnoreValue) {
      // if head is kIgnoreValue, recycle head now
      n->next = old->next;
      recycle = true;
    }
  } while (!head.compare_exchange_strong(old, n));

  if (recycle)
    delete old;
  else
    size++;
  // TODO: GC
  return true;
}

// return: if the version in extra array is closer, the VarStr we read; else, nullptr
VarStr *LinkedListExtraVHandle::ReadWithVersion(uint64_t sid, uint64_t ver, SortedArrayVHandle* handle)
{
  Entry *p = head;
  while (p && p->version >= sid)
    p = p->next;

  if (!p)
    return nullptr;

  auto ver_extra = p->version;
  if (ver_extra < ver)
    return nullptr;
  volatile uintptr_t *addr = &p->object;
  util::Impl<VHandleSyncService>().WaitForData(addr, sid, ver_extra, (void *) this);
  auto varstr_ptr = *addr;
  if (varstr_ptr == kIgnoreValue)
    return handle->ReadWithVersion(ver_extra);

  // mark read bit
  if (PriorityTxnService::g_read_bit && !(varstr_ptr & kReadBitMask)) {
    *addr = varstr_ptr | kReadBitMask;
  }
  varstr_ptr = varstr_ptr & ~kReadBitMask;

  return (VarStr *) varstr_ptr;
}

// bool& is_in:  true if the previous version is indeed in the extra array
// return value: true if read bit is set
bool LinkedListExtraVHandle::CheckReadBit(uint64_t sid, uint64_t ver, SortedArrayVHandle* handle, bool& is_in) {
  abort_if(!PriorityTxnService::g_read_bit, "ExtraVHandle CheckReadBit() is called when read bit is off");
  is_in = false;
  Entry *p = head;
  while (p && p->version >= sid)
    p = p->next;

  if (!p)
    return false;

  auto ver_extra = p->version;
  if (ver_extra < ver)
    return false;

  is_in = true;
  auto varstr_ptr = p->object;
  if (varstr_ptr == kPendingValue)
    return false; // if it's not written, it couldn't be read
  if (varstr_ptr == kIgnoreValue)
    return handle->CheckReadBit(ver_extra);
  return varstr_ptr & kReadBitMask;
}

bool LinkedListExtraVHandle::InitDelete(uint64_t sid) {
  // mark versions in extra versions
  Entry *p = head;
  while (p && p->version > sid) {
    if (!util::Impl<VHandleSyncService>().IsPendingVal(p->object))
      return false;
    util::Impl<VHandleSyncService>().OfferData(&p->object, (uintptr_t)kDeletePendingValue);
    if (!util::Impl<VHandleSyncService>().IsDeletePendingVal(p->object))
      return false;
  }
  return true;
}

void LinkedListExtraVHandle::RevertInitDelete(uint64_t sid) {
  Entry *p = head;
  while (p && p->version > sid) {
    if (util::Impl<VHandleSyncService>().IsDeletePendingVal(p->object))
      util::Impl<VHandleSyncService>().OfferData(&p->object, (uintptr_t)kDeletePendingValue);
  }
}

void LinkedListExtraVHandle::PriorityDelete(uint64_t sid) {
  Entry *p = head;
  while (p && p->version > sid) {
    if (util::Impl<VHandleSyncService>().IsDeletePendingVal(p->object))
      util::Impl<VHandleSyncService>().OfferData(&p->object, (uintptr_t)nullptr);
  }
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
    if (cur != &dummy && cur->version <= min)
      return min; // check this before moving the ptr!!!
    cur = cur->next;
  }
  if (cur == &dummy)
    return 0;
  return cur->version;
}

uint64_t LinkedListExtraVHandle::FindFirstUnreadVersion(uint64_t min)
{
  Entry dummy(0, 0, 0);
  dummy.next = head;
  Entry *cur = &dummy;
  uint64_t last_unread = ~0; // last unread SID in the linked list
  while (cur->next && cur->next->version >= min) {
    cur = cur->next;
    if (!(cur->object & kReadBitMask)) {
      abort_if(last_unread < cur->version, "what? last_unread < cur->version");
      last_unread = cur->version;
    }
  }
  if (cur == &dummy)
    return 0;
  uint64_t max_prog = util::Instance<PriorityTxnService>().GetMaxProgress();
  if (last_unread >= max_prog)
    return max_prog;
  return last_unread;
}

bool LinkedListExtraVHandle::WriteWithVersion(uint64_t sid, VarStr *obj)
{
  Entry *p = head;
  while (p && p->version != sid)
    p = p->next;

  if (!p) {
    logger->critical("Diverging outcomes! sid {}", sid);
    std::stringstream ss;
    Entry *p = head;
    while (p) {
      ss << "{" << std::dec << p->version << "(hex" << std::hex << p->version <<"), 0x" << std::hex << p->object << "}->";
      p = p->next;
    }
    logger->critical("Extra Linked list: {}nullptr", ss.str());
    return false;
  }

  volatile uintptr_t *addr = &p->object;
  util::Impl<VHandleSyncService>().OfferData(addr, (uintptr_t) obj);
  return true;
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
