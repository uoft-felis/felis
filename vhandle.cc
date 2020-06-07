#include <cstdlib>
#include <cstdint>

#include "util.h"
#include "log.h"
#include "vhandle.h"
#include "node_config.h"
#include "epoch.h"
#include "gc.h"

#include "opts.h"
#include "vhandle_batchappender.h"

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

void SortedArrayVHandle::IncreaseSize(int delta, uint64_t epoch_nr)
{
  if (EpochClient::g_workload_client) {
    auto &lm = EpochClient::g_workload_client->get_execution_locality_manager();
    lm.PlanLoad(this_coreid, (long) delta);
  }

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
        gc.Collect((VHandle *) this, epoch_nr, std::min<size_t>(16_K, latest - 1));
      gc.RemoveRow((VHandle *) this, handle);
      gc_handle.store(0, std::memory_order_relaxed);
    }

    if (!garbage_left && latest >= 0
        && GC::IsDataGarbage((VHandle *) this, (VarStr *) objects[latest]))
      garbage_left = true;

    if (garbage_left)
      gc_handle.store(gc.AddRow((VHandle *) this, epoch_nr), std::memory_order_relaxed);
  }
}

void SortedArrayVHandle::AppendNewVersionNoLock(uint64_t sid, uint64_t epoch_nr, bool is_ondemand_split)
{
  if (is_ondemand_split) nr_ondsplt++;

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
  versions[i + extra_shift] = last;

  return i;
}

// Insert a new version into the version array, with value pending.
void SortedArrayVHandle::AppendNewVersion(uint64_t sid, uint64_t epoch_nr, bool is_ondemand_split)
{
  probes::VHandleAppend{this, sid, alloc_by_regionid}();

  if (likely(!VHandleSyncService::g_lock_elision)) {
    util::MCSSpinLock::QNode qnode;
    VersionBufferHandle handle;

    if (sid == 0) goto slowpath;
    if (!Options::kVHandleBatchAppend) goto slowpath;

    if (buf_pos.load(std::memory_order_acquire) == -1
        && size - cur_start < EpochClient::g_splitting_threshold
        && lock.TryLock(&qnode)) {
      AppendNewVersionNoLock(sid, epoch_nr, is_ondemand_split);
      lock.Unlock(&qnode);
      return;
    }

    handle = util::Instance<BatchAppender>().GetOrInstall((VHandle *) this);
    if (handle.prealloc_ptr) {
      handle.Append((VHandle *) this, sid, epoch_nr, is_ondemand_split);
      return;
    }

 slowpath:
    lock.Lock(&qnode);
    AppendNewVersionNoLock(sid, epoch_nr, is_ondemand_split);
    lock.Unlock(&qnode);
  } else {
    AppendNewVersionNoLock(sid, epoch_nr, is_ondemand_split);
  }
}

volatile uintptr_t *SortedArrayVHandle::WithVersion(uint64_t sid, int &pos)
{
  assert(size > 0);

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
    logger->critical("ReadWithVersion() {} cannot found for sid {} start is {} begin is {}",
                     (void *) this, sid, *start, *versions);
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
  int pos;
  volatile uintptr_t *addr = WithVersion(sid, pos);
  if (!addr) return nullptr;

  sync().WaitForData(addr, sid, versions[pos], (void *) this);

  return (VarStr *) *addr;
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

  return (VarStr *) *addr;
}

bool SortedArrayVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr)
{
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
mem::ParallelSlabPool BaseVHandle::inline_pool;

void BaseVHandle::InitPool()
{
  pool = mem::ParallelSlabPool(mem::VhandlePool, 128, 4);
  inline_pool = mem::ParallelSlabPool(mem::VhandlePool, 256, 4);
  pool.Register();
  inline_pool.Register();
}

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
