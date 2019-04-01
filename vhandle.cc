#include <cstdlib>
#include <cstdint>

#include "util.h"
#include "log.h"
#include "vhandle.h"
#include "node_config.h"

namespace felis {

VHandleSyncService &BaseVHandle::sync()
{
  return util::Impl<VHandleSyncService>();
}

SortedArrayVHandle::SortedArrayVHandle()
    : lock(false)
{
  capacity = 4;
  // value_mark = 0;
  size = 0;
  this_coreid = go::Scheduler::CurrentThreadPoolId() - 1;
  alloc_by_regionid = mem::CurrentAllocAffinity();

  versions = (uint64_t *) mem::GetThreadLocalRegion(alloc_by_regionid).Alloc(2 * capacity * sizeof(uint64_t));
  latest_version.store(0);
}

static void EnlargePair64Array(uint64_t *old_p, uint old_cap, int old_regionid,
			       uint64_t *&new_p, uint &new_cap, int new_regionid)
{
  new_cap = 2 * old_cap;
  const size_t old_len = old_cap * sizeof(uint64_t);
  const size_t new_len = new_cap * sizeof(uint64_t);
  auto &reg = mem::GetThreadLocalRegion(new_regionid);
  auto &old_reg = mem::GetThreadLocalRegion(old_regionid);

  new_p = (uint64_t *) reg.Alloc(2 * new_len);
  memcpy(new_p, old_p, old_cap * sizeof(uint64_t));
  memcpy((uint8_t *) new_p + new_len, (uint8_t *) old_p + old_len, old_cap * sizeof(uint64_t));
  old_reg.Free(old_p, 2 * old_len);
}

void SortedArrayVHandle::EnsureSpace()
{
  if (unlikely(size == capacity)) {
    auto current_regionid = mem::CurrentAllocAffinity();
    EnlargePair64Array(versions, capacity, alloc_by_regionid,
		       versions, capacity, current_regionid);
    alloc_by_regionid = current_regionid;
  }
}

// Insert a new version into the version array, with value pending.
bool SortedArrayVHandle::AppendNewVersion(uint64_t sid, uint64_t epoch_nr)
{
  bool expected = false;
  if (!lock.compare_exchange_strong(expected, true)) {
    return false;
  }
  gc_rule((VHandle *) this, sid, epoch_nr);

  // append this version at the end of version array
  size++;
  EnsureSpace();
  versions[size - 1] = sid;
  auto objects = versions + capacity;
  objects[size - 1] = kPendingValue;

  // find the location this version is supposed to be
  uint64_t last = versions[size - 1];
  int i = std::lower_bound(versions, versions + size - 1, last) - versions;

  // move versions by 1 to make room, and put this version back
  memmove(&versions[i + 1], &versions[i], sizeof(uint64_t) * (size - i - 1));
  versions[i] = last;

  // We don't need to move the values, because they are all kPendingValue in
  // this epoch anyway. Of course, this is assuming sid will never smaller than
  // the minimum sid of this epoch. In felis, we can assume this. However if we
  // were to replay Ermia, we couldn't.

  lock.store(false);
  return true;
}

volatile uintptr_t *SortedArrayVHandle::WithVersion(uint64_t sid, int &pos)
{
  assert(size > 0);

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
  int pos;
  volatile uintptr_t *addr = WithVersion(sid, pos);
  if (!addr) return nullptr;

  sync().WaitForData(addr, sid, versions[pos], (void *) this);

  return (VarStr *) *addr;
}

// Read the exact version. version_idx is the version offset in the array, not serial id
VarStr *SortedArrayVHandle::ReadExactVersion(uint64_t version_idx)
{
  assert(size > 0);
  assert(size < capacity);
  assert(version_idx < size);

  auto objects = versions + capacity;
  volatile uintptr_t *addr = &objects[version_idx];
  assert(addr);

  return (VarStr *) *addr;
}

bool SortedArrayVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run)
{
  assert(this);
  // Finding the exact location
  auto it = std::lower_bound(versions, versions + size, sid);
  if (it == versions + size || *it != sid) {
    // sid is greater than all the versions, or the located lower_bound isn't sid version
    logger->critical("Diverging outcomes! sid {} pos {}/{}", sid, it - versions, size);
    std::stringstream ss;
    for (int i = 0; i < size; i++) {
      ss << versions[i] << ' ';
    }
    logger->critical("Versions: {}", ss.str());
    return false;
  }

  if (dry_run) return true;

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

// just append a version, with sid = last sid + 1, and with value.
// therefore this sid makes no sense when it comes to transaction serial id.
void SortedArrayVHandle::WriteNewVersion(uint64_t epoch_nr, VarStr *obj) {
  uint64_t sid;
  while (AppendNewVersion( sid = versions[size - 1] + 1, epoch_nr)) {
    asm("pause" : : :"memory");
  }
  WriteWithVersion(sid, obj, epoch_nr);
}

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

static thread_local int g_vhandle_alloc_aff = -1;

void *SortedArrayVHandle::operator new(size_t nr_bytes)
{
  auto aff = g_vhandle_alloc_aff;
  if (aff == -1)
    aff = go::Scheduler::CurrentThreadPoolId() - 1;
  return pools[aff].Alloc();
}

mem::Pool *BaseVHandle::pools;

static mem::Pool *InitPerCorePool(size_t ele_size, size_t nr_ele)
{
  auto pools = (mem::Pool *) malloc(sizeof(mem::Pool) * NodeConfiguration::g_nr_threads);
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto node = (i + NodeConfiguration::g_core_shifting) / mem::kNrCorePerNode;
    pools[i].move(mem::BasicPool(mem::VhandlePool, ele_size, nr_ele, node));
  }
  return pools;
}

void BaseVHandle::InitPools()
{
  pools = InitPerCorePool(64, 2 << 20);
}

void BaseVHandle::SetAllocAffinity(int aff)
{
  g_vhandle_alloc_aff = aff;
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

  DTRACE_PROBE2(felis, linklist_search_read, search_count, size);

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
  DTRACE_PROBE2(felis, linklist_search_write, search_count, size);
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
