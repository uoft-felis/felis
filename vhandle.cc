#include <cstdlib>
#include <cstdint>

#include "util.h"
#include "log.h"
#include "vhandle.h"

namespace dolly {

SortedArrayVHandle::SortedArrayVHandle()
    : lock(false)
{
  capacity = 4;
  size = 0;
  this_coreid = alloc_by_coreid = mem::CurrentAllocAffinity();

  // uint8_t *p = (uint8_t *) malloc(2 * len);
  versions = (uint64_t *) mem::GetThreadLocalRegion(alloc_by_coreid).Alloc(2 * capacity * sizeof(uint64_t));
  objects = versions + capacity;
}

static void EnlargePair64Array(uint64_t *old_p, size_t old_cap, int old_coreid,
			       uint64_t *&new_p, size_t &new_cap, int new_coreid)
{
  new_cap = 2 * old_cap;
  const size_t old_len = old_cap * sizeof(uint64_t);
  const size_t new_len = new_cap * sizeof(uint64_t);
  auto &reg = mem::GetThreadLocalRegion(new_coreid);
  auto &old_reg = mem::GetThreadLocalRegion(old_coreid);

  new_p = (uint64_t *) reg.Alloc(2 * new_len);
  memcpy(new_p, old_p, old_cap * sizeof(uint64_t));
  memcpy((uint8_t *) new_p + new_len, (uint8_t *) old_p + old_len, old_cap * sizeof(uint64_t));
  old_reg.Free(old_p, 2 * old_len);
}

void SortedArrayVHandle::EnsureSpace()
{
  if (unlikely(size == capacity)) {
    auto current_coreid = mem::CurrentAllocAffinity();
    EnlargePair64Array(versions, capacity, alloc_by_coreid,
		       versions, capacity, current_coreid);
    alloc_by_coreid = current_coreid;
    objects = (uintptr_t *) versions + capacity;
  }
}

bool SortedArrayVHandle::AppendNewVersion(uint64_t sid)
{
  bool expected = false;
  if (!lock.compare_exchange_strong(expected, true)) {
    return false;
  }
  gc_rule(*this, sid);

  size++;
  EnsureSpace();
  versions[size - 1] = sid;
  objects[size - 1] = kPendingValue;

  // now we need to swap backwards... hope this won't take too long...
  // ERMIA 2.0 guarantee there is no duplicate write set keys now
  // TODO: replace this with a cleverer binary search if matters
  uint64_t last = versions[size - 1];
  int i = size - 1;
  while (i > 0 && versions[i - 1] > last) i--;

  memmove(&versions[i + 1], &versions[i], sizeof(uint64_t) * (size - i - 1));
  versions[i] = last;
  memmove(&objects[i + 1], &objects[i], sizeof(uintptr_t) * (size - i - 1));
  objects[i] = kPendingValue;

  lock.store(false);
  return true;
}

volatile uintptr_t *SortedArrayVHandle::WithVersion(uint64_t sid, int &pos)
{
  assert(size > 0);
  __builtin_prefetch(versions);

  auto it = std::lower_bound(versions, versions + size, sid);
  if (it == versions) {
    // it's likely a read-your-own-insert happened here.
    // it should be served from the CommitBuffer.
    // if not in the CommitBuffer (Get() shouldn't lead you here, but Scan() could),
    // we should return as if this record is deleted
    return nullptr;
  }
  pos = --it - versions;
  return &objects[pos];
}

// Spinner Slots Because we're using a coroutine library, so our coroutines are
// not preemptive.  When a coroutine needs to wait for some condition, it just
// spins on this per-cpu spinner slot rather than the condition.
//
// This also requires the condition notifier be able to aware of that. What we
// can do for our sorted versioning is to to store a bitmap in the magic number
// count. This limits us to *63 cores* at maximum though. Exceeding this limit
// might lead us to use share spinner slots.

class SpinnerSlot {
  static const int kNrSpinners = 32;
  struct {
    volatile long done;
    long __padding__[7];
  } slots[kNrSpinners];
 public:
  SpinnerSlot() { memset(slots, 0, 64 * kNrSpinners); }

  void Wait();
  void NotifyAll(uint64_t bitmap);
};


void SpinnerSlot::Wait()
{
  int idx = go::Scheduler::CurrentThreadPoolId() - 1;
  if (unlikely(idx < 0)) {
    std::abort();
  }

  long dt = 0;
  while (!slots[idx].done) {
    asm("pause" : : :"memory");
    dt++;
  }

  asm volatile("" : : :"memory");

  DTRACE_PROBE1(dolly, wait_jiffies, dt);
  slots[idx].done = 0;
}

void SpinnerSlot::NotifyAll(uint64_t bitmap)
{
  while (bitmap) {
    int idx = __builtin_ctzll(bitmap);
    slots[idx].done = 1;
    bitmap &= ~(1 << idx);
  }
}

static SpinnerSlot gSpinnerSlots;

static bool IsPendingVal(uintptr_t val)
{
  if ((val >> 32) == (kPendingValue >> 32))
    return true;
  return false;
}

static void WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver, void *handle)
{
  DTRACE_PROBE1(dolly, version_read, handle);

  uintptr_t oldval = *addr;
  if (!IsPendingVal(oldval)) return;
  DTRACE_PROBE1(dolly, blocking_version_read, handle);

  int core = go::Scheduler::CurrentThreadPoolId() - 1;

  while (true) {
    uintptr_t val = oldval;
    uint64_t mask = 1ULL << core;
    uintptr_t newval = val & ~mask;
    oldval = __sync_val_compare_and_swap(addr, val, newval);
    if (oldval == val) {
      gSpinnerSlots.Wait();
      oldval = *addr;
    }
    if (!IsPendingVal(oldval))
      return;
  }
}

VarStr *SortedArrayVHandle::ReadWithVersion(uint64_t sid)
{
  // if (versions.size() > 0) assert(versions[0] == 0);
  int pos;
  volatile uintptr_t *addr = WithVersion(sid, pos);
  if (!addr) return nullptr;

  WaitForData(addr, sid, versions[pos], (void *) this);

  return (VarStr *) *addr;
}

bool SortedArrayVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, bool dry_run)
{
  assert(this);
  // Writing to exact location
  auto it = std::lower_bound(versions, versions + size, sid);
  if (it == versions + size || *it != sid) {
    logger->critical("Diverging outcomes! sid {} pos {}/{}", sid, it - versions, size);
    std::stringstream ss;
    for (int i = 0; i < size; i++) {
      ss << versions[i] << ' ';
    }
    logger->critical("Versions: {}", ss.str());
    throw DivergentOutputException();
  }
  if (!dry_run) {
    volatile uintptr_t *addr = &objects[it - versions];
    uintptr_t oldval = *addr;
    uintptr_t newval = (uintptr_t) obj;

    // installing newval
    while (true) {
      uintptr_t val = __sync_val_compare_and_swap(addr, oldval, newval);
      if (val == oldval) break;
      oldval = val;
    }

    // need to notify according to the bitmaps, which is oldval
    uint64_t hibitmap = ~(oldval << 32);
    gSpinnerSlots.NotifyAll(hibitmap >> 32);

    if (obj == nullptr && it - versions == size - 1) {
      return false;
    }
  }
  return true;
}

void SortedArrayVHandle::GarbageCollect()
{
  value_mark = size;
  if (size < 2) return;

  for (int i = 0; i < size; i++) {
    if (versions[i] < gc_rule.min_of_epoch) {
      VarStr *o = (VarStr *) objects[i];
      delete o;
    } else {
      assert(versions[i] == gc_rule.min_of_epoch);
      memmove(&versions[0], &versions[i], sizeof(int64_t) * (size - i));
      memmove(&objects[0], &objects[i], sizeof(uintptr_t) * (size - i));
      size -= i;
      return;
    }
  }
}

mem::Pool<true> *BaseVHandle::pools;

static mem::Pool<true> *InitPerCorePool(size_t ele_size, size_t nr_ele)
{
  auto pools = (mem::Pool<true> *) malloc(sizeof(mem::Pool<true>) * Epoch::kNrThreads);
  for (int i = 0; i < Epoch::kNrThreads; i++) {
    new (&pools[i]) mem::Pool<true>(ele_size, nr_ele, i / mem::kNrCorePerNode);
  }
  return pools;
}

void BaseVHandle::InitPools()
{
  pools = InitPerCorePool(64, 16 << 20);
}

LinkListVHandle::LinkListVHandle()
    : this_coreid(mem::CurrentAllocAffinity()), lock(false), head(nullptr), size(0)
{
}

mem::Pool<true> *LinkListVHandle::Entry::pools;

void LinkListVHandle::Entry::InitPools()
{
  pools = InitPerCorePool(32, 16 << 20);
}

bool LinkListVHandle::AppendNewVersion(uint64_t sid)
{
  bool expected = false;
  if (!lock.compare_exchange_strong(expected, true)) {
    return false;
  }

  gc_rule(*this, sid);

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

  DTRACE_PROBE2(dolly, linklist_search_read, search_count, size);

  if (!p) return nullptr;

  volatile uintptr_t *addr = &p->object;
  WaitForData(addr, sid, p->version, (void *) this);
  return (VarStr *) *addr;
}

bool LinkListVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, bool dry_run)
{
  assert(this);
  Entry *p = head;
  int search_count = 1;
  while (p && p->version != sid) {
    search_count++;
    p = p->next;
  }
  DTRACE_PROBE2(dolly, linklist_search_write, search_count, size);
  if (!p) {
    logger->critical("Diverging outcomes! sid {}", sid);
    throw DivergentOutputException();
  }
  if (!dry_run) {
    volatile uintptr_t *addr = &p->object;
    *addr = (uintptr_t) obj;
    if (obj == nullptr && p->next == nullptr) {
      return false;
    }
  }
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

bool CalvinVHandle::AppendNewVersion(uint64_t sid)
{
  return AppendNewAccess(sid);
}

bool CalvinVHandle::AppendNewAccess(uint64_t sid, bool is_read)
{
  bool expected = false;
  if (!lock.compare_exchange_strong(expected, true))
    return false;

  gc_rule(*this, sid);

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
  if (pos.load(std::memory_order_acquire) >= size) std::abort();
  while (true) {
    uint64_t turn = accesses[pos.load(std::memory_order_acquire)];
    if ((turn >> 1) == sid) {
      return turn;
    }
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
  if ((accesses[0] >> 1) < sid) {
    std::abort();
  }
  return false;
}

bool CalvinVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, bool dry_run)
{
  uint64_t turn = WaitForTurn(sid);

  if (!dry_run) {
    delete this->obj;
    this->obj = obj;
    bool is_last = (obj == nullptr && pos.load(std::memory_order_acquire) == size - 1);
    pos.fetch_add(1, std::memory_order_release);
    return !is_last;
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
  if ((turn & 0x01) == 0) {
    // I only need to read this record, so advance the pos. Otherwise, I do not
    // need to advance the pos, because I will write to this later.
    pos.fetch_add(1, std::memory_order_release);
  }
  return o;
}

VarStr *CalvinVHandle::DirectRead()
{
  return obj;
}

void CalvinVHandle::GarbageCollect()
{
  if (size < 2) return;

  auto it = std::lower_bound(accesses, accesses + size, gc_rule.min_of_epoch);
  memmove(accesses, it, (it - accesses) * sizeof(uint64_t));
  size -= it - accesses;
  pos.fetch_sub(it - accesses, std::memory_order_release);
  return;
}

}
