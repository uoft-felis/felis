#include <cstdlib>
#include <cstdint>

#include "util.h"
#include "log.h"
#include "vhandle.h"
#include "node_config.h"
#include "iface.h"

namespace felis {

VHandleSyncService &BaseVHandle::sync()
{
  return util::Impl<VHandleSyncService>();
}

#if 0
mem::Pool *SkipListVHandle::Block::pool;

void SkipListVHandle::Block::InitPool()
{
  pool = new mem::Pool(sizeof(Block), 1 << 30);
}

SkipListVHandle::Block::Block()
{
  min = 0;
  size = 0;
  memset(versions, 0xff, kLimit * sizeof(uint64_t));
  for (int i = 0; i < kLimit; i++) objects[i] = kPendingValue;
  for (int i = 0; i < kLevels; i++) levels[i] = nullptr;
}

bool SkipListVHandle::Block::Add(uint64_t view, uint64_t version)
{
  auto tot_size = size.load();
  do {
    if ((tot_size >> kLimitBits) != (view >> kLimitBits)) {
      // The block must have been splitted after I viewed it. We cannot simply
      // append this version in this block, as it might be larger than the
      // maximum of this block can hold.
      return false;
    }
    if ((tot_size & kSizeMask) == kLimitBits + 1) {
      // Somebody is splitting this block right now.
      return false;
    }
  } while (!size.compare_exchange_strong(tot_size, tot_size + 1));
  int idx = tot_size & kSizeMask;

  if (idx < kLimit) {
    versions[idx] = version;
    return true;
  }

  assert(idx == kLimit);
  // Split this block
  Block *next_block = new Block();
  uint64_t half_size = kLimit / 2;
  std::sort(versions, versions + kLimit);
  memcpy(next_block->versions, versions + half_size, sizeof(uint64_t) * (kLimit - half_size));
  next_block->min = next_block->versions[0];
  next_block->size = kLimit - half_size;
  next_block->levels[0].store(levels[0].load(), std::memory_order_relaxed);

  if (version > next_block->min) {
    next_block->versions[next_block->size] = version;
    next_block->size.fetch_add(1, std::memory_order_relaxed);
  } else {
    versions[half_size] = version;
    half_size++;
  }

  tot_size &= ~kSizeMask;
  tot_size += (1UL << kLimitBits) + half_size;

  levels[0].store(next_block);
  size.store(tot_size);

  return true;
}

SkipListVHandle::Block *SkipListVHandle::Block::Find(uint64_t version, bool inclusive, ulong &iterations)
{
  if (min > version)
    return nullptr;
  if (!inclusive && min == version)
    return nullptr;
  for (int i = kLevels - 1; i >= 0; i--) {
    if (levels[i].load() == nullptr) continue;
    auto next = levels[i].load()->Find(version, inclusive, iterations);
    if (next)
      return next;
  }
  return this;
}

SkipListVHandle::SkipListVHandle()
    : lock(false), alloc_by_coreid(mem::CurrentAllocAffinity()),
      flush_tid(0), size(0), shared_blocks(nullptr)
{
}

#endif

int RowEntity::EncodeIOVec(struct iovec *vec, int max_nr_vec)
{
  if (max_nr_vec < 3)
    return 0;

  vec[0].iov_len = 4;
  vec[0].iov_base = &rel_id;
  vec[1].iov_len = k->len;
  vec[1].iov_base = (void *) k->data;
  ulong n_ver = this->newest_version.load();
  vec[2].iov_len = handle_ptr->ReadWithVersion(n_ver)->len;
  vec[2].iov_base = (void *) handle_ptr->ReadWithVersion(n_ver)->data;

  encoded_len = 4 + k->len + handle_ptr->ReadWithVersion(n_ver)->len;

  shipping_handle()->PrepareSend();

  return 3;
}

RowEntity::RowEntity(int rel_id, VarStr *k, VHandle *handle, int slice_id)
    : rel_id(rel_id), k(k), handle_ptr(handle), shandle(this), slice(slice_id)
{
  handle_ptr->row_entity.reset(this);
}

mem::Pool RowEntity::pool;

void RowEntity::InitPools()
{
  pool.move(mem::Pool(sizeof(RowEntity), 64 << 20));
}

void RowShipmentReceiver::Run()
{
  TBD();
}

SortedArrayVHandle::SortedArrayVHandle()
    : lock(false)
{
  capacity = 4;
  value_mark = size = 0;
  this_coreid = alloc_by_coreid = mem::CurrentAllocAffinity();

  versions = (uint64_t *) mem::GetThreadLocalRegion(alloc_by_coreid).Alloc(3 * capacity * sizeof(uint64_t));
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
  }
}

bool SortedArrayVHandle::AppendNewVersion(uint64_t sid, uint64_t epoch_nr)
{
  bool expected = false;
  if (!lock.compare_exchange_strong(expected, true)) {
    return false;
  }
  gc_rule(*this, sid, epoch_nr);

  size++;
  EnsureSpace();
  versions[size - 1] = sid;
  auto objects = versions + capacity;
  objects[size - 1] = kPendingValue;

  uint64_t last = versions[size - 1];
  int i = std::lower_bound(versions, versions + size - 1, last) - versions;

  memmove(&versions[i + 1], &versions[i], sizeof(uint64_t) * (size - i - 1));
  versions[i] = last;

  if (i < value_mark) {
    value_mark++;
    memmove(&objects[i + 1], &objects[i], sizeof(uintptr_t) * (value_mark - i - 1));
    objects[i] = kPendingValue;
  }

  lock.store(false);
  return true;
}

volatile uintptr_t *SortedArrayVHandle::WithVersion(uint64_t sid, int &pos)
{
  assert(size > 0);
  auto it = std::lower_bound(versions, versions + size, sid);
  if (it == versions) {
    // it's likely a read-your-own-insert happened here.
    // it should be served from the CommitBuffer.
    // if not in the CommitBuffer (Get() shouldn't lead you here, but Scan() could),
    // we should return as if this record is deleted
    return nullptr;
  }
  pos = --it - versions;
  auto objects = versions + capacity;
  return &objects[pos];
}

VarStr *SortedArrayVHandle::ReadWithVersion(uint64_t sid)
{
  // if (versions.size() > 0) assert(versions[0] == 0);
  int pos;
  volatile uintptr_t *addr = WithVersion(sid, pos);
  if (!addr) return nullptr;

  sync().WaitForData(addr, sid, versions[pos], (void *) this);

  return (VarStr *) *addr;
}

bool SortedArrayVHandle::WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run)
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
    return false;
  }

  ulong nversion = row_entity->newest_version.load();
  while (it - versions > nversion) {
    if (row_entity->newest_version.compare_exchange_strong(nversion, it - versions))
      break;
  }

  if (dry_run) return true;

  auto objects = versions + capacity;
  volatile uintptr_t *addr = &objects[it - versions];

  sync().OfferData(addr, (uintptr_t) obj);
  return true;
}

void SortedArrayVHandle::GarbageCollect()
{
  auto objects = versions + capacity;
  if (size < 2) goto done;

  for (int i = 0; i < size; i++) {
    if (versions[i] < gc_rule.min_of_epoch) {
      VarStr *o = (VarStr *) objects[i];
      delete o;
    } else {
      assert(versions[i] == gc_rule.min_of_epoch);
      memmove(&versions[0], &versions[i], sizeof(int64_t) * (size - i));
      memmove(&objects[0], &objects[i], sizeof(uintptr_t) * (size - i));
      size -= i;
      goto done;
    }
  }
done:
  value_mark = size;
  return;
}

mem::Pool *BaseVHandle::pools;

static mem::Pool *InitPerCorePool(size_t ele_size, size_t nr_ele)
{
  auto pools = (mem::Pool *) malloc(sizeof(mem::Pool) * NodeConfiguration::g_nr_threads);
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto node = (i + NodeConfiguration::g_core_shifting) / mem::kNrCorePerNode;
    pools[i].move(mem::Pool(ele_size, nr_ele, node));
  }
  return pools;
}

void BaseVHandle::InitPools()
{
  pools = InitPerCorePool(64, 16 << 20);
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
