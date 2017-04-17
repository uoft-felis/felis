#include <fstream>
#include <limits>
#include <cstdlib>
#include <streambuf>
#include <iomanip>
#include <dlfcn.h>
#include "index.h"
#include "epoch.h"
#include "util.h"
#include "mem.h"
#include "json11/json11.hpp"
#include "gopp/gopp.h"

using util::Instance;

// export global variables
namespace util {

template <>
dolly::RelationManager &Instance()
{
  static dolly::RelationManager mgr;
  return mgr;
}

}

namespace dolly {

std::atomic<unsigned long> TxnValidator::tot_validated;

RelationManagerBase::RelationManagerBase()
{
  std::string err;
  std::ifstream fin("relation_map.json");

  // Wow...I thought I will never encounter most-vexing parse for the rest of my
  // life....and today, I encountered two of them on Clang!
  // - Mike
  std::string conf_text {
    std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>() };

  json11::Json conf_doc = json11::Json::parse(conf_text, err);
  if (!err.empty()) {
    logger->critical(err);
    logger->critical("Cannot read relation id map configuration!");
    std::abort();
  }

  auto json_map = conf_doc.object_items();
  for (auto it = json_map.begin(); it != json_map.end(); ++it) {
    // assume it->second is int!
    // TODO: validations!
    relation_id_map[it->first] = it->second.int_value();
    logger->info("relation name {} map to id {}", it->first,
		 relation_id_map[it->first]);
  }
}

void TxnValidator::CaptureWrite(const Txn &tx, int fid, const VarStr *k, VarStr *obj)
{
#ifdef VALIDATE_TXN
  if (k)
    update_crc32(k->data, k->len, &key_crc);

  TxnKey *kptr = (TxnKey *) keys_ptr;
  if (!k || kptr->fid != fid || kptr->len != k->len
      || memcmp(kptr->data, k->data, k->len) != 0) {
    is_valid = false;
    logger->alert("Out-of-Order Write. sid {} fid {}",
		  tx.serializable_id(), kptr->fid);
    VarStr real_k;
    real_k.data = kptr->data;
    real_k.len = kptr->len;

    DebugVarStr("Expected Key", &real_k);
    DebugVarStr("Actual Key", k);
  }

  keys_ptr += sizeof(TxnKey) + kptr->len;

  unsigned int value_crc = INITIAL_CRC32_VALUE;
  if (obj != nullptr) {
    update_crc32(obj->data, obj->len, &value_crc);
    value_size += obj->len;
  }

  if (value_crc != *(unsigned int *) keys_ptr) {
    is_valid = false;
    logger->alert("value csum mismatch, type {:d} sid {} fid {}, {} should be {}",
		  tx.type, tx.serializable_id(), fid,
		  value_crc, *(unsigned int *) keys_ptr);
    std::stringstream prefix;

    prefix << "Key sid " << tx.serializable_id() << " ";
    DebugVarStr(prefix.str().c_str(), k);

    prefix.str(std::string());
    prefix << "Actual Value sid " << tx.serializable_id() << " ";
    DebugVarStr(prefix.str().c_str(), obj);
  }
  keys_ptr += 4;
#endif
}

void TxnValidator::DebugVarStr(const char *prefix, const VarStr *s)
{
  if (!s) {
    logger->alert("{}: null", prefix);
    return;
  }

  std::stringstream ss;
  ss << std::hex << std::setfill('0') << std::setw(2);
  for (int i = 0; i < s->len; i++) {
    ss << "0x" << (int) s->data[i] << ' ';
  }
  logger->alert("{}: {}", prefix, ss.str());
}

void TxnValidator::Validate(const Txn &tx)
{
#ifdef VALIDATE_TXN
  while (keys_ptr != tx.key_buffer() + tx.key_buffer_size()) {
    logger->alert("left over keys!");
    CaptureWrite(tx, -1, nullptr, nullptr);
  }
  if (is_valid) {
    logger->debug("txn sid {} valid! Total {} txns data size {} bytes",
		  tx.serializable_id(), tot_validated.fetch_add(1), value_size);
  } else {
    logger->alert("txn sid {} invalid!", tx.serializable_id());
  }
#endif
}

static DeletedGarbageHeads gDeletedGarbage;

DeletedGarbageHeads::DeletedGarbageHeads()
{
  for (int i = 0; i < NR_THREADS; i++) {
    garbage_heads[i].Initialize();
  }
}

#define PROACTIVE_GC

void DeletedGarbageHeads::AttachGarbage(CommitBufferEntry *g)
{
#ifdef PROACTIVE_GC
  int idx = go::Scheduler::CurrentThreadPoolId() - 1;
  g->lru_node.InsertAfter(&garbage_heads[idx]);
#else
  delete g->key;
  delete g;
#endif
}

void DeletedGarbageHeads::CollectGarbage(uint64_t epoch_nr)
{
#ifdef PROACTIVE_GC
  int idx = go::Scheduler::CurrentThreadPoolId() - 1;
  ListNode *head = &garbage_heads[idx];
  ListNode *ent = head->prev;
  size_t gc_count = 0;
  auto &mgr = Instance<RelationManager>();
  while (ent != head) {
    auto prev = ent->prev;
    CommitBufferEntry *entry = container_of(ent, CommitBufferEntry, lru_node);
    if (epoch_nr - entry->epoch_nr < 2)
      break;
    mgr.GetRelationOrCreate(entry->fid).ImmediateDelete(entry->key);
    gc_count++;
    ent->Remove();
    delete entry->key;
    delete entry;

    ent = prev;
  }
  DTRACE_PROBE1(dolly, deleted_gc_per_core, gc_count);
  logger->info("Proactive GC {} cleaned {} garbage keys", idx, gc_count);
#endif
}

void CommitBuffer::Put(int fid, const VarStr *key, VarStr *obj)
{
  unsigned int h = Hash(fid, key);
  ListNode *head = &htable[h % kHashTableSize];
  ListNode *node = head->next;
  while (node != head) {
    auto entry = container_of(node, CommitBufferEntry, ht_node);
    if (entry->fid != fid)
      goto next;
    if (*entry->key != *key)
      goto next;

    // update this node
    delete entry->key;
    delete entry->obj;

    entry->key = key;
    entry->obj = obj;
    // entry->lru_node.Remove();
    // entry->lru_node.InsertAfter(&lru);
    return;
  next:
    node = node->next;
  }
  auto entry = new CommitBufferEntry(fid, key, obj);
  entry->ht_node.InsertAfter(head);
  entry->lru_node.InsertAfter(&lru);
}

VarStr *CommitBuffer::Get(int fid, const VarStr *key)
{
  unsigned int h = Hash(fid, key);
  ListNode *head = &htable[h % kHashTableSize];
  ListNode *node = head->next;
  while (node != head) {
    auto entry = container_of(node, CommitBufferEntry, ht_node);
    if (entry->fid != fid)
      goto next;
    if (*entry->key != *key)
      goto next;

    return entry->obj;
  next:
    node = node->next;
  }
  return nullptr;
}

void CommitBuffer::Commit(uint64_t sid, TxnValidator *validator)
{
  ListNode *head = &lru;
  ListNode *node = head->prev;
  auto &mgr = Instance<RelationManager>();

  if (validator)
    validator->set_keys_ptr(tx->key_buffer());
  while (node != head) {
    ListNode *prev = node->prev;
    auto entry = container_of(node, CommitBufferEntry, lru_node);
    bool is_garbage;

    if (validator)
      validator->CaptureWrite(*tx, entry->fid, entry->key, entry->obj);
    try {
      is_garbage = not mgr.GetRelationOrCreate(entry->fid).CommitPut(entry->key, sid, entry->obj);
    } catch (...) {
      logger->critical("Error during commit key {}", entry->key->ToHex().c_str());
      throw DivergentOutputException();
    }

    if (!is_garbage) {
      delete entry->key;
      delete entry;
    } else {
      gDeletedGarbage.AttachGarbage(entry);
    }
    node = prev;
  }

  if (validator)
    validator->Validate(*tx);
}

typedef Checkpoint* (*InitChkptFunc)(void);

Checkpoint *Checkpoint::LoadCheckpointImpl(const std::string &filename)
{
  void *handle = dlopen(filename.c_str(), RTLD_LAZY);
  InitChkptFunc func = (InitChkptFunc) dlsym(handle, "InitializeChkpt");
  return func();
}

SortedArrayVHandle::SortedArrayVHandle()
  : lock(false)
{
  capacity = 4;
  size = 0;

  const size_t len = capacity * sizeof(uint64_t);
  this_coreid = alloc_by_coreid = mem::CurrentAllocAffinity();

  // uint8_t *p = (uint8_t *) malloc(2 * len);
  uint8_t *p = (uint8_t *) mem::GetThreadLocalRegion(alloc_by_coreid).Alloc(2 * len);

  versions = (uint64_t *) p;
  objects = (uintptr_t *) (p + len);

  // slots = (TxnWaitSlot *) mem::GetThreadLocalRegion(alloc_by_coreid)
  // .Alloc(sizeof(TxnWaitSlot) * capacity);
}

void SortedArrayVHandle::EnsureSpace()
{
  if (unlikely(size == capacity)) {
    capacity *= 2;
    const size_t len = capacity * sizeof(uint64_t);
    auto old_id = alloc_by_coreid;
    void *old_p = versions;
    uint8_t *p = nullptr;

    alloc_by_coreid = mem::CurrentAllocAffinity();

    auto &reg = mem::GetThreadLocalRegion(alloc_by_coreid);
    auto &old_reg = mem::GetThreadLocalRegion(old_id);

    // uint8_t *p = (uint8_t *) malloc(2 * len);
    p = (uint8_t *) reg.Alloc(2 * len);

    memcpy(p, versions, len / 2);
    memcpy(p + len, objects, len / 2);

    versions = (uint64_t *) p;
    objects = (uintptr_t *) (p + len);

    old_reg.Free(old_p, len);

    // nobody's waiting on the slots right now anyway!
    // old_reg.Free(slots, capacity / 2 * sizeof(TxnWaitSlot));
    // slots = (TxnWaitSlot *) reg.Alloc(capacity * sizeof(TxnWaitSlot));
    // free(old_p);

    // but we need to initialize them all because somebody need to use that
    // any time later
    // for (int i = 0; i < capacity; i++) {
    // new (&slots[i]) TxnWaitSlot();
    // }
  }
}

void SortedArrayVHandle::AppendNewVersion(uint64_t sid)
{
  bool expected = false;
  while (!lock.compare_exchange_weak(expected, true,
				     std::memory_order_release,
				     std::memory_order_relaxed)) {
    expected = false;
    asm("pause" : : :"memory");
  }
  gc_rule(*this, sid);

  size++;
  EnsureSpace();
  versions[size - 1] = sid;
  objects[size - 1] = PENDING_VALUE;

  // now we need to swap backwards... hope this won't take too long...
  // TODO: replace this with a cleverer binary search if matters
  uint64_t last = versions[size - 1];
  for (int i = size - 1; i >= 0; i--) {
    if (i > 0 && versions[i - 1] == last) {
      size--;
      goto done_sort; // duplicates!
    } else if (i == 0 || versions[i - 1] < last) {
      memmove(&versions[i + 1], &versions[i], sizeof(uint64_t) * (size - i - 1));
      versions[i] = last;
      memmove(&objects[i + 1], &objects[i], sizeof(uintptr_t) * (size - i - 1));
      objects[i] = PENDING_VALUE;
      goto done_sort;
    } else {
      // The following assertion means future epoch cannot commit back in time.
      // It actually could!
      // assert(objects[i - 1] == PENDING_VALUE);
    }
  }
done_sort:
  lock.store(false);
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

static void WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver, void *handle)
{
  DTRACE_PROBE1(dolly, version_read, handle);
  if (*addr != PENDING_VALUE) return;

  DTRACE_PROBE1(dolly, blocking_version_read, handle);
  static const uint64_t kDeadlockThreshold = 6400000000;
  uint64_t dt = 0;
  while ((dt++) < kDeadlockThreshold) {
    if (*addr != PENDING_VALUE) {
      DTRACE_PROBE2(dolly, wait_jiffies, handle, dt - 1);
      return;
    }
    asm("pause" : : :"memory");
  }
  // Deadlocked?
  fprintf(stderr, "core %d deadlock detected 0x%lx wait for 0x%lx\n",
	  go::Scheduler::CurrentThreadPoolId(),
	  sid, ver);
  sleep(32);
  std::abort();
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
    *addr = (uintptr_t) obj;

    if (obj == nullptr && it - versions == size - 1) {
      return false;
    }
  }
  return true;
}

void SortedArrayVHandle::GarbageCollect()
{
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

void LinkListVHandle::AppendNewVersion(uint64_t sid)
{
  bool expected = false;
  while (!lock.compare_exchange_weak(expected, true, std::memory_order_release, std::memory_order_relaxed)) {
    expected = false;
    asm("pause" ::: "memory");
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
  n = new Entry {cur, sid, PENDING_VALUE, mem::CurrentAllocAffinity()};
  *p = n;
  size++;
done:
  lock.store(false);
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

}

namespace util {

template <>
dolly::DeletedGarbageHeads &Instance()
{
  return dolly::gDeletedGarbage;
}

}
