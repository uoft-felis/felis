#include <fstream>
#include <streambuf>
#include "index.h"
#include "epoch.h"
#include "util.h"
#include "mem.h"
#include "json11/json11.hpp"
#include "goplusplus/gopp.h"

// #define VALIDATE_TXN 1

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

void TxnValidator::CaptureWrite(const VarStr *k, VarStr *obj)
{
#ifdef VALIDATE_TXN
  update_crc32(k->data, k->len, &key_crc);

  if (obj != nullptr) {
    // size_t dummy_size = obj->len;
    // update_crc32(&dummy_size, sizeof(size_t), &value_crc);
    update_crc32(obj->data, obj->len, &value_crc);
    data.push_back(std::vector<uint8_t>{obj->data, obj->data + obj->len});
    value_size += obj->len;
  }
#endif
}

void TxnValidator::Validate(const Txn &tx)
{
#ifdef VALIDATE_TXN
  if (tx.value_checksum() != value_crc) {
    logger->alert("value csum mismatch, type {:d}", tx.type);
    logger->alert("tx csum 0x{:x}, result csum 0x{:x}. Dumping:",
		  tx.value_checksum(), value_crc);
    for (auto line_data: data) {
      std::string str;
      for (auto ch: line_data) {
	char buf[1024];
	snprintf(buf, 1024, "0x%.2x ", ch);
	str.append(buf);
      }
      logger->alert(str);
    }
    sleep(1);
    std::abort();
  }
  logger->debug("txn sid {} valid! Total {} txns data size {} bytes",
		tx.serializable_id(), tot_validated.fetch_add(1), value_size);
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
    entry->lru_node.Remove();
    entry->lru_node.InsertAfter(&lru);
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

  while (node != head) {
    ListNode *prev = node->prev;
    auto entry = container_of(node, CommitBufferEntry, lru_node);

    if (validator)
      validator->CaptureWrite(entry->key, entry->obj);
    mgr.GetRelationOrCreate(entry->fid).CommitPut(entry->key, sid, entry->obj);

    delete entry->key;
    delete entry;
    node = prev;
  }

  if (validator)
    validator->Validate(*tx);
}

const int SortedArrayVHandle::kMaxRetry;

SortedArrayVHandle::SortedArrayVHandle()
  : locked(false), last_gc_epoch(Epoch::CurrentEpochNumber())
{
  capacity = 4;
  size = 0;

  const size_t len = capacity * sizeof(uint64_t);
  alloc_by_coreid = mem::CurrentAllocAffinity();

  // uint8_t *p = (uint8_t *) malloc(2 * len);
  uint8_t *p = (uint8_t *) mem::GetThreadLocalRegion(alloc_by_coreid).Alloc(2 * len);

  versions = (uint64_t *) p;
  objects = (uintptr_t *) (p + len);
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

    // uint8_t *p = (uint8_t *) malloc(2 * len);
    p = (uint8_t *) mem::GetThreadLocalRegion(alloc_by_coreid).Alloc(2 * len);

    memcpy(p, versions, len / 2);
    memcpy(p + len, objects, len / 2);

    versions = (uint64_t *) p;
    objects = (uintptr_t *) (p + len);

    mem::GetThreadLocalRegion(old_id).Free(old_p, len);
    // free(old_p);
  }
}

void SortedArrayVHandle::AppendNewVersion(uint64_t sid)
{
  bool expected = false;
  while (!locked.compare_exchange_weak(expected, true, std::memory_order_release,
				       std::memory_order_relaxed)) {
    expected = false;
  }

  uint64_t ep = Epoch::CurrentEpochNumber();
  if (ep > last_gc_epoch) {
    // gaurantee that we're the *first one* to garbage collect at the *epoch boundary*.
    GarbageCollect();
    last_gc_epoch = ep;
  }

  size++;
  EnsureSpace();
  versions[size - 1] = sid;
  objects[size - 1] = PENDING_VALUE;

  // now we need to swap backwards... hope this won't take too long...
  // TODO: replace this with a cleverer binary search if matters
  for (int i = size - 1; i > 0; i--) {
    if (versions[i - 1] > versions[i]) {
      std::swap(versions[i - 1], versions[i]);
    } else {
      break;
    }
  }
  locked.store(false);
}

volatile uintptr_t *SortedArrayVHandle::WithVersion(uint64_t sid)
{
  assert(size > 0);
  int pos;

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

VarStr *SortedArrayVHandle::ReadWithVersion(uint64_t sid)
{
  // if (versions.size() > 0) assert(versions[0] == 0);
  volatile uintptr_t *addr = WithVersion(sid);
  if (!addr) return nullptr;

  // Locking + cv might be a bad idea. lock is too expensive for this critical path!
  // So, let's just do an avoidance.
  // Also, make sure this is running on a go::Routine
  while (true) {
    for (int i = 0; i < kMaxRetry; i++) {
      if (*addr != PENDING_VALUE) {
	return (VarStr *) *addr;
      }
    }
    go::Scheduler::Current()->RunNext(go::Scheduler::NextReadyState);
  }
}

void SortedArrayVHandle::GarbageCollect()
{
  return;
  if (size < 2) return;
  uint64_t latest_version = versions[size - 1];
  uintptr_t latest_object = objects[size - 1];

  for (int i = size - 2; i >= 0; i--) {
    VarStr *o = (VarStr *) objects[i];
    delete o;
  }
  versions[0] = latest_version;
  objects[0] = latest_object;
  size = 1;
}

mem::Pool<true> *SortedArrayVHandle::pools;

void SortedArrayVHandle::InitPools()
{
  pools = (mem::Pool<true> *) malloc(sizeof(mem::Pool<true>) * Epoch::kNrThreads);
  for (int i = 0; i < Epoch::kNrThreads; i++) {
    new (&pools[i]) mem::Pool<true>(64, 16 << 20, i / mem::kNrCorePerNode);
  }
}

}
