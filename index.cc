#include <fstream>
#include <limits>
#include <streambuf>
#include <iomanip>
#include <dlfcn.h>
#include "index.h"
#include "epoch.h"
#include "util.h"
#include "mem.h"
#include "json11/json11.hpp"
#include "gopp/gopp.h"

#include <sys/sdt.h>

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
  if (!k || -kptr->fid != fid || kptr->len != k->len
      || memcmp(kptr->data, k->data, k->len) != 0) {
    is_valid = false;
    logger->alert("Out-of-Order Write. sid {} fid {}",
		  tx.serializable_id(), -kptr->fid);
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
    std::abort();
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
    auto &rel = mgr.GetRelationOrCreate(entry->fid);
    auto handle = rel.Search(entry->key);
    if (handle->last_update_epoch() == entry->epoch_nr)
      rel.ImmediateDelete(entry->key);

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

    mgr.GetRelationOrCreate(entry->fid).CommitPut(entry->key, sid, entry->obj, is_garbage);

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

std::map<std::string, Checkpoint *> Checkpoint::impl;

}

namespace util {

template <>
dolly::DeletedGarbageHeads &Instance()
{
  return dolly::gDeletedGarbage;
}

}
