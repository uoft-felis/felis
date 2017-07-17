// -*- C++ -*-
#ifndef INDEX_H
#define INDEX_H

#include <memory>
#include <mutex>
#include <atomic>
#include <cstdlib>

#include "mem.h"
#include "log.h"
#include "epoch.h"
#include "util.h"

#include "vhandle.h"

namespace dolly {

class TxnValidator {
  unsigned int key_crc, value_crc;
  size_t value_size;
  static std::atomic<unsigned long> tot_validated;
  uint8_t *keys_ptr;
  bool is_valid;
 public:
  TxnValidator() :
      key_crc(INITIAL_CRC32_VALUE), value_crc(INITIAL_CRC32_VALUE),
      value_size(0), keys_ptr(nullptr), is_valid(true) {}

  void set_keys_ptr(uint8_t *kptr) { keys_ptr = kptr; }
  void CaptureWrite(const Txn &tx, int fid, const VarStr *k, VarStr *obj);
  void Validate(const Txn &tx);

  static void DebugVarStr(const char *prefix, const VarStr *s);
};

using util::ListNode;

struct CommitBufferEntry {
  ListNode ht_node;
  ListNode lru_node; // fifo_node? for ERMIA 2.0?
  uint64_t epoch_nr;

  int fid;
  const VarStr *key;
  VarStr *obj;
  CommitBufferEntry(int id, const VarStr *k, VarStr *o)
      : epoch_nr(Epoch::CurrentEpochNumber()), fid(id), key(k), obj(o) {}
};

class DeletedGarbageHeads {
  ListNode garbage_heads[NR_THREADS];

 public:
  DeletedGarbageHeads();

  void AttachGarbage(CommitBufferEntry *g);
  void CollectGarbage(uint64_t epoch_nr);
};

// we need this class because we need to handle read local-write scenario.
class CommitBuffer {
  const Txn *tx;

  ListNode lru;
  ListNode *htable;
  static const int kHashTableSize = 37;
 public:

  CommitBuffer(Txn *txn) : tx(txn) {
    htable = new ListNode[kHashTableSize];
    for (int i = 0; i < kHashTableSize; i++) {
      htable[i].Initialize();
    }
    lru.Initialize();
  }
  CommitBuffer(const CommitBuffer &rhs) = delete;
  CommitBuffer(CommitBuffer &&rhs) {
    tx = rhs.tx;
    lru = rhs.lru;
    htable = rhs.htable;
    rhs.htable = nullptr;
  }
  ~CommitBuffer() {
    delete [] htable;
  }

  unsigned int Hash(int fid, const VarStr *key) {
    unsigned int h = fid;
    unsigned int l = key->len;
    const uint8_t *p = key->data;
    while (l >= 8) {
      h ^= *((const uint64_t *) p);
      p += 8;
      l -= 8;
    }
    if (l > 0) {
      uint64_t extra = 0xFFFFFFFFFFFFFFFFUL;
      memcpy(&extra, p, l);
      h ^= extra;
    }
    return h;
  }

  void Put(int fid, const VarStr *key, VarStr *obj);
  VarStr *Get(int fid, const VarStr *k);
  void Commit(uint64_t sid, TxnValidator *validator = nullptr);
};

class Checkpoint {
  static std::map<std::string, Checkpoint *> impl;
 public:
  static void RegisterCheckpointFormat(std::string fmt, Checkpoint *pimpl) { impl[fmt] = pimpl; }
  static Checkpoint *checkpoint_impl(std::string fmt) { return impl[fmt]; }
  virtual void Export() = 0;
};

// Relations is an index or a table. Both are associates between keys and
// rows.
//
// Since we are doing replay, we need two layers of indexing.
// First layer is indexing the keys.
// Second layer is the versioning layer. The versioning layers could be simply
// kept as a sorted array
class BaseRelation {
 protected:
  int id;
  bool read_only;
  size_t key_len;
 public:
  BaseRelation() : id(-1), read_only(false) {}

  void set_id(int relation_id) { id = relation_id; }
  int relation_id() { return id; }

  void set_key_length(size_t l) { key_len = l; }
  size_t key_length() const { return key_len; }

  void set_read_only(bool v) { read_only = v; }
  bool is_read_only() const { return read_only; }
};

class RelationManagerBase {
 protected:
  std::map<std::string, int> relation_id_map;
 public:
  RelationManagerBase();
  int LookupRelationId(std::string name) const {
    auto it = relation_id_map.find(name);
    if (it == relation_id_map.end()) {
      logger->critical("Cannot find relation {}", name);
      std::abort();
    }
    return it->second;
  }

  std::map<std::string, int> relation_mapping() const { return relation_id_map; }
};

template <class T>
class RelationManagerPolicy : public RelationManagerBase {
 public:
  static const int kMaxNrRelations = 1024;

  RelationManagerPolicy() {
    // for (int i = 0; i < kMaxNrRelations; i++) relations[i].set_id(i);
    for (auto &p: relation_id_map) {
      relations[p.second].set_id(p.second);
    }
  }

  T &GetRelationOrCreate(int fid) {
#ifndef NDEBUG
    if (fid < 0 || fid >= kMaxNrRelations || relations[fid].relation_id() == -1) {
      logger->critical("WTF is {}?", fid);
      std::abort();
    }
#endif
    return relations[fid];
  }
 protected:
  std::array<T, kMaxNrRelations> relations;
};

template <class IndexPolicy>
class RelationPolicy : public BaseRelation,
		       public IndexPolicy {
 public:
  void SetupReExec(const VarStr *k, uint64_t sid, VarStr *obj = (VarStr *) kPendingValue) {
    auto handle = this->InsertOrCreate(k);
    while (!handle->AppendNewVersion(sid)) {
      asm("pause" : : :"memory");
    }
    if (obj != (void *) kPendingValue) {
      handle->WriteWithVersion(sid, obj);
    }
  }

  bool SetupReExecAsync(const VarStr *k, uint64_t sid) {
    auto handle = this->InsertOrCreate(k);
    return handle->AppendNewVersion(sid);
  }

#ifdef CALVIN_REPLAY
  bool SetupReExecAccessAsync(const VarStr *k, uint64_t sid) {
    auto handle = this->InsertOrCreate(k);
    return handle->AppendNewAccess(sid, true);
  }
#endif

  const VarStr *Get(const VarStr *k, uint64_t sid, CommitBuffer &buffer) {
#ifdef CALVIN_REPLAY
    if (is_read_only()) {
      return this->Search(k)->DirectRead();
    }
#endif

    const VarStr *o = buffer.Get(id, k);
    if (!o) o = this->Search(k)->ReadWithVersion(sid);
    return o;
  }

  template <typename T>
  const T Get(const VarStr *k, uint64_t sid, CommitBuffer &buffer) {
    const VarStr *o = Get(k, sid, buffer);
    return o->ToType<T>();
  }

  void Put(const VarStr *k, uint64_t sid, VarStr *obj, CommitBuffer &buffer) {
    buffer.Put(id, std::move(k), obj);
#ifndef NDEBUG
    // Dry run to test. Extremely helpful for debugging deadlock
    this->Search(k)->WriteWithVersion(sid, obj, true);
#endif
  }

  bool CommitPut(const VarStr *k, uint64_t sid, VarStr *obj) {
    if (!this->Search(k)->WriteWithVersion(sid, obj)) {
      this->nr_keys[go::Scheduler::CurrentThreadPoolId() - 1].del_cnt++; // delete an object, this won't be checkpointed
      return false;
    }
    return true;
  }

  void Scan(const VarStr *k, uint64_t sid, CommitBuffer &buffer,
	    std::function<bool (const VarStr *k, const VarStr *v)> callback) {
    CallScanCallback(this->SearchIterator(k, sid, buffer), sid, buffer, callback);
  }

  void Scan(const VarStr *start, const VarStr *end, uint64_t sid,
	    CommitBuffer &buffer,
	    std::function<bool (const VarStr *k, const VarStr *v)> callback) {
    CallScanCallback(this->SearchIterator(start, end, sid, buffer), sid, buffer, callback);
  }

  typename IndexPolicy::Iterator SearchIterator(const VarStr *k, uint64_t sid,
                                                CommitBuffer &buffer) {
    auto it = this->IndexSearchIterator(k, id, is_read_only(), sid, buffer);
    return it;
  }

  typename IndexPolicy::Iterator SearchIterator(const VarStr *start, const VarStr *end,
                                                uint64_t sid, CommitBuffer &buffer) {
    auto it = this->IndexSearchIterator(start, end, id, is_read_only(), sid, buffer);
    return it;
  }

 private:
  void CallScanCallback(typename IndexPolicy::Iterator it, uint64_t sid,
			CommitBuffer &buffer,
			std::function<bool (const VarStr *k, const VarStr *v)> callback) {
    while (it.IsValid()) {
      const VarStr &key = it.key();
      const VarStr *value = it.object();

      bool should_continue = callback(&key, value);
      if (!should_continue) break;

      it.Next();
    }
  }
};

}

#endif /* INDEX_H */

// include the real index implementation here
// #include "stdmap_index_impl.h"
#include "masstree_index_impl.h"
