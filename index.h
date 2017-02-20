// -*- c++ -*-
#ifndef INDEX_H
#define INDEX_H

#include <map>
#include <vector>
#include <tuple>
#include <algorithm>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <atomic>
#include "mem.h"
#include "log.h"
#include "epoch.h"
#include "util.h"

namespace dolly {

class TxnValidator {
  unsigned int key_crc, value_crc;
  size_t value_size;
  static std::atomic<unsigned long> tot_validated;
  std::vector<std::vector<uint8_t>> data;
public:
  TxnValidator() :
    key_crc(INITIAL_CRC32_VALUE), value_crc(INITIAL_CRC32_VALUE),
    value_size(0){}

  void CaptureWrite(const VarStr *k, VarStr *obj);
  void Validate(const Txn &tx);
};

using util::ListNode;

struct CommitBufferEntry {
  ListNode ht_node;
  ListNode lru_node;
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
public:
  // TODO: load checkpoint plugins dynamically?
  // static void LoadPlugins();

  static Checkpoint *LoadCheckpointImpl(const std::string &filename);
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
  size_t key_len;
public:
  BaseRelation() : id(-1) {}

  void set_id(int relation_id) { id = relation_id; }
  int relation_id() { return id; }

  void set_key_length(size_t l) { key_len = l; }
  size_t key_length() const { return key_len; }

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
    assert(fid < kMaxNrRelations);
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


#define PENDING_VALUE 0xFFFFFFF9E11D1110 // hope this pointer is weird enough

template <template<typename> class IndexPolicy, class VHandle>
class RelationPolicy : public BaseRelation,
		       public IndexPolicy<VHandle> {
public:
  void SetupReExec(const VarStr *k, uint64_t sid, VarStr *obj = (VarStr *) PENDING_VALUE) {
    auto handle = this->InsertOrCreate(k);
    handle->AppendNewVersion(sid);
    if (obj != (void *) PENDING_VALUE) {
      handle->WriteWithVersion(sid, obj);
    }
  }

  const VarStr *Get(const VarStr *k, uint64_t sid, CommitBuffer &buffer) {
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
    // dry run to test
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

  typename IndexPolicy<VHandle>::Iterator SearchIterator(const VarStr *k, uint64_t sid,
							 CommitBuffer &buffer) {
    return std::move(this->IndexSearchIterator(k, id, sid, buffer));
  }

  typename IndexPolicy<VHandle>::Iterator SearchIterator(const VarStr *start, const VarStr *end,
							 uint64_t sid, CommitBuffer &buffer) {
    return std::move(this->IndexSearchIterator(start, end, id, sid, buffer));
  }


private:
  void CallScanCallback(typename IndexPolicy<VHandle>::Iterator it, uint64_t sid,
			CommitBuffer &buffer,
			std::function<bool (const VarStr *k, const VarStr *v)> callback) {
    while (it.IsValid()) {
      const VarStr &key = it.key();
      const VarStr *value = it.object();

      bool should_continue = callback(&key, value);
      if (!should_continue) break;

      it.Next(sid, buffer);
    }
  }
};

class BaseVHandle {
public:
  static mem::Pool<true> *pools;
  static void InitPools();
};

class SortedArrayVHandle : public BaseVHandle {
  short alloc_by_coreid;
  short this_coreid;
  std::atomic_bool lock;

  int64_t last_gc_epoch;
  int64_t min_of_epoch;
  size_t capacity;
  size_t size;
  uint64_t *versions;
  uintptr_t *objects;

  struct TxnWaitSlot {
    std::mutex lock;
    go::WaitSlot slot;
  };
  // TxnWaitSlot *slots;

public:
  // static SortedArrayVHandle *New() {
  //   return new (pools[mem::CurrentAllocAffinity()].Alloc()) SortedArrayVHandle();
  // }
  //
  // static void Free(SortedArrayVHandle *ptr) {
  //   pools[ptr->this_coreid].Free(ptr);
  // }

  static void *operator new(size_t nr_bytes) {
    return pools[mem::CurrentAllocAffinity()].Alloc();
  }

  static void operator delete(void *ptr) {
    SortedArrayVHandle *phandle = (SortedArrayVHandle *) ptr;
    pools[phandle->this_coreid].Free(ptr);
  }

  SortedArrayVHandle();
  SortedArrayVHandle(SortedArrayVHandle &&rhs) = delete;

  void AppendNewVersion(uint64_t sid);
  VarStr *ReadWithVersion(uint64_t sid);
  bool WriteWithVersion(uint64_t sid, VarStr *obj, bool dry_run = false);
  void GarbageCollect();
private:
  void EnsureSpace();
  volatile uintptr_t *WithVersion(uint64_t sid, int &pos);
};

static_assert(sizeof(SortedArrayVHandle) <= 64, "SortedArrayVHandle is larger than a cache line");

}

#endif /* INDEX_H */

// include the real index implementation here
// #include "stdmap_index_impl.h"
#include "masstree_index_impl.h"
