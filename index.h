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

// we need this class because we need to handle read local-write scenario.
class CommitBuffer {
  const Txn *tx;
  struct CommitBufferEntry {
    ListNode ht_node;
    ListNode lru_node;

    int fid;
    const VarStr *key;
    VarStr *obj;
    CommitBufferEntry(int id, const VarStr *k, VarStr *o) : fid(id), key(k), obj(o) {}
  };
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
  CommitBuffer(CommitBuffer &&rhs) = delete;
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
public:
  BaseRelation() {}

  void set_id(int relation_id) { id = relation_id; }
  int relation_id() { return id; }
};

class RelationManagerBase {
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

private:
  std::map<std::string, int> relation_id_map;
};

template <class T>
class RelationManagerPolicy : public RelationManagerBase {
public:
  static const int kMaxNrRelations = 256;

  RelationManagerPolicy() {
    for (int i = 0; i < kMaxNrRelations; i++) relations[i].set_id(i);
  }

  T &GetRelationOrCreate(int fid) {
    assert(fid < kMaxNrRelations);
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
    // TODO: get rid of stat?
    auto handle = this->Insert(k, std::move(VHandle()));
    handle->AppendNewVersion(sid);
    if (obj != (void *) PENDING_VALUE) {
      handle->WriteWithVersion(sid, obj);
    }
  }

  VarStr *Get(const VarStr *k, uint64_t sid, CommitBuffer &buffer) {
    // read your own write
    VarStr *o = buffer.Get(id, k);
    if (o != nullptr) return o;

    auto handle = this->Search(k);
    assert(handle);
    return handle->ReadWithVersion(sid);
  }

  template <typename T>
  const T Get(const VarStr *k, uint64_t sid, CommitBuffer &buffer) {
    T instance;
    instance.DecodeFrom(Get(k, sid, buffer));
    return instance;
  }

  void Put(const VarStr *k, VarStr *obj, CommitBuffer &buffer) {
    buffer.Put(id, std::move(k), obj);
  }

  void CommitPut(const VarStr *k, uint64_t sid, VarStr *obj) {
    auto handle = this->Insert(k, std::move(VHandle{}));
    handle->WriteWithVersion(sid, obj);
  }

  void Scan(const VarStr *k, uint64_t sid, CommitBuffer &buffer,
	    std::function<bool (const VarStr *k, const VarStr *v)> callback) {
    CallScanCallback(this->SearchIterator(k), sid, buffer, callback);
  }

  void Scan(const VarStr *start, const VarStr *end, uint64_t sid,
	    CommitBuffer &buffer,
	    std::function<bool (const VarStr *k, const VarStr *v)> callback) {
    CallScanCallback(this->SearchIterator(start, end), sid, buffer, callback);
  }

  typename IndexPolicy<VHandle>::Iterator SearchIterator(const VarStr *k) {
    return std::move(this->IndexSearchIterator(k, id));
  }

  typename IndexPolicy<VHandle>::Iterator SearchIterator(const VarStr *start, const VarStr *end) {
    return std::move(this->IndexSearchIterator(start, end, id));
  }


private:
  void CallScanCallback(typename IndexPolicy<VHandle>::Iterator it, uint64_t sid,
			CommitBuffer &buffer,
			std::function<bool (const VarStr *k, const VarStr *v)> callback) {
    while (it.IsValid()) {
      const VarStr &key = it.key();
      const VarStr *value = it.ReadVersion(sid, buffer);
      bool should_continue = callback(&key, value);
      if (!should_continue) break;
      it.Next();
    }
  }
};

template <class IteratorImpl>
struct IndexIterator : public IteratorImpl {
  using IteratorImpl::IteratorImpl;

  VarStr *ReadVersion(uint64_t sid, CommitBuffer &buffer) {
    VarStr *o = buffer.Get(this->relation_id, &this->key());
    if (o) return o;
    return this->vhandle().ReadWithVersion(sid);
  }
  template <typename T>
  T ReadVersion(uint64_t sid, CommitBuffer &buffer) {
    T instance;
    instance.DecodeFrom(ReadVersion(sid, buffer));
    return instance;
  }
};

class SortedArrayVHandle {
public:
  std::atomic_bool locked;
  int64_t last_gc_epoch;
  size_t capacity;
  size_t size;
  uint64_t *versions;
  uintptr_t *objects;

  SortedArrayVHandle() : locked(false), last_gc_epoch(Epoch::CurrentEpochNumber()) {
    capacity = 4;
    size = 0;
    versions = (uint64_t *) malloc(capacity * sizeof(uint64_t));
    objects = (uintptr_t *) malloc(capacity * sizeof(uintptr_t));
  }

  SortedArrayVHandle(SortedArrayVHandle &&rhs)
    : locked(false), last_gc_epoch(Epoch::CurrentEpochNumber()) {
    capacity = rhs.capacity;
    size = rhs.size;
    versions = rhs.versions;
    objects = rhs.objects;

    rhs.capacity = rhs.size = 0;
    rhs.versions = nullptr;
    rhs.objects = nullptr;
  }

  void EnsureSpace() {
    if (unlikely(size == capacity)) {
      capacity *= 2;
      versions = (uint64_t *) realloc(versions, capacity * sizeof(uint64_t));
      objects = (uintptr_t *) realloc(objects, capacity * sizeof(uintptr_t));
    }
  }

  void AppendNewVersion(uint64_t sid) {
    // we're single thread, and we are assuming sid is monotoniclly increasing
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
    for (int i = size - 1; i > 0; i--) {
      if (versions[i - 1] > versions[i]) std::swap(versions[i - 1], versions[i]);
      else break;
    }
    locked.store(false);
  }

  volatile uintptr_t *WithVersion(uint64_t sid) {
    assert(size > 0);

    if (size == 1) {
      assert(versions[0] < sid);
      return &objects[0];
    }

    auto it = std::lower_bound(versions, versions + size, sid);
    if (it == versions) {
      logger->critical("Going way too back. GC bug. ver {} sid {}", *it, sid);
      std::abort();
    }
    --it;
    return &objects[it - versions];
  }

  VarStr *ReadWithVersion(uint64_t sid) {
    // if (versions.size() > 0) assert(versions[0] == 0);
    volatile uintptr_t *addr = WithVersion(sid);
    // TODO: this is spinning. we should do a u-context switch to other txns
    while (*addr == PENDING_VALUE);
    return (VarStr *) *addr;
  }

  void WriteWithVersion(uint64_t sid, VarStr *obj) {
    // Writing to exact location
    auto it = std::lower_bound(versions, versions + size, sid);
    if (*it != sid) {
      logger->critical("Diverging outcomes!");
      std::abort();
    }
    volatile uintptr_t *addr = &objects[it - versions];
    *addr = (uintptr_t) obj;
  }

  void GarbageCollect() {
    if (size < 2) return;
    uint64_t latest_version = versions[size - 1];
    uintptr_t latest_object = objects[size - 1];

    for (int i = size - 2; i >= 0; i--) {
      VarStr *o = (VarStr *) objects[i];
      free(o);
    }
    versions[0] = latest_version;
    objects[0] = latest_object;
    size = 1;
  }
};

}

#endif /* INDEX_H */

// include the real index implementation here
// #include "stdmap_index_impl.h"
#include "masstree_index_impl.h"
