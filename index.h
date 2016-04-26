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


// we need this class because we need to handle read local-write scenario.
class CommitBuffer {
  typedef std::map<std::tuple<int, VarStr>, std::pair<int, VarStr *>> BufferMap;

  BufferMap buf;
  std::vector<BufferMap::iterator> write_seq;
  const Txn *tx;
public:
  CommitBuffer(Txn *txn) : tx(txn) {}

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
  std::vector<uint64_t> versions;
  std::vector<uintptr_t> objects;

  SortedArrayVHandle() : locked(false) {}
  SortedArrayVHandle(SortedArrayVHandle &&rhs)
    : versions(std::move(rhs.versions)), objects(std::move(rhs.objects)), locked(false) {}

  void AppendNewVersion(uint64_t sid) {
    // we're single thread, and we are assuming sid is monotoniclly increasing
    bool expected = false;
    while (!locked.compare_exchange_weak(expected, true, std::memory_order_release,
					 std::memory_order_relaxed)) {
      expected = false;
    }

    versions.push_back(sid);
    objects.emplace_back(PENDING_VALUE);

    // now we need to swap backwards... hope this won't take too long...
    for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
      auto prev = it;
      ++prev;
      if (prev == versions.rend())
	break;
      if (*prev > *it) {
	std::swap(*prev, *it);
      } else {
	break;
      }
    }
    locked.store(false);
  }

  volatile uintptr_t *WithVersion(uint64_t sid) {
    assert(versions.size() > 0);

    if (versions.size() == 1) {
      assert(versions[0] < sid);
      return &objects[0];
    }

    auto it = std::lower_bound(versions.begin(),
			       versions.end(), sid);
    if (it == versions.begin()) {
      logger->critical("Going way too back. GC bug");
      std::abort();
    }
    --it;
    return &objects[it - versions.begin()];
  }

  VarStr *ReadWithVersion(uint64_t sid) {
    // if (versions.size() > 0) assert(versions[0] == 0);
    volatile uintptr_t *addr = WithVersion(sid);
    while (*addr == PENDING_VALUE); // TODO: this is spinning. use futex for waiting?
    return (VarStr *) *addr;
  }

  void WriteWithVersion(uint64_t sid, VarStr *obj) {
    // Writing to exact location
    auto it = std::lower_bound(versions.begin(), versions.end(), sid);
    if (sid == 7) {
      logger->info("WTF {} {}", versions.size(), obj->len);
    }
    if (*it != sid) {
      logger->critical("Diverging outcomes!");
      std::abort();
    }
    volatile uintptr_t *addr = &objects[it - versions.begin()];
    *addr = (uintptr_t) obj;
  }

  void GarbageCollect(uint64_t max_allowed_version) {
    /*
    for (int i = 0; i < versions.size(); i++) {
      if (versions[i] >= max_allowed_version) return;
    }
    */
  }
};

}

#endif /* INDEX_H */

// include the real index implementation here
// #include "stdmap_index_impl.h"
#include "masstree_index_impl.h"
