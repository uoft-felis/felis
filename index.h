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

// keys in the index data structure
struct BaseIndexKey {
  VarStr *k;
  bool operator<(const BaseIndexKey &rhs) const {
    if (rhs.k == nullptr) return false;
    if (k == nullptr) return true;
    int r = memcmp(k->data, rhs.k->data, std::min(k->len, rhs.k->len));
    return r < 0 || (r == 0 && k->len < rhs.k->len);
  }

  bool operator==(const BaseIndexKey &rhs) const {
    if (rhs.k == nullptr && k == nullptr) return true;
    else if (rhs.k == nullptr || k == nullptr) return false;

    return k->len == rhs.k->len && memcmp(k->data, rhs.k->data, k->len) == 0;
  }
  bool operator!=(const BaseIndexKey &rhs) const { return !(*this == rhs); }

  BaseIndexKey(VarStr *key) : k(key) {}
  BaseIndexKey(const BaseIndexKey &key) = delete;
  BaseIndexKey(BaseIndexKey &&rhs) {
    k = rhs.k;
    rhs.k = nullptr;
  }
};

struct IndexKey : public BaseIndexKey {
  IndexKey(VarStr *key) : BaseIndexKey(key) {}
  ~IndexKey() { delete k; }
};

// used for on-stack zero copy index keys
struct ConstIndexKey : public BaseIndexKey {
  ConstIndexKey(VarStr *key) : BaseIndexKey(key) {}
  ~ConstIndexKey() {}
};

class TxnValidator {
  unsigned int key_crc, value_crc;
  size_t value_size;
  static std::atomic<unsigned long> tot_validated;
  std::vector<std::vector<uint8_t>> data;
public:
  TxnValidator() :
    key_crc(INITIAL_CRC32_VALUE), value_crc(INITIAL_CRC32_VALUE),
    value_size(0){}

  void CaptureWrite(const BaseIndexKey &k, VarStr *obj);
  void Validate(const Txn &tx);
};


// we need this class because we need to handle read local-write scenario.
class CommitBuffer {
  typedef std::map<std::tuple<int, BaseIndexKey>, std::pair<int, VarStr *>> BufferMap;

  BufferMap buf;
  std::vector<BufferMap::iterator> write_seq;
  const Txn *tx;
public:
  CommitBuffer(Txn *txn) : tx(txn) {}

  void Put(int fid, IndexKey && key, VarStr *obj);
  VarStr *Get(int fid, const ConstIndexKey &k);
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
  struct {
    uint64_t new_key_cnt;
    uint64_t key_cnt;
  } stat;
  int id;
public:
  BaseRelation() { ClearStat(); }
  void ClearStat() { memset(&stat, 0, sizeof(stat)); }
  void LogStat() const;

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
  static const int kMaxNrRelations = 256;
  std::array<T, kMaxNrRelations> relations;
public:
  RelationManagerPolicy() {
    for (int i = 0; i < kMaxNrRelations; i++) relations[i].set_id(i);
  }

  T &GetRelationOrCreate(int fid) {
    assert(fid < kMaxNrRelations);
    return relations[fid];
  }
  void LogStat() {
    for (auto &r: relations) r.LogStat();
  }
  void ClearStat() {
    for (auto &r: relations) r.ClearStat();
  }
};


#define PENDING_VALUE 0xFFFFFFF9E11D1110 // hope this pointer is weird enough

template <template<typename> class IndexPolicy, class VHandle>
class RelationPolicy : public BaseRelation,
		       public IndexPolicy<VHandle> {
public:
  void SetupReExec(IndexKey &&k, uint64_t sid, VarStr *obj = (VarStr *) PENDING_VALUE) {
    auto handle = this->Search(k);
    if (handle == nullptr) {
      handle = this->Insert(std::move(k), std::move(VHandle()));
      // compile may perform copy-elision optimization, therefore, our move
      // behavior might not be called at all.
      stat.new_key_cnt++;
      assert(handle != nullptr);
    }
    stat.key_cnt++;
    handle->AppendNewVersion(sid);
    if (obj != (void *) PENDING_VALUE) {
      handle->WriteWithVersion(sid, obj);
    }
  }

  VarStr *Get(const ConstIndexKey &k, uint64_t sid, CommitBuffer &buffer) {
    // read your own write
    VarStr *o = buffer.Get(id, k);
    if (o != nullptr) return o;

    auto handle = this->Search(k);
    assert(handle);
    return handle->ReadWithVersion(sid);
  }

  template <typename T>
  const T Get(const ConstIndexKey &k, uint64_t sid, CommitBuffer &buffer) {
    T instance;
    instance.DecodeFrom(Get(k, sid, buffer));
    return instance;
  }

  void Put(IndexKey &&k, VarStr *obj, CommitBuffer &buffer) {
    buffer.Put(id, std::move(k), obj);
  }

  void CommitPut(IndexKey &&k, uint64_t sid, VarStr *obj) {
    auto handle = this->Search(k);
    if (handle == nullptr) {
      handle = this->Insert(std::move(k), std::move(VHandle{}));
      assert(handle != nullptr);
    }
    handle->WriteWithVersion(sid, obj);
  }

  void Scan(const ConstIndexKey &k, uint64_t sid, CommitBuffer &buffer,
	    std::function<bool (const BaseIndexKey &k, const VarStr *v)> callback) {
    CallScanCallback(this->SearchIterator(k), sid, buffer, callback);
  }

  void Scan(const ConstIndexKey &start, const ConstIndexKey &end, uint64_t sid,
	    CommitBuffer &buffer,
	    std::function<bool (const BaseIndexKey &k, const VarStr *v)> callback) {
    CallScanCallback(this->SearchIterator(start, end), sid, buffer, callback);
  }

  typename IndexPolicy<VHandle>::Iterator SearchIterator(const BaseIndexKey &k) {
    return this->IndexSearchIterator(k, id);
  }

  typename IndexPolicy<VHandle>::Iterator SearchIterator(const BaseIndexKey &start, const BaseIndexKey &end) {
    return this->IndexSearchIterator(start, end, id);
  }


private:
  void CallScanCallback(typename IndexPolicy<VHandle>::Iterator it, uint64_t sid,
			CommitBuffer &buffer,
			std::function<bool (const BaseIndexKey &k, const VarStr *v)> callback) {
    while (it.IsValid()) {
      const BaseIndexKey &key = it.key();
      const VarStr *value = it.ReadVersion(sid, buffer);
      bool should_continue = callback(key, value);
      if (!should_continue) break;
      it.Next();
    }
  }
};

template <class IteratorImpl>
struct IndexIterator : public IteratorImpl {
  using IteratorImpl::IteratorImpl;

  VarStr *ReadVersion(uint64_t sid, CommitBuffer &buffer) {
    VarStr *o = buffer.Get(this->relation_id, this->key().k);
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

template <class VHandle>
class StdMapIndex {
  std::map<BaseIndexKey, VHandle> map;
protected:
  struct StdMapIteratorImpl {
    typedef typename std::map<BaseIndexKey, VHandle>::iterator MapIterator;
    StdMapIteratorImpl(MapIterator current_it, MapIterator end_it, int rid)
      : current(current_it), end(end_it), relation_id(rid) {}

    void Next() { ++current; }
    bool IsValid() const { return current != end; }

    const BaseIndexKey &key() const { return current->first; }
    const VHandle &vhandle() const { return current->second; }
    VHandle &vhandle() { return current->second; }
    MapIterator current, end;
    int relation_id;
  };

  typedef IndexIterator<StdMapIteratorImpl> Iterator;

public:
  VHandle *Insert(IndexKey &&k, VHandle &&vhandle) {
    auto p = map.emplace(std::move(k), std::move(vhandle));
    return &p.first->second;
  }

protected:
  Iterator IndexSearchIterator(const BaseIndexKey &k, int relation_id) {
    return Iterator(map.lower_bound(k), map.end(), relation_id);
  }
  Iterator IndexSearchIterator(const BaseIndexKey &start, const BaseIndexKey &end, int relation_id) {
    return Iterator(map.lower_bound(start), map.upper_bound(end), relation_id);
  }

  VHandle *Search(const BaseIndexKey &k) {
    auto it = map.find(k);
    if (it == map.end()) return nullptr;
    return &it->second;
  }

};

class SortedArrayVHandle {
public:
  std::vector<uint64_t> versions;
  std::vector<uintptr_t> objects;

  void AppendNewVersion(uint64_t sid) {
    // we're single thread, and we are assuming sid is monotoniclly increasing
    versions.push_back(sid);
    objects.emplace_back(PENDING_VALUE);
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

// current relation implementation

typedef RelationPolicy<StdMapIndex, SortedArrayVHandle> Relation;

typedef RelationManagerPolicy<Relation> RelationManager;

}

#endif /* INDEX_H */
