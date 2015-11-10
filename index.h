// -*- c++ -*-
#ifndef INDEX_H
#define INDEX_H

#include <map>
#include <vector>
#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <atomic>
#include "log.h"

#pragma GCC diagnostic ignored "-fpermissive"

namespace db_backup {

struct TxnKey;

struct IndexKey {
  TxnKey *k;
  bool operator<(const IndexKey &rhs) const;
  bool operator==(const IndexKey &rhs) const;
  IndexKey(TxnKey *key) : k(key) {}
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
public:
  BaseRelation() { ClearStat(); }
  void ClearStat() { memset(&stat, 0, sizeof(stat)); }
  void LogStat() const;
};

template <template<typename> class IndexPolicy, class VersioningPolicy>
class Relation : public BaseRelation,
		 public IndexPolicy<typename VersioningPolicy::VHandle>,
		 public VersioningPolicy {
public:
  void SetupReExec(const IndexKey &k, uint64_t sid) {
    auto handle = this->Search(k);
    if (handle == nullptr) {
      handle = this->CreateVHandle();
      this->Insert(k, handle);
      stat.new_key_cnt++;
    }
    stat.key_cnt++;
    this->AppendNewVersion(handle, sid);
  }

  void *Get(const IndexKey &k, uint64_t sid) {
    auto handle = this->Search(k);
    assert(handle);
    return this->ReadWithVerion(handle, sid);
  }

  void Put(const IndexKey &k, uint64_t sid, void *obj) {
    auto handle = this->Search(k);
    assert(handle);
    this->WriteWithVersion(handle, sid, obj);
  }

  void Scan(const IndexKey &k, uint64_t sid,
	    std::function<bool (const IndexKey &k, const void *v)> callback) {
  }
};

template <class T>
class RelationManager {
  std::vector<T> relations;
public:
  T &GetRelationOrCreate(int fid) {
    if (fid >= relations.size()) {
      for (int i = relations.size(); i <= fid; i++)
	relations.emplace(relations.end(), std::move(T()));
    }
    return relations[fid];
  }
  void LogStat() {
    for (auto &r: relations) r.LogStat();
  }
  void ClearStat() {
    for (auto &r: relations) r.ClearStat();
  }
};

template <class VHandle>
class StdMapIndex {
  std::map<IndexKey, VHandle*> map;
public:
  void Insert(const IndexKey &k, VHandle *vhandle) {
    map.insert(std::make_pair(k, vhandle));
  }
  VHandle *Search(const IndexKey &k) {
    auto it = map.find(k);
    if (it == map.end()) return nullptr;
    return it->second;
  }
};

class SortedArrayVersioning {
public:
  struct VHandle {
    std::vector<uint64_t> versions;
    std::vector<uintptr_t> objects;
  };

  VHandle *CreateVHandle() {
    return new VHandle();
  }

  void AppendNewVersion(VHandle *ver_vec, uint64_t sid) {
    // we're single thread, and we are assuming sid is monotoniclly increasing
    ver_vec->versions.push_back(sid);
    // nullptr is the dummy object
    ver_vec->objects.emplace_back(0);
  }

  volatile uintptr_t *WithVersion(VHandle *ver_vec, uint64_t sid) {
    auto it = std::lower_bound(ver_vec->versions.begin(),
			       ver_vec->versions.end(), sid);

    if (it == ver_vec->versions.end()) {
      if (ver_vec->versions.empty()) {
	logger->alert("Cannot read version in tx {}, where version vector is empty!",
		      sid);
      } else {
	logger->alert("Cannot read version in tx {}, where the lowest is {}", sid,
		      ver_vec->versions[0]);
      }
      abort();
    }
    return &ver_vec->objects[it - ver_vec->versions.begin()];
  }

  void *ReadWithVerion(VHandle *ver_vec, uint64_t sid) {
    volatile uintptr_t *addr = WithVersion(ver_vec, sid);
    while (*addr != 0); // TODO: this is spinning. use futex for waiting?
    return (void *) *addr;
  }

  void *WriteWithVersion(VHandle *ver_vec, uint64_t sid, void *obj) {
    // I will be the only thread writing to this pointer! Yeah!
    volatile uintptr_t *addr = WithVersion(ver_vec, sid);
    *addr = (uintptr_t) obj;
  }
};

}

#endif /* INDEX_H */
