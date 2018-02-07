// -*- C++ -*-
#ifndef INDEX_COMMON_H
#define INDEX_COMMON_H

#include <memory>
#include <mutex>
#include <atomic>
#include <cstdlib>

#include "mem.h"
#include "log.h"
#include "epoch.h"
#include "txn.h"
#include "util.h"

#include "vhandle.h"

namespace dolly {

using util::ListNode;

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
 protected:
  RelationManagerPolicy() {
    // for (int i = 0; i < kMaxNrRelations; i++) relations[i].set_id(i);
    for (auto &p: relation_id_map) {
      relations[p.second].set_id(p.second);
    }
  }

 public:
  static const int kMaxNrRelations = 1024;

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
      bool is_garbage;
      if (!handle->WriteWithVersion(sid, obj, is_garbage)) {
        logger->error("Diverging outcomes during setup setup");
        std::abort();
      }
    }
  }

  bool SetupReExecSync(const VarStr *k, uint64_t sid) {
    SetupReExec(k, sid);
    return true;
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
    DTRACE_PROBE3(dolly, index_get, id, (const void *) k, sid);
#ifdef CALVIN_REPLAY
    if (is_read_only()) {
      return this->Search(k)->DirectRead();
    }
#endif

    auto *e = buffer.GetEntry(id, k);
    const VarStr *o = buffer.Get(e);
    if (o) return o;

    VHandle *handle = e ? e->handle : this->Search(k);
    return handle->ReadWithVersion(sid);
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
    bool is_garbage;
    if (!this->Search(k)->WriteWithVersion(sid, obj, is_garbage, true)) {
      logger->error("Diverging outcomes!");
      std::abort();
    }
#endif
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

#endif /* INDEX_COMMON_H */
