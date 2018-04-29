// -*- C++ -*-
#ifndef INDEX_COMMON_H
#define INDEX_COMMON_H

#include <memory>
#include <mutex>
#include <atomic>
#include <cstdlib>

#include "mem.h"
#include "log.h"
#include "util.h"

#include "vhandle.h"
#include "node_config.h"

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

  T &operator()(int fid) {
    return GetRelationOrCreate(fid);
  }

 protected:
  std::array<T, kMaxNrRelations> relations;
};

template <class IndexPolicy>
class RelationPolicy : public BaseRelation,
		       public IndexPolicy {
 public:

  VHandle *SetupReExec(const VarStr *k, uint64_t sid, uint64_t epoch_nr, VarStr *obj = (VarStr *) kPendingValue) {
    auto handle = this->InsertOrCreate(k);
    while (!handle->AppendNewVersion(sid, epoch_nr)) {
      asm("pause" : : :"memory");
    }
    if (obj != (void *) kPendingValue) {
      if (!handle->WriteWithVersion(sid, obj, epoch_nr)) {
        logger->error("Diverging outcomes during setup setup");
        std::abort();
      }
    }
    return handle;
  }

  VHandle *InitValue(const VarStr *k, VarStr *obj) {
    return SetupReExec(k, 0, 0, obj);
  }

  bool SetupReExecSync(const VarStr *k, uint64_t sid, uint64_t epoch_nr) {
    SetupReExec(k, sid, epoch_nr);
    return true;
  }

  VHandle *SetupReExecAsync(const VarStr *k, uint64_t sid, uint64_t epoch_nr) {
    auto handle = this->InsertOrCreate(k);
    if (!handle->AppendNewVersion(sid, epoch_nr))
      return nullptr;
    return handle;
  }
};

}

#endif /* INDEX_COMMON_H */
