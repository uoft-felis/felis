#ifndef MASSTREE_INDEX_IMPL_H
#define MASSTREE_INDEX_IMPL_H

#include <cstdio>
#include <atomic>

#include "index.h"
#include "util.h"
#include "log.h"
#include "vhandle.h"

class threadinfo;

namespace dolly {

class RelationManager;
class MasstreeMap;
class MasstreeMapForwardScanIteratorImpl;

class MasstreeIndex {
 protected:
  MasstreeMap *map;
  static threadinfo *GetThreadInfo();
 public:
  struct Iterator {
    const VarStr *end_key; // null key means never active terminates
    MasstreeMapForwardScanIteratorImpl *it; // TODO: fix memory leaks!
    threadinfo *ti;
    int relation_id;
    bool relation_read_only;
    uint64_t sid;
    CommitBuffer *buffer;

    VarStr cur_key;

    Iterator(MasstreeMapForwardScanIteratorImpl *scan_it,
             const VarStr *terminate_key,
             int relation_id, bool read_only,
             uint64_t sid, CommitBuffer &buffer);

    Iterator(MasstreeMapForwardScanIteratorImpl *scan_it,
             int relation_id, bool read_only, uint64_t sid, CommitBuffer &buffer)
        : Iterator(scan_it, nullptr, relation_id, read_only, sid, buffer) {}

    void AdaptKey();

    void Next();

    bool IsValid() const;

    const VarStr &key() const {
      return cur_key;
    }

    const VarStr *object() const {
      return obj;
    }
   private:
    bool ShouldSkip();
    const VarStr *obj;
  };
  void Initialize(threadinfo *ti);
 protected:
  friend DeletedGarbageHeads;

  struct {
    uint64_t add_cnt;
    uint64_t del_cnt;
  } nr_keys [NR_THREADS]; // scalable counting

 public:
  VHandle *InsertOrCreate(const VarStr *k);
  VHandle *Search(const VarStr *k);

  Iterator IndexSearchIterator(const VarStr *k, int relation_id, bool read_only, uint64_t sid,
                               CommitBuffer &buffer);
  Iterator IndexSearchIterator(const VarStr *start, const VarStr *end, int relation_id, bool read_only,
                               uint64_t sid, CommitBuffer &buffer);

  size_t nr_unique_keys() const {
    size_t rs = 0;
    for (int i = 0; i < NR_THREADS; i++) {
      rs += nr_keys[i].add_cnt - nr_keys[i].del_cnt;
    }
    return rs;
  }
  void ImmediateDelete(const VarStr *k);
  void FakeDelete(const VarStr *k) {
    // delete an object, this won't be checkpointed
    nr_keys[go::Scheduler::CurrentThreadPoolId() - 1].del_cnt++;
  }
};

class Relation : public RelationPolicy<MasstreeIndex> {};

class RelationManager : public RelationManagerPolicy<Relation> {
  threadinfo *ti;
 public:
  RelationManager();
  threadinfo *GetThreadInfo() { return ti; }
};

}

#endif /* MASSTREE_INDEX_IMPL_H */
