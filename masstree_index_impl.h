#ifndef MASSTREE_INDEX_IMPL_H
#define MASSTREE_INDEX_IMPL_H

#include <cstdio>
#include <atomic>

#include "index.h"
#include "util.h"
#include "log.h"

// OMG...
#include "masstree/build/config.h"
#include "masstree/masstree.hh"
#include "masstree/masstree_insert.hh"
#include "masstree/masstree_remove.hh"
#include "masstree/masstree_tcursor.hh"
#include "masstree/masstree_print.hh"
#include "masstree/masstree_scan.hh"
#include "masstree/kvthread.hh"
#include "masstree/timestamp.hh"

namespace dolly {

template <class VHandle>
struct VHandlePrinter {
  static void print(VHandle *value, FILE *fp, const char *prefix, int indent,
		    lcdf::Str key, kvtimestamp_t ts, char *suffix) {
    // TODO: maybe use the logger to print?
  }
};

template <class VHandle>
class MasstreeIndex {

  struct MasstreeDollyParam : public Masstree::nodeparams<15, 15> {
    typedef VHandle * value_type;
    typedef typename Masstree::value_print<VHandle> value_print_type;
    typedef threadinfo threadinfo_type;
  };
  typedef typename Masstree::basic_table<MasstreeDollyParam> MasstreeMap;

  MasstreeMap map;

protected:
  struct MasstreeMapIteratorImpl {

    const VarStr *end_key; // null key means never active terminates
    typename MasstreeMap::forward_scan_iterator it;
    threadinfo *ti;
    int relation_id;

    VarStr cur_key;

    MasstreeMapIteratorImpl(typename MasstreeMap::forward_scan_iterator &&scan_it,
			    const VarStr *terminate_key,
			    int relation_id, uint64_t sid, CommitBuffer &buffer)
      : end_key(terminate_key), it(std::move(scan_it)), relation_id(relation_id) {
      AdaptKey();
      ti = &MasstreeIndex<VHandle>::GetThreadInfo();
      if (IsValid()) {
	if (ShouldSkip(sid, buffer)) Next(sid, buffer);
      }
    }

    MasstreeMapIteratorImpl(typename MasstreeMap::forward_scan_iterator &&scan_it,
			    int relation_id, uint64_t sid, CommitBuffer &buffer)
      : end_key(nullptr), it(std::move(scan_it)), relation_id(relation_id) {
      AdaptKey();
      ti = &MasstreeIndex<VHandle>::GetThreadInfo();
      if (IsValid()) {
	if (ShouldSkip(sid, buffer)) Next(sid, buffer);
      }
    }

    void AdaptKey() {
      if (it.is_valid()) {
	// wrap the iterator
	auto s = it.key();
	cur_key.len = s.length();
	cur_key.data = (const uint8_t *) s.data();
      }
    }

    void Next(uint64_t sid, CommitBuffer &buffer) {
      do {
	it.next(*ti);
	AdaptKey();
      } while (IsValid() && ShouldSkip(sid, buffer));
    }

    bool IsValid() const {
      if (end_key == nullptr)
	return it.is_valid();
      else
	return it.is_valid() && !(*end_key < cur_key);
    }

    const VarStr &key() const {
      return cur_key;
    }

    const VarStr *object() const {
      return obj;
    }
  private:
    bool ShouldSkip(uint64_t sid, CommitBuffer &buffer) {
      obj = buffer.Get(relation_id, &key());
      if (!obj) obj = it.value()->ReadWithVersion(sid);
      return obj == nullptr;
    }
    const VarStr *obj;
  };
public:
  typedef MasstreeMapIteratorImpl Iterator;
  void Initialize(threadinfo &ti) {
    map.initialize(ti);
  }

protected:
  static threadinfo &GetThreadInfo();

  std::atomic_ulong nr_keys;

  VHandle *InsertOrCreate(const VarStr *k) {
    auto &ti = GetThreadInfo();
    typename MasstreeMap::cursor_type cursor(map, k->data, k->len);
    bool found = cursor.find_insert(ti);
    if (!found) {
      cursor.value() = VHandle::New();
      nr_keys.fetch_add(1);
    }
    auto result = cursor.value();
    cursor.finish(1, ti);
    assert(result != nullptr);
    return result;
  }

  VHandle *Search(const VarStr *k) {
    auto &ti = GetThreadInfo();
    VHandle *result = nullptr;
    map.get(lcdf::Str(k->data, k->len), result, ti);
    return result;
  }

  Iterator IndexSearchIterator(const VarStr *k, int relation_id, uint64_t sid, CommitBuffer &buffer) {
    return Iterator(std::move(map.find_iterator(lcdf::Str(k->data, k->len), GetThreadInfo())),
		    relation_id, sid, buffer);
  }
  Iterator IndexSearchIterator(const VarStr *start, const VarStr *end, int relation_id, uint64_t sid,
			       CommitBuffer &buffer) {
    return Iterator(std::move(map.find_iterator(lcdf::Str(start->data, start->len), GetThreadInfo())),
		    end, relation_id, sid, buffer);
  }

public:
  size_t nr_unique_keys() const { return nr_keys.load(); }
};

// current relation implementation
typedef RelationPolicy<MasstreeIndex, SortedArrayVHandle> Relation;

class RelationManager : public RelationManagerPolicy<Relation> {
  threadinfo *ti;
public:
  RelationManager() : RelationManagerPolicy<Relation>() {
    // initialize all relations
    ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    for (int i = 0; i < kMaxNrRelations; i++) {
      relations[i].Initialize(*ti);
    }
  }
  threadinfo *GetThreadInfo() { return ti; }
};

static __thread threadinfo *TLSThreadInfo;

template <class VHandle>
threadinfo &MasstreeIndex<VHandle>::GetThreadInfo()
{
  if (TLSThreadInfo == nullptr)
    TLSThreadInfo = threadinfo::make(threadinfo::TI_PROCESS, go::Scheduler::CurrentThreadPoolId());
  return *TLSThreadInfo;
}

}

#endif /* MASSTREE_INDEX_IMPL_H */
