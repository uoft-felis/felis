#ifndef STDMAP_INDEX_IMPL_H
#define STDMAP_INDEX_IMPL_H

#include "index.h"

namespace dolly {

template <class VHandle>
class StdMapIndex {
protected:
  struct StdMapIteratorImpl {
    typedef typename std::map<VarStr, VHandle>::iterator MapIterator;
    StdMapIteratorImpl(MapIterator current_it, MapIterator end_it, int rid, uint64_t sid,
		       CommitBuffer &buffer)
      : current(current_it), end(end_it), relation_id(rid), obj(nullptr) {
      if (IsValid()) {
	if (ShouldSkip(sid, buffer)) Next(sid, buffer);
      }
    }

    void Next(uint64_t sid, CommitBuffer &buffer) {
      do {
	++current;
      } while (IsValid() && ShouldSkip(sid, buffer));
    }

    bool IsValid() const { return current != end; }

    const VarStr &key() const { return current->first; }
    const VarStr *object() const { return obj; }
    MapIterator current, end;
    int relation_id;
  private:
    bool ShouldSkip(uint64_t sid, CommitBuffer &buffer) {
      obj = buffer.Get(relation_id, &key());
      if (!obj) obj = current->second.ReadWithVersion(sid);
      return obj != nullptr;
    }
    const VarStr *obj;
  };
public:
  typedef StdMapIteratorImpl Iterator;
private:
  std::map<VarStr, VHandle> map;
protected:

  VHandle *InsertOrCreate(const VarStr *k) {
    // implicit copy
    VarStr *key = VarStr::New(k->len);
    uint8_t *ptr = (uint8_t *) key + sizeof(VarStr);
    memcpy(ptr, k->data, k->len);

    auto p = map.emplace(*key, std::move(VHandle()));
    return &p.first->second;
  }

  Iterator IndexSearchIterator(const VarStr *k, int relation_id, uint64_t sid,
			       CommitBuffer &buffer) {
    return Iterator(map.lower_bound(*k), map.end(), relation_id, sid, buffer);
  }
  Iterator IndexSearchIterator(const VarStr *start, const VarStr *end, int relation_id,
			       uint64_t sid, CommitBuffer &buffer) {
    return Iterator(map.lower_bound(*start), map.upper_bound(*end), relation_id, sid, buffer);
  }

  VHandle *Search(const VarStr *k) {
    auto it = map.find(*k);
    if (it == map.end()) return nullptr;
    return &it->second;
  }
};

// current relation implementation
typedef RelationPolicy<StdMapIndex, SortedArrayVHandle> Relation;
typedef RelationManagerPolicy<Relation> RelationManager;

}

#endif /* STDMAP_INDEX_IMPL_H */
