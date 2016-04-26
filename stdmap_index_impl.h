#ifndef STDMAP_INDEX_IMPL_H
#define STDMAP_INDEX_IMPL_H

#include "index.h"

namespace dolly {

template <class VHandle>
class StdMapIndex {
protected:
  struct StdMapIteratorImpl {
    typedef typename std::map<VarStr, VHandle>::iterator MapIterator;
    StdMapIteratorImpl(MapIterator current_it, MapIterator end_it, int rid)
      : current(current_it), end(end_it), relation_id(rid) {}

    void Next() { ++current; }
    bool IsValid() const { return current != end; }

    const VarStr &key() const { return current->first; }
    const VHandle &vhandle() const { return current->second; }
    VHandle &vhandle() { return current->second; }
    MapIterator current, end;
    int relation_id;
  };
public:
  typedef IndexIterator<StdMapIteratorImpl> Iterator;
private:
  std::map<VarStr, VHandle> map;
protected:

  VHandle *Insert(const VarStr *k, VHandle &&vhandle) {
    // implicit copy
    VarStr *key = VarStr::New(k->len);
    uint8_t *ptr = (uint8_t *) key + sizeof(VarStr);
    memcpy(ptr, k->data, k->len);

    auto p = map.emplace(*key, std::move(vhandle));
    return &p.first->second;
  }

  Iterator IndexSearchIterator(const VarStr *k, int relation_id) {
    return Iterator(map.lower_bound(*k), map.end(), relation_id);
  }
  Iterator IndexSearchIterator(const VarStr *start, const VarStr *end, int relation_id) {
    return Iterator(map.lower_bound(*start), map.upper_bound(*end), relation_id);
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
