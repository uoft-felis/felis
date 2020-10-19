#ifndef MASSTREE_INDEX_IMPL_H
#define MASSTREE_INDEX_IMPL_H

#include <cstdio>
#include <atomic>

#include "index_common.h"
#include "log.h"
#include "vhandle.h"

class threadinfo;

namespace felis {

class MasstreeMap;
class TableManager;

class MasstreeIndex final : public Table {
 private:
  friend class MasstreeMap;
  friend class TableManager;

  static threadinfo *GetThreadInfo();

  MasstreeMap *get_map() {
    // Let's reduce cache miss
    return (MasstreeMap *) ((uint8_t *) (this + 1));
  }

  template <typename Func>
  VHandle *SearchOrCreateImpl(const VarStr *k, Func f);
 public:
  static void ResetThreadInfo();

  MasstreeIndex(std::tuple<bool> conf) noexcept; // no configuration required

  static void *operator new(size_t sz);
  static void operator delete(void *p);

  VHandle *SearchOrCreate(const VarStr *k, bool *created) override;
  VHandle *SearchOrCreate(const VarStr *k) override;
  VHandle *Search(const VarStr *k) override ;

  Table::Iterator *IndexSearchIterator(const VarStr *start, const VarStr *end = nullptr) override;
  Table::Iterator *IndexReverseIterator(const VarStr *start, const VarStr *end = nullptr) override;

  void ImmediateDelete(const VarStr *k);

};

}

#endif /* MASSTREE_INDEX_IMPL_H */
