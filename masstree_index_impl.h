#ifndef MASSTREE_INDEX_IMPL_H
#define MASSTREE_INDEX_IMPL_H

#include <cstdio>
#include <atomic>

#include "index_common.h"
#include "log.h"
#include "vhandle.h"
#include "index_info.h"

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
  IndexInfo *SearchOrCreateImpl(const VarStrView &k, Func f);
 public:
  static void ResetThreadInfo();

  MasstreeIndex(std::tuple<bool> conf) noexcept; // no configuration required

  static void *operator new(size_t sz);
  static void operator delete(void *p);

  IndexInfo *SearchOrCreate(const VarStrView &k, bool *created) override;
  IndexInfo *SearchOrCreate(const VarStrView &k) override;
  IndexInfo *Search(const VarStrView &k) override;
  IndexInfo *RecoverySearchOrCreate(const VarStrView &k, void *vhandle) override;

  Table::Iterator *IndexSearchIterator(const VarStrView &start, const VarStrView &end) override;
  Table::Iterator *IndexSearchIterator(const VarStrView &start) override;
  Table::Iterator *IndexReverseIterator(const VarStrView &start, const VarStrView &end) override;
  Table::Iterator *IndexReverseIterator(const VarStrView &start) override;

  void ImmediateDelete(const VarStrView &k);
};

}

#endif /* MASSTREE_INDEX_IMPL_H */
