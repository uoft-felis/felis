#ifndef DPTREE_INDEX_IMPL_H
#define DPTREE_INDEX_IMPL_H

#include <cstdio>
#include <atomic>

#include "index_common.h"
#include "log.h"
#include "vhandle.h"
#include "index_info.h"


namespace felis {

class TableManager;

class DptreeIndex final : public Table {
 private:
  friend class TableManager;

  template <typename Func>
  IndexInfo *SearchOrCreateImpl(const VarStrView &k, Func f);
 public:
  void *tree;
  DptreeIndex(std::tuple<bool> conf) noexcept; // no configuration required

  IndexInfo *SearchOrCreate(const VarStrView &k, bool *created) override;
  IndexInfo *SearchOrCreate(const VarStrView &k) override;
  IndexInfo *Search(const VarStrView &k) override;
  IndexInfo *RecoverySearchOrCreate(const VarStrView &k, void *vhandle) override;

  Table::Iterator *IndexSearchIterator(const VarStrView &start, const VarStrView &end) override;
  Table::Iterator *IndexSearchIterator(const VarStrView &start) override;
  std::vector<IndexInfo *> SearchRange(const VarStrView &start, const VarStrView &end) override;
  void IndexMerge() override;
  void IndexLog() override;

};

}

#endif /* DPTREE_INDEX_IMPL_H */
