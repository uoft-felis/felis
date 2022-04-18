#include "dptree_index_impl.h"
// shirley pmem: include dptree
#include "dptree/include/concur_dptree.hpp"
#include <vector>
namespace felis {


typedef dptree::concur_dptree<uint64_t, entry_pair> dpt;

DptreeIndex::DptreeIndex(std::tuple<bool> conf) noexcept
    : Table()
{
  enable_inline = std::get<0>(conf);
  // remove existing dptree file
  system("rm /mnt/pmem0/dp_pool");

  // initialize DPTree
  tree = new dptree::concur_dptree<uint64_t, entry_pair>();
}

template <typename Func>
IndexInfo *DptreeIndex::SearchOrCreateImpl(const VarStrView &k, Func f)
{
  uint64_t _k = *(uint64_t *)(k.data());
  entry_pair value = entry_pair(0, 0);
  bool res = ((dpt *)tree)->lookup(_k, value);
  if(!res) {
    IndexInfo *new_row = f();
    ((dpt *)tree)->insert(_k, entry_pair((uint64_t)new_row, (uint64_t)(new_row->vhandle_ptr())));
    return new_row;
  }
  return (IndexInfo *)(value.first);

  IndexInfo *result;
  return result;
}

IndexInfo *DptreeIndex::SearchOrCreate(const VarStrView &k)
{
  return SearchOrCreateImpl(k, [=]() { return NewRow(); });
}

IndexInfo *DptreeIndex::SearchOrCreate(const VarStrView &k, bool *created)
{
  *created = false;
  return SearchOrCreateImpl(k, [=]() { *created = true; return NewRow(); });
}

IndexInfo *DptreeIndex::Search(const VarStrView &k)
{
  uint64_t _k = *(uint64_t *)(k.data());
  entry_pair value = entry_pair(0, 0);
  bool res = ((dpt *)tree)->lookup(_k, value);
  assert(res);
  return (IndexInfo *)(value.first);
}

IndexInfo *DptreeIndex::RecoverySearchOrCreate(const VarStrView &k, void *vhandle)
{
  return SearchOrCreateImpl(k, [=]() { return NewRow(vhandle); });
}


Table::Iterator *DptreeIndex::IndexSearchIterator(const VarStrView &start, const VarStrView &end)
{
  Table::Iterator *it = nullptr;
  return it;
}

Table::Iterator *DptreeIndex::IndexSearchIterator(const VarStrView &start)
{
  return IndexSearchIterator(start, VarStrView(std::numeric_limits<uint16_t>::max(), nullptr));
}


}
