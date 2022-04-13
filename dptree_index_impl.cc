#include "dptree_index_impl.h"


namespace felis {


DptreeIndex::DptreeIndex(std::tuple<bool> conf) noexcept
    : Table()
{
  enable_inline = std::get<0>(conf);
  // initialize DPTree
}

template <typename Func>
IndexInfo *DptreeIndex::SearchOrCreateImpl(const VarStrView &k, Func f)
{
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
  IndexInfo *result = nullptr;
  return result;
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
