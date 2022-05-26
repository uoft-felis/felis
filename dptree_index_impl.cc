#include "dptree_index_impl.h"
// shirley pmem: include dptree
#include "dptree/include/concur_dptree.hpp"
#include <vector>
namespace felis {


typedef dptree::concur_dptree<uint64_t, entry_pair> dpt;

// shirley: do not use.
// typedef btreeolc::BTree<uint64_t, entry_pair>::unsafe_iterator dpt_it;

// struct DptreeIterator : public Table::Iterator, public dpt_it {
//   void Adapt();

//   void Next() override final;
//   bool IsValid() const override final;

//   DptreeIterator(VarStr &start) {
    
//   }

//   static void *operator new(size_t sz) {
//     // shirley: probe bytes allocated for masstree map iterator
//     // felis::probes::IndexSizeTotal{sz}();
//     return mem::AllocFromRoutine(sz);
//   }
//   static void operator delete(void *p) {}
// };

// void DptreeIterator::Adapt()
// {
//   if (this->node) {
//     // wrap the iterator
//     uint64_t s = ((dpt_it *) this)->key();
//     *(uint64_t *)(cur_key.data()) = s;
//     index_info = (IndexInfo *) ((dpt_it *) this)->value().first;
//   }
// }

// void DptreeIterator::Next()
// {
//   // uint64_t cur_key_old = *(uint64_t *)(cur_key.data());
//   ++((dpt_it)(*(dpt_it *)this));
//   Adapt();
//   // uint64_t cur_key_new = *(uint64_t *)(cur_key.data());
//   // printf("Iterator: cur_key_old = %lu, cur_key_new = %lu\n", 
//   //         cur_key_old,
//   //         cur_key_new);
// }

// bool DptreeIterator::IsValid() const
// {
//   return this->node && !(end_key < cur_key);
// }

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
  uint64_t _k = be64toh(*(uint64_t *)(k.data()));
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
  uint64_t _k = be64toh(*(uint64_t *)(k.data()));
  entry_pair value = entry_pair(0, 0);
  bool res = ((dpt *)tree)->lookup(_k, value);
  assert(res);
  return (IndexInfo *)(value.first);
}

IndexInfo *DptreeIndex::RecoverySearchOrCreate(const VarStrView &k, void *vhandle)
{
  return SearchOrCreateImpl(k, [=]() { return NewRow(vhandle); });
}

// shirley: do not use for dptree, bc need both buffer & base tree for scan.
Table::Iterator *DptreeIndex::IndexSearchIterator(const VarStrView &start, const VarStrView &end)
{
  return nullptr;
}

Table::Iterator *DptreeIndex::IndexSearchIterator(const VarStrView &start)
{
  return IndexSearchIterator(start, VarStrView(std::numeric_limits<uint16_t>::max(), nullptr));
}

std::vector<IndexInfo *> DptreeIndex::SearchRange(const VarStrView &start, const VarStrView &end) {
  std::vector<entry_pair> res;
  
  uint64_t key_start = be64toh(*(uint64_t *)(start.data()));
  uint64_t key_end = be64toh(*(uint64_t *)(end.data()));
  // uint64_t key_start = *(uint64_t *)(start.data());
  // uint64_t key_end = *(uint64_t *)(end.data());

  // printf("key_start = %lu, key_end = %lu\n", key_start, key_end);

  ((dpt *)tree)->lookup_range(key_start, key_end - key_start + 1, res);

  std::vector<IndexInfo *> result;
  for (auto p : res) {
    result.push_back((IndexInfo *) (p.first));
  }
  return result;
}

void DptreeIndex::IndexMerge() {
  ((dpt *)tree)->force_merge();
  while (((dpt *)tree)->is_merging());
  while (!(((dpt *)tree)->is_no_merge()));
  return;
}

void DptreeIndex::IndexLog() {
  bool from_dram = felis::Options::kDptreeDramLog;
  ((dpt *)tree)->construct_pmlog(from_dram);
  return;
}


}
