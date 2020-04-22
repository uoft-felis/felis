#include "masstree_index_impl.h"

#include "masstree/build/config.h"
#include "masstree/masstree_insert.hh"
#include "masstree/masstree_remove.hh"
#include "masstree/masstree_tcursor.hh"
#include "masstree/masstree_print.hh"
#include "masstree/masstree_scan.hh"
#include "masstree/kvthread.hh"
#include "masstree/timestamp.hh"
#include "masstree/masstree.hh"

volatile mrcu_epoch_type active_epoch;
volatile mrcu_epoch_type globalepoch = 1;

kvtimestamp_t initial_timestamp;
kvepoch_t global_log_epoch;

namespace felis {

struct MasstreeDollyParam : public Masstree::nodeparams<15, 15> {
  typedef VHandle* value_type;
  // typedef VHandlePrinter value_print_type;
  typedef threadinfo threadinfo_type;
};

class MasstreeMap : public Masstree::basic_table<MasstreeDollyParam> {
 public:
  template <class MasstreeIteratorImpl>
  struct Iterator : public BaseRelation::Iterator,
                    public MasstreeIteratorImpl {
    threadinfo *ti;
    void Adapt();

    using MasstreeIteratorImpl::MasstreeIteratorImpl;

    void Next() override final;
    bool IsValid() const override final;

    static void *operator new(size_t sz) {
      return mem::AllocFromRoutine(sz);
    }
    static void operator delete(void *p) {}
  };

  using ForwardIterator = Iterator<forward_scan_iterator_impl>;
  using ReverseIterator = Iterator<reverse_scan_iterator_impl>;
};

template <class MasstreeIteratorImpl>
void MasstreeMap::Iterator<MasstreeIteratorImpl>::Adapt()
{
  if (!this->terminated) {
    // wrap the iterator
    auto s = ((MasstreeIteratorImpl *) this)->key.full_string();
    cur_key.len = s.length();
    cur_key.data = (const uint8_t *) s.data();
    vhandle = this->entry.value();
  }
}

template <class MasstreeIteratorImpl>
void MasstreeMap::Iterator<MasstreeIteratorImpl>::Next()
{
  this->next(*ti);
  Adapt();
}

template <class MasstreeIteratorImpl>
bool MasstreeMap::Iterator<MasstreeIteratorImpl>::IsValid() const
{
  if (end_key == nullptr)
    return !this->terminated;
  else
    return !this->terminated && !(*end_key < cur_key);
}

void MasstreeIndex::Initialize(threadinfo *ti)
{
  auto tree = new MasstreeMap();
  tree->initialize(*ti);
  map = tree;
}

VHandle *MasstreeIndex::SearchOrDefaultImpl(const VarStr *k,
                                            const SearchOrDefaultHandler &default_func)
{
  VHandle *result;
  // result = this->Search(k);
  // if (result) return result;
  auto ti = GetThreadInfo();
  typename MasstreeMap::cursor_type cursor(*map, k->data, k->len);
  bool found = cursor.find_insert(*ti);
  if (!found) {
    cursor.value() = default_func();
    // nr_keys[go::Scheduler::CurrentThreadPoolId() - 1].add_cnt++;
  }
  result = cursor.value();
  cursor.finish(1, *ti);
  assert(result != nullptr);
  return result;
}

VHandle *MasstreeIndex::Search(const VarStr *k)
{
  auto ti = GetThreadInfo();
  VHandle *result = nullptr;
  map->get(lcdf::Str(k->data, k->len), result, *ti);
  return result;
}


static __thread threadinfo *TLSThreadInfo;

threadinfo *MasstreeIndex::GetThreadInfo()
{
  if (TLSThreadInfo == nullptr)
    TLSThreadInfo = threadinfo::make(threadinfo::TI_PROCESS, go::Scheduler::CurrentThreadPoolId());
  return TLSThreadInfo;
}

void MasstreeIndex::ResetThreadInfo()
{
  TLSThreadInfo = nullptr;
}

BaseRelation::Iterator *MasstreeIndex::IndexSearchIterator(const VarStr *start, const VarStr *end)
{
  auto it = map->find_iterator<MasstreeMap::ForwardIterator>(
      lcdf::Str(start->data, start->len), *GetThreadInfo());
  it->set_end_key(end);
  it->Adapt();
  return it;
}

void MasstreeIndex::ImmediateDelete(const VarStr *k)
{
  auto ti = GetThreadInfo();
  typename MasstreeMap::cursor_type cursor(*map, k->data, k->len);
  bool found = cursor.find_locked(*ti);
  if (found) {
    VHandle *phandle = cursor.value();
    cursor.value() = nullptr;
    asm volatile ("": : :"memory");
    delete phandle;
  }
  cursor.finish(-1, *ti);
}

RelationManager::RelationManager()
{
  // initialize all relations
  ti = threadinfo::make(threadinfo::TI_MAIN, -1);
  for (int i = 0; i < kMaxNrRelations; i++) {
    relations[i].Initialize(ti);
  }
}

}
