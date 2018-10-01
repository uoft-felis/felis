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

class MasstreeMap : public Masstree::basic_table<MasstreeDollyParam> {};
class MasstreeMapForwardScanIteratorImpl : public MasstreeMap::forward_scan_iterator_impl {
 public:
  using MasstreeMap::forward_scan_iterator_impl::forward_scan_iterator_impl;

  static void *operator new(size_t sz) {
    return mem::AllocFromRoutine(sizeof(MasstreeMapForwardScanIteratorImpl), [](void *p) {
        auto i = (MasstreeMapForwardScanIteratorImpl *) p;
        delete i;
      });
  }

  static void operator delete(void *p) {
    // noop;
  }
};

MasstreeIndex::Iterator::Iterator(MasstreeMapForwardScanIteratorImpl *scan_it,
                                  const VarStr *terminate_key,
                                  int relation_id, bool read_only,
                                  uint64_t sid)
    : end_key(terminate_key), it(scan_it), relation_id(relation_id),
      relation_read_only(read_only), sid(sid)
{
  AdaptKey();
  ti = GetThreadInfo();
  if (IsValid()) {
    if (ShouldSkip()) Next();
  }
}

void MasstreeIndex::Iterator::AdaptKey()
{
  if (!it->terminated) {
    // wrap the iterator
    auto s = it->key.full_string();
    cur_key.len = s.length();
    cur_key.data = (const uint8_t *) s.data();
  }
}

void MasstreeIndex::Iterator::Next()
{
  do {
    it->next(*ti);
    AdaptKey();
  } while (IsValid() && ShouldSkip());
}

bool MasstreeIndex::Iterator::IsValid() const
{

  if (end_key == nullptr)
    return !it->terminated;
  else
    return !it->terminated && !(*end_key < cur_key);
}

bool MasstreeIndex::Iterator::ShouldSkip()
{
  VarStr *obj = nullptr;
  auto handle = it->entry.value();
  if (!handle) return true;
  if (__builtin_expect(sid == std::numeric_limits<int64_t>::max(), 0)) {
    DTRACE_PROBE2(felis, chkpt_scan,
                  handle->nr_versions(),
                  handle->last_update_epoch());
  }
#ifdef CALVIN_REPLAY
  if (relation_read_only) {
    obj = handle->DirectRead();
  } else {
    obj = handle->ReadWithVersion(sid);
  }
#else
  obj = handle->ReadWithVersion(sid);
#endif

  return obj == nullptr;
}

void MasstreeIndex::Initialize(threadinfo *ti)
{
  auto tree = new MasstreeMap();
  tree->initialize(*ti);
  map = tree;
}

VHandle *MasstreeIndex::InsertOrCreate(const VarStr *k)
{
  VHandle *result;
  // result = this->Search(k);
  // if (result) return result;
  auto ti = GetThreadInfo();
  typename MasstreeMap::cursor_type cursor(*map, k->data, k->len);
  bool found = cursor.find_insert(*ti);
  if (!found) {
    auto h = new VHandle();
    asm volatile("": : :"memory"); // don't you dare to reorder the new after the commit!
    cursor.value() = h;
    nr_keys[go::Scheduler::CurrentThreadPoolId() - 1].add_cnt++;
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

MasstreeIndex::Iterator MasstreeIndex::IndexSearchIterator(const VarStr *k, int relation_id,
                                                           bool read_only,
                                                           uint64_t sid)
{
  auto p = map->find_iterator<MasstreeMapForwardScanIteratorImpl>(
      lcdf::Str(k->data, k->len), *GetThreadInfo());
  return Iterator(p, relation_id, read_only, sid);
}

MasstreeIndex::Iterator MasstreeIndex::IndexSearchIterator(const VarStr *start, const VarStr *end,
                                                           int relation_id, bool read_only,
                                                           uint64_t sid)
{
  auto p = map->find_iterator<MasstreeMapForwardScanIteratorImpl>(
      lcdf::Str(start->data, start->len), *GetThreadInfo());
  return Iterator(p, end, relation_id, read_only, sid);
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
