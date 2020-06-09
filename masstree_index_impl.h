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

  struct {
    uint64_t add_cnt;
    uint64_t del_cnt;
  } nr_keys [NodeConfiguration::kMaxNrThreads]; // scalable counting

  MasstreeMap *get_map() {
    // Let's reduce cache miss
    return (MasstreeMap *) ((uint8_t *) (this + 1));
  }

  template <typename Func>
  VHandle *SearchOrCreateImpl(const VarStr *k, Func f);
 public:
  static void ResetThreadInfo();

  MasstreeIndex(std::tuple<> conf) noexcept; // no configuration required

  static void *operator new(size_t sz);
  static void operator delete(void *p);

  VHandle *SearchOrCreate(const VarStr *k, bool *created) override;
  VHandle *SearchOrCreate(const VarStr *k) override;
  VHandle *Search(const VarStr *k) override ;

  Table::Iterator *IndexSearchIterator(const VarStr *start, const VarStr *end = nullptr) override;
  Table::Iterator *IndexReverseIterator(const VarStr *start, const VarStr *end = nullptr) override;

  size_t nr_unique_keys() const {
    size_t rs = 0;
    for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
      rs += nr_keys[i].add_cnt - nr_keys[i].del_cnt;
    }
    return rs;
  }
  void ImmediateDelete(const VarStr *k);
  void FakeDelete(const VarStr *k) {
    // delete an object, this won't be checkpointed
    nr_keys[go::Scheduler::CurrentThreadPoolId() - 1].del_cnt++;
  }
};

}

#endif /* MASSTREE_INDEX_IMPL_H */
