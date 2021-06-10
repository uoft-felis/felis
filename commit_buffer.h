#include <atomic>
#include "varstr.h"
#include "mem.h"

namespace felis {

// Commit buffer to deal with repeat updates within one transaction. Usually
// this is a per-transaction hashtable, however, in our case, we don't have a
// notion of commit, and we have to preserve this hashtable across multiple
// phases.
//
// So, our commit buffer is a per-epoch hashtable, it will be reset (in
// parallel) at the epoch boundary.

class VHandle;
class IndexInfo;

//shirley: is this the write buffer in the paper for read your writes?
class CommitBuffer {
 public:
  struct Entry {
    IndexInfo *index_info;
    uint32_t short_sid; // sid inside the epoch.
    std::atomic_int32_t wcnt;
    union {
      std::atomic<Entry *> dup = nullptr;
      VarStr *value;
    } u;
    std::atomic<Entry *> next = nullptr;

    Entry(IndexInfo *index_info, uint32_t sid) : index_info(index_info), short_sid(sid), wcnt(1) {}
  };
 private:
  std::atomic<Entry *> *ref_hashtable;
  unsigned long ref_hashtable_size;
  std::atomic<Entry *> *dup_hashtable;
  unsigned long dup_hashtable_size;

  std::atomic_uint64_t clear_refcnt; // 0 means all clear

  std::array<mem::Brk *,
             mem::ParallelAllocationPolicy::kMaxNrPools> entbrks;

  void EnsureReady();

 public:
  CommitBuffer();

  void Reset();
  void Clear(int core_id);
  bool AddRef(int core_id, IndexInfo *index_info, uint64_t sid);
  Entry *LookupDuplicate(IndexInfo *index_info, uint64_t sid);
};

using WriteSetDesc = CommitBuffer::Entry;

}
