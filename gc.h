#ifndef GC_H
#define GC_H

#include "util.h"
#include "node_config.h"
#include "sqltypes.h"

namespace felis {

class VHandle;

// GC for deleted keys and vhandle
class DeletedGarbageHeads {
  util::ListNode garbage_heads[NodeConfiguration::kMaxNrThreads];
 public:
  DeletedGarbageHeads();
 public:
  void AttachGarbage(int rel_id, VarStr key, VHandle *vhandle, uint64_t epoch_nr);
  void CollectGarbage(uint64_t epoch_nr);
};

// GC on the vhandle
struct EpochGCRule {
  uint64_t last_gc_epoch;
  uint64_t min_of_epoch;

  EpochGCRule() : last_gc_epoch(0), min_of_epoch(0) {}

  template <typename VHandle>
  void operator()(VHandle &handle, uint64_t sid, uint64_t epoch_nr);
};

template <typename VHandle>
void EpochGCRule::operator()(VHandle &handle, uint64_t sid, uint64_t ep)
{
  if (ep > last_gc_epoch) {
    // gaurantee that we're the *first one* to garbage collect at the *epoch boundary*.
    handle.GarbageCollect();
    // DTRACE_PROBE3(dolly, versions_per_epoch_on_gc, &handle, ep - 1, handle.nr_versions());
    min_of_epoch = sid;
    last_gc_epoch = ep;
  }

  if (min_of_epoch > sid) min_of_epoch = sid;
}


// TODO: Also need a scan based GC to clean up the old versions

}

#endif /* GC_H */
