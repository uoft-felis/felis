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

  EpochGCRule() : last_gc_epoch(1) {}
  void operator()(VHandle *handle, uint64_t sid, uint64_t epoch_nr);
};

// TODO: Also need a scan based GC to clean up the old versions

}

#endif /* GC_H */
