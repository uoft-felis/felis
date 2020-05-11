#ifndef GC_H
#define GC_H

#include "util.h"
#include "mem.h"
#include "node_config.h"

namespace felis {

class VHandle;
struct GarbageBlockSlab;
struct GarbageBlock;

class GC {
  static std::array<GarbageBlockSlab *, NodeConfiguration::kMaxNrThreads> g_slabs;
  std::atomic<GarbageBlock *> collect_head = nullptr;
 public:
  uint64_t AddRow(VHandle *row, uint64_t epoch_nr);
  void RemoveRow(VHandle *row, uint64_t gc_handle);
  void PrepareGCForAllCores();
  void RunGC();

  static void InitPool();

  static bool IsDataGarbage(VHandle *row, VarStr *data);
  static void FreeIfGarbage(VHandle *row, VarStr *data, VarStr *next, size_t *nr_bytes);

  size_t Collect(VHandle *handle, uint64_t cur_epoch_nr, size_t limit, size_t *nr_bytes);
 private:
  size_t Process(VHandle *handle, uint64_t cur_epoch_nr, size_t limit, size_t *nr_bytes);

  static unsigned int g_gc_every_epoch;
};

}

namespace util {

using namespace felis;

template <> struct InstanceInit<GC> {
  static constexpr bool kHasInstance = true;
  static GC *instance;
};

}

#endif /* GC_H */
