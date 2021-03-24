#ifndef GC_H
#define GC_H

#include "util/objects.h"
#include "mem.h"
#include "node_config.h"

namespace felis {

class VHandle;
struct GarbageBlockSlab;
struct GarbageBlock;

class GC {
  friend class GarbageBlockSlab;
  static std::array<GarbageBlockSlab *, NodeConfiguration::kMaxNrThreads> g_slabs;
  std::atomic<GarbageBlock *> collect_head = nullptr;

  struct {
    int nr_rows, nr_blocks;
    size_t nr_bytes;
    bool straggler;
    uint32_t padding[11];
  } stats[NodeConfiguration::kMaxNrThreads];

 public:
  uint64_t AddRow(VHandle *row, uint64_t epoch_nr);
  void RemoveRow(VHandle *row, uint64_t gc_handle);
  void PrepareGCForAllCores();
  void RunGC();
  void RunPmemGC();
  void PrintStats();
  void ClearStats() {
    for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
      memset(&stats[i], 0, 64);
    }
  }

  static void InitPool();

  static bool IsDataGarbage(VHandle *row, VarStr *data);
  bool FreeIfGarbage(VHandle *row, VarStr *data, VarStr *next);

  size_t Collect(VHandle *handle, uint64_t cur_epoch_nr, size_t limit);

  static unsigned int g_gc_every_epoch;
  static bool g_lazy;
 private:
  size_t Process(VHandle *handle, uint64_t cur_epoch_nr, size_t limit);
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
