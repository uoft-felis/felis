#ifndef GC_DRAM_H
#define GC_DRAM_H

#include "util/objects.h"
#include "mem.h"
#include "node_config.h"

namespace felis {

class VHandle;
class IndexInfo;
struct GarbageBlockSlabDram;
struct GarbageBlockDram;

class GC_Dram {
  friend class GarbageBlockSlabDram;
  static std::array<GarbageBlockSlabDram *, NodeConfiguration::kMaxNrThreads> g_slabs;
  std::atomic<GarbageBlockDram *> collect_head = nullptr;

  struct {
    int nr_rows, nr_blocks;
    size_t nr_bytes;
    bool straggler;
    uint32_t padding[11];
  } stats[NodeConfiguration::kMaxNrThreads];

 public:
  uint64_t AddRow(IndexInfo *row, uint64_t epoch_nr);
  // void RemoveRow(IndexInfo *row, uint64_t gc_handle);
  void PrepareGCForAllCores();
  void RunGC();
  void PrintStats();
  void ClearStats() {
    for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
      memset(&stats[i], 0, 64);
    }
  }

  static void InitPool();

  size_t Collect(IndexInfo *handle, uint64_t cur_epoch_nr, size_t limit);

  static unsigned int g_gc_every_epoch;
  static bool g_lazy;
 private:
   size_t Process(IndexInfo *handle, uint64_t cur_epoch_nr, size_t limit);
};
}

namespace util {

using namespace felis;

template <> struct InstanceInit<GC_Dram> {
  static constexpr bool kHasInstance = true;
  static GC_Dram *instance;
};

}

#endif /* GC_DRAM_H */
