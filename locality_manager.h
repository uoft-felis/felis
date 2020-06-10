#ifndef LOCALITY_MANAGER_H
#define LOCALITY_MANAGER_H

#include <atomic>

namespace felis {

class VHandle;

// Thread Safe
class LocalityManager {

  struct WeightDist {
    long weight;
    size_t nr_dist;
    long *dist;
    long *cores;
    long load;
    long weights_per_core[];
  } *per_core_weights[64]; // 64 maximum number of cores.

  static constexpr size_t kPrealloc = 1024;
  long dist_prealloc[kPrealloc];
  long core_prealloc[kPrealloc];

 public:
  LocalityManager();
  ~LocalityManager();

  void Balance();
  void PrintLoads();
  void Reset();
  void PlanLoad(int core, long delta);
  uint64_t GetScheduleCore(int core_affinity, int weight = 1);

  static VHandle *SelectRow(uint64_t bitmap, VHandle *const *it);

 private:
  void OffloadCore(int core, WeightDist &w, long limit);
};

}

#endif
