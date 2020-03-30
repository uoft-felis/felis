#ifndef LOCALITY_MANAGER_H
#define LOCALITY_MANAGER_H

#include <atomic>
#include <initializer_list>

#include "util.h"

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

  bool enable;
 public:
  LocalityManager();
  ~LocalityManager();

  void set_enable(bool v) { this->enable = v; }
  void Balance();
  void PrintLoads();
  void Reset();
  void PlanLoad(int core, long delta);
  uint64_t GetScheduleCore(int core_affinity);
  uint64_t GetScheduleCore(uint64_t bitmap, VHandle *const *it);

  uint64_t GetScheduleCore(uint64_t bitmap, std::initializer_list<VHandle *> con) {
    return GetScheduleCore(bitmap, con.begin());
  }

 private:
  void OffloadCore(int core, WeightDist &w, long limit);
};

}

#endif
