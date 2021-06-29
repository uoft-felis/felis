#ifndef THRESHOLD_AUTOTUNE_H
#define THRESHOLD_AUTOTUNE_H

#include <cstdint>
#include <limits>
#include "log.h"

namespace felis {

class ThresholdAutoTuneController {
  long thre = 0;
  uint64_t nr_splitted = std::numeric_limits<uint32_t>::max();
  uint64_t exec_time = std::numeric_limits<uint32_t>::max();
 public:
  // Cautious: x and y shouldn't be too large, or we'll overflow.
  static int FuzzyCompare(uint64_t x, uint64_t y) {
    if (y == 0) {
      return (x > 0) ? 1 : 0;
    }
    uint64_t r = 128ULL * x / y;
    if (r < 120) return -1;
    else if (r < 136) return 0;
    else return 1;
  }
  long GetNextThreshold(long current_thre, uint64_t current_nr_splitted, uint64_t current_exec_time) {
    logger->info("Autotune nr_split {}->{} exec {}->{}",
                 nr_splitted, current_nr_splitted, exec_time, current_exec_time);
    int spl_cmp = FuzzyCompare(nr_splitted, current_nr_splitted);
    if (spl_cmp == 1) {
      if (FuzzyCompare(exec_time, current_exec_time) != -1) {
        thre = current_thre;
        nr_splitted = current_nr_splitted;
        exec_time = current_exec_time;
        // So far current_thre is the best, let's try a larger threshold!
        return std::min(thre * 2, long{std::numeric_limits<int32_t>::max()});
      } else {
        // Backoff, because the previous solution is better
        if (thre != current_thre)
          return thre;
        else
          return thre / 2;
      }
    } else if (spl_cmp == -1) {
      logger->warn("threshold tuning {}->{} but the nr_splitted {}->{}",
                   thre, current_thre, nr_splitted, current_nr_splitted);
      return thre;
    } else {
      // current_thre doesn't make a difference in nr_splitted, let's try a
      // even larger value.
      return current_thre * 2;
    }
  }
};

}

#endif /* THRESHOLD_AUTOTUNE_H */
