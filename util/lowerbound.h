#ifndef UTIL_LOWERBOUND_H
#define UTIL_LOWERBOUND_H

#include <cstdint>

namespace util {

static inline uint64_t *FastLowerBound(uint64_t *start, uint64_t *end, uint64_t value)
{
  unsigned int len = end - start;
  if (len == 0) return start;

  unsigned int maxstep = 1 << (31 - __builtin_clz(len));
  unsigned int ret = (maxstep < len && start[maxstep] <= value) * (len - maxstep);

  __builtin_prefetch(end - 16);

  // printf("len %u middle %u ret %u\n", len, middle, ret);
  for (auto x = 11; x >= 0; x--) {
    auto stepping = 1 << x;
    if (stepping < maxstep) ret += (start[ret + stepping] <= value) * stepping;
  }
  return start[ret] <= value ? start + ret + 1 : start + ret;
}

}

#endif
