// -*- c++ -*-
#ifndef UTIL_ARCH_H
#define UTIL_ARCH_H

#include <utility>

#ifndef likely
#define likely(x) __builtin_expect((x),1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x),0)
#endif

namespace util {

// padded, aligned primitives
template <typename T>
struct CacheAligned : public T {
 public:

  template <class... Args>
  CacheAligned(Args &&... args) : T(std::forward<Args>(args)...) {}
} __attribute__((aligned(CACHE_LINE_SIZE)));

// Prefetch a lot of pointers
template <typename Iterator>
static inline void Prefetch(Iterator begin, Iterator end)
{
  for (auto p = begin; p != end; ++p) {
    __builtin_prefetch(*p);
  }
}

static inline void Prefetch(std::initializer_list<void *> arg)
{
  Prefetch(arg.begin(), arg.end());
}

static inline size_t Align(size_t x, size_t a = 16)
{
  return a * ((x - 1) / a + 1);
}

}

#endif
