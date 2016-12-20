// -*- c++ -*-

#ifndef UTIL_H
#define UTIL_H

#include <sys/types.h>
#include <functional>
#include <string>
#include <cassert>
#include <atomic>
#include <unistd.h>
#include <sched.h>
#include <pthread.h>

#include "gopp/gopp.h"

#define CACHELINE_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define __XCONCAT2(a, b) a ## b
#define CACHE_PADOUT \
  char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))

#ifndef likely
#define likely(x) __builtin_expect((x),1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x),0)
#endif

#ifndef container_of
#define container_of(ptr, type, member) ({			\
      (type *)((char *)ptr - offsetof(type, member)); })
#endif

namespace util {

// padded, aligned primitives
template <typename T, bool Pedantic = true>
class CacheAligned {
public:

  template <class... Args>
  CacheAligned(Args &&... args)
    : elem(std::forward<Args>(args)...)
  {
    if (Pedantic)
      assert(((uintptr_t)this % CACHELINE_SIZE) == 0);
  }

  T elem;
  CACHE_PADOUT;

  // syntactic sugar- can treat like a pointer
  inline T & operator*() { return elem; }
  inline const T & operator*() const { return elem; }
  inline T * operator->() { return &elem; }
  inline const T * operator->() const { return &elem; }

private:
  inline void
  __cl_asserter() const
  {
    static_assert((sizeof(*this) % CACHELINE_SIZE) == 0, "xx");
  }
} CACHE_ALIGNED;

// not thread-safe
//
// taken from java:
//   http://developer.classpath.org/doc/java/util/Random-source.html
class FastRandom {
public:
  FastRandom(unsigned long seed)
    : seed(0)
  {
    set_seed0(seed);
  }

  unsigned long next() {
    return ((unsigned long) next(32) << 32) + next(32);
  }

  uint32_t next_u32() {
    return next(32);
  }

  uint16_t next_u16() {
    return next(16);
  }

  /** [0.0, 1.0) */
  double next_uniform() {
    return (((unsigned long) next(26) << 27) + next(27)) / (double) (1L << 53);
  }

  char next_char() {
    return next(8) % 256;
  }

  char next_readable_char() {
    static const char readables[] = "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    return readables[next(6)];
  }

  std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++)
      s[i] = next_char();
    return s;
  }

  std::string next_readable_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++)
      s[i] = next_readable_char();
    return s;
  }

  unsigned long get_seed() {
    return seed;
  }

  void set_seed(unsigned long seed) {
    this->seed = seed;
  }

private:
  void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return (unsigned long) (seed >> (48 - bits));
  }

  unsigned long seed;
};

// link list headers. STL is too slow
struct ListNode {
  ListNode *prev, *next;

  void InsertAfter(ListNode *parent) {
    prev = parent;
    next = parent->next;
    parent->next->prev = this;
    parent->next = this;
  }

  void Remove() {
    prev->next = next;
    next->prev = prev;
    prev = next = nullptr; // detached
  }

  void Initialize() {
    prev = next = this;
  }
};

template <class... M>
class MixIn : public M... {};

// instance of a global object. So that we don't need the ugly extern.
template <class O> O &Instance();

// CPU pinning
static void PinToCPU(int cpu)
{
  // linux only
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu % sysconf(_SC_NPROCESSORS_CONF), &set);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &set);
  pthread_yield();
}

// #define TRACE_ENABLE

// Counter Stuff
template <int N>
class Counter {
  long cnt[N];
  std::string name;
public:
  Counter(const char *n) : name(n) {
    memset(cnt, 0, sizeof(long) * N);
  }
  ~Counter() {
#ifdef TRACE_ENABLE
    long sum = 0;
    for (int i = 0; i < N; i++) sum += cnt[i];
    fprintf(stderr, "%s: %ld\n", name.c_str(), sum);
#endif
  }
  void Increment() {
    cnt[go::Scheduler::CurrentThreadPoolId() - 1]++;
  }
};

}

#endif /* UTIL_H */
