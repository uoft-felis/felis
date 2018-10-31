#ifndef MEM_H
#define MEM_H

#include <cstdlib>
#include <string>
#include <atomic>
#include <cstdio>
#include <functional>
#include "json11/json11.hpp"

namespace mem {

const int kNrCorePerNode = 8;

class Pool {
  void *data;
  size_t len;
  std::atomic<void *> head;
  size_t capacity;
  unsigned char __pad__[32];

 public:
  Pool() : data(nullptr), len(0) {}

  Pool(size_t chunk_size, size_t cap, int numa_node = -1);

  Pool(const Pool &rhs) = delete;
  ~Pool();

  void *Alloc();
  void Free(void *ptr);

  size_t total_capacity() const { return capacity; }
};

static_assert(sizeof(Pool) == 64, "Pool object size is not 64 bytes");

class Region {
  static const int kMaxPools = 20;
  // static const int kMaxPools = 12;
  Pool pools[32];
  size_t proposed_caps[32];
 public:
  Region() {
    for (int i = 0; i < kMaxPools; i++) {
      proposed_caps[i] = 1 << (27 - 5 - i);
    }
  }

  Region(const Region &) = delete;

  static int SizeToClass(size_t sz) {
    int idx = 64 - __builtin_clzl(sz - 1) - 5;
    if (__builtin_expect(idx >= kMaxPools, 0)) {
      std::abort();
    }
    return idx < 0 ? 0 : idx;
  }

  void ApplyFromConf(json11::Json conf);

  void set_pool_capacity(size_t sz, size_t cap) {
    proposed_caps[SizeToClass(sz)] = cap;
    fprintf(stderr, "%lu, bin %d, cap %lu, estimate size %.1lfMB\n", sz,
	    SizeToClass(sz), cap,
	    (1 << (SizeToClass(sz) + 5)) * cap * 1. / 1024 / 1024);
  }

  void InitPools(int node = -1) {
    for (int i = 0; i < kMaxPools; i++) {
      new (&pools[i]) Pool(1 << (i + 5), proposed_caps[i], node);
    }
  }

  void *Alloc(size_t sz);
  void Free(void *ptr, size_t sz);
};

using ThreadLocalRegion = Region;

void InitThreadLocalRegions(int tot);
ThreadLocalRegion &GetThreadLocalRegion(int idx);

int CurrentAllocAffinity();
void SetThreadLocalAllocAffinity(int h);

class Brk {
  struct Deleter {
    std::function<void (void *)> del_f;
    void *p;
    Deleter *next;
  };

  size_t offset;
  size_t limit;
  uint8_t *data;
  Deleter *deleters;

 public:
  Brk() : offset(0), limit(0), data(nullptr), deleters(nullptr) {}
  Brk(void *p, size_t limit) : offset(0), limit(limit), data((uint8_t *) p), deleters(nullptr) {}
  ~Brk();

  // This is a special New() function. It avoids memory allocation.
  static Brk *New(void *buf, size_t sz) {
    auto *p = (uint8_t *) buf;
    return new (p) Brk(p + sizeof(Brk), sz - sizeof(Brk));
  }

  void *Alloc(size_t s);
  void *Alloc(size_t s, std::function<void (void *)> deleter);
  uint8_t *ptr() const { return data; }
};

#define NewStackBrk(sz) mem::Brk::New(alloca(sz), sz)
#define INIT_ROUTINE_BRK(sz) go::RoutineScopedData _______(NewStackBrk(sz));

void *AllocFromRoutine(size_t sz);
void *AllocFromRoutine(size_t sz, std::function<void (void *)> deleter);

}

#endif /* MEM_H */
