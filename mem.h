#ifndef MEM_H
#define MEM_H

#include <cstdlib>
#include <string>
#include <atomic>
#include <cstdio>
#include <functional>
#include <sys/mman.h>

#include "json11/json11.hpp"
#include "util.h"

namespace mem {

const int kNrCorePerNode = 8;

enum MemAllocType {
  GenericMemory,
  EpochQueueItem,
  EpochQueuePromise,
  Promise,
  Epoch,
  EpochQueuePool,
  RowEntityPool,
  VhandlePool,
  RegionPool,
  Coroutine,
  NumMemTypes,
};

const std::string kMemAllocTypeLabel[] = {
  "generic",
  "epoch queue item",
  "epoch queue promise",
  "promise",
  "epoch",
  "^pool:epoch queue",
  "^pool:row entity",
  "^pool:vhandle",
  "^pool:region",
  "coroutine",
};

class BasicPool {
  void *data;
  size_t len;
  void * head;
  size_t capacity;
  MemAllocType alloc_type;

 public:
  BasicPool() : data(nullptr), len(0) {}

  BasicPool(MemAllocType alloc_type, size_t chunk_size, size_t cap, int numa_node = -1);
  BasicPool(const BasicPool &rhs) = delete;
  ~BasicPool();

  void move(BasicPool &&rhs) {
    data = rhs.data;
    capacity = rhs.capacity;
    len = rhs.len;
    alloc_type = rhs.alloc_type;
    head = rhs.head;
    rhs.data = nullptr;
    rhs.head = nullptr;
    rhs.capacity = 0;
    rhs.len = 0;
  }

  void *Alloc();
  void Free(void *ptr);

  size_t total_capacity() const { return capacity; }
};

// Thread-Safe version
class Pool : public BasicPool {
  util::SpinLock lock;
 public:
  using BasicPool::BasicPool;
  void *Alloc() {
    auto _ = util::Guard(lock);
    return BasicPool::Alloc();
  }
  void Free(void *ptr) {
    auto _ = util::Guard(lock);
    BasicPool::Free(ptr);
  }
};

class Region {
  static const int kMaxPools = 20;
  // static const int kMaxPools = 12;
  Pool pools[32];
  size_t proposed_caps[32];
 public:
  Region() {
    for (int i = 0; i < kMaxPools; i++) {
      proposed_caps[i] = 1 << (25 - 5 - i);
    }
  }

  Region(const Region &) = delete;

  static int SizeToClass(size_t sz) {
    int idx = 64 - __builtin_clzl(sz - 1) - 5;
    if (__builtin_expect(idx >= kMaxPools, 0)) {
      fprintf(stderr, "Requested invalid size class %d\n", idx);
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
      new (&pools[i]) BasicPool(mem::RegionPool, 1 << (i + 5), proposed_caps[i], node);
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

  std::atomic_size_t offset;
  size_t limit;
  uint8_t *data;
  Deleter *deleters;

  std::memory_order ord = std::memory_order_relaxed;

 public:
  Brk() : offset(0), limit(0), data(nullptr), deleters(nullptr) {}
  Brk(void *p, size_t limit) : offset(0), limit(limit), data((uint8_t *) p),
                               deleters(nullptr) {}
  ~Brk();

  void move(Brk &&rhs) {
    data = rhs.data;
    limit = rhs.limit;
    offset.store(rhs.offset.load(std::memory_order_relaxed), std::memory_order_relaxed);
    deleters = rhs.deleters;
    rhs.deleters = nullptr;
    ord = rhs.ord;
  }

  void set_thread_safe(bool safe) {
    if (safe)
      ord = std::memory_order_seq_cst;
    else
      ord = std::memory_order_relaxed;
  }

  // This is a special New() function. It avoids memory allocation.
  static Brk *New(void *buf, size_t sz) {
    auto *p = (uint8_t *) buf;
    auto hdr_size = util::Align(sizeof(Brk), 16);
    return new (p) Brk(p + hdr_size, sz - hdr_size);
  }

  bool Check(size_t s) { return offset + s <= limit; }
  void Reset() { offset = 0; }

  void *Alloc(size_t s);
  void *Alloc(size_t s, std::function<void (void *)> deleter);
  uint8_t *ptr() const { return data; }
  size_t current_size() const { return offset; }
};

#define NewStackBrk(sz) mem::Brk::New(alloca(sz), sz)
#define INIT_ROUTINE_BRK(sz) go::RoutineScopedData _______(NewStackBrk(sz));

void *AllocFromRoutine(size_t sz);
void *AllocFromRoutine(size_t sz, std::function<void (void *)> deleter);

void PrintMemStats();
void *MemMap(mem::MemAllocType alloc_type, void *addr, size_t length, int prot,
             int flags, int fd, off_t offset);

static inline void *
MemMapAlloc(mem::MemAllocType alloc_type, size_t length)
{
  int flags = MAP_ANONYMOUS | MAP_PRIVATE;
  if (length >= 2 >> 20)
    flags |= MAP_HUGETLB;
  void *data = MemMap(alloc_type, nullptr, length,
                      PROT_READ | PROT_WRITE, flags, -1, 0);
  if (mlock(data, length) < 0) {
    fprintf(stderr, "WARNING: mlock() failed\n");
    perror("mlock");
  }
  return data;
}

long TotalMemoryAllocated();

}

std::string MemTypeToString(mem::MemAllocType alloc_type);

#endif /* MEM_H */
