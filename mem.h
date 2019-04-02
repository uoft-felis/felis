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

class WeakPool {
 protected:
  friend class ParallelPool;
  friend void PrintMemStats();
  void *data;
  size_t len;
  void * head;
  size_t capacity;
  MemAllocType alloc_type;
  bool need_unmap;

  struct PoolStatistics {
    long long used;
    long long watermark;
  } stats;

 public:
  WeakPool() : data(nullptr), len(0), head(nullptr), capacity(0), need_unmap(false) {}

  WeakPool(MemAllocType alloc_type, size_t chunk_size, size_t cap, int numa_node = -1);
  WeakPool(MemAllocType alloc_type, size_t chunk_size, size_t cap, void *data);
  WeakPool(const WeakPool &rhs) = delete;
  WeakPool(WeakPool &&rhs) { *this = std::move(rhs); }
  ~WeakPool();

  WeakPool &operator=(WeakPool &&rhs) {
    std::swap(data, rhs.data);
    std::swap(capacity, rhs.capacity);
    std::swap(len, rhs.len);
    std::swap(alloc_type, rhs.alloc_type);
    std::swap(head, rhs.head);
    std::swap(stats, rhs.stats);
    std::swap(need_unmap, rhs.need_unmap);

    return *this;
  }

  void *Alloc();
  void Free(void *ptr);

  size_t total_capacity() const { return capacity; }

  void Register();
};

// This checks ownership of the pointer
class BasicPool : public WeakPool {
 public:
  using WeakPool::WeakPool;

  long CheckPointer(void *ptr);
  void *Alloc();
  void Free(void *ptr);
};

// Thread-Safe version
class Pool : public BasicPool {
  util::SpinLock lock;
 public:
  using BasicPool::BasicPool;

  Pool &operator=(Pool &&rhs) {
    auto &o = (WeakPool &)(*this);
    o = (WeakPool &&) rhs;

    return (*this);
  }

  void *Alloc() {
    auto _ = util::Guard(lock);
    return BasicPool::Alloc();
  }
  void Free(void *ptr) {
    auto _ = util::Guard(lock);
    BasicPool::Free(ptr);
  }
};

// Fast Parallel Pool, but need a quiescence
class ParallelPool {
  static constexpr int kMaxNrPools = 64;
  BasicPool *pools;
  uintptr_t *free_nodes;
  size_t chunk_size;
  size_t total_cap;
  MemAllocType alloc_type;

  static thread_local int g_affinity;
  static int g_nr_cores;
  static int g_cores_per_node;
  static int g_core_shifting;
 public:
  ParallelPool() : pools(nullptr), free_nodes(nullptr) {}
  ParallelPool(MemAllocType alloc_type, size_t chunk_size, size_t total_cap);
  ParallelPool(const ParallelPool& rhs) = delete;
  ParallelPool(ParallelPool &&rhs) { *this = std::move(rhs); }
  ~ParallelPool();

  ParallelPool &operator=(ParallelPool &&rhs) {
    std::swap(pools, rhs.pools);
    std::swap(free_nodes, rhs.free_nodes);
    std::swap(chunk_size, rhs.chunk_size);
    std::swap(total_cap, rhs.total_cap);
    std::swap(alloc_type, rhs.alloc_type);
    return *this;
  }

  void Register() {
    for (auto i = 0; i < g_nr_cores; i++) pools[i].Register();
  }

  // You can add a dedicate pool.
  void AddExtraBasicPool(int core, size_t cap = 0, int node = -1);

  void Prefetch();
  void *Alloc();
  void Free(void *ptr, int alloc_core);
  void Quiescence();

  // Affinity can override the current thread id. However, this has to be
  // exclusive among different cores. That's why we need the maximum number of
  // cores upfront.
  //
  // SetCurrentAffinity() will acquire a lock before setting the affinity for
  // the current thread. This is essential to keep the ParallelPool safe.
  static void SetCurrentAffinity(int aff);
  static void InitTotalNumberOfCores(int nr_cores, int core_shifting = 0);
  static int CurrentAffinity();
};

// In the future, we might also need a Plain Region that makes up of Pools
// instead of ParallelPools?
class ParallelRegion {
  static const int kMaxPools = 20;
  // static const int kMaxPools = 12;
  ParallelPool pools[32];
  size_t proposed_caps[32];
 public:
  ParallelRegion();

  ParallelRegion(const ParallelRegion &) = delete;

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
  }

  void InitPools();

  void *Alloc(size_t sz);
  void Free(void *ptr, int alloc_core, size_t sz);
  void Quiescence();
};

ParallelRegion &GetDataRegion();

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
void *MemMapAlloc(mem::MemAllocType alloc_type, size_t length, int numa_node = -1);

long TotalMemoryAllocated();

}

std::string MemTypeToString(mem::MemAllocType alloc_type);

#endif /* MEM_H */
