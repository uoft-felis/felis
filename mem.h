#ifndef MEM_H
#define MEM_H

#include <cstdlib>
#include <string>
#include <mutex>
#include <cstdio>
#include <array>
#include <sys/mman.h>

#include "json11/json11.hpp"
#include "util/arch.h"
#include "util/locks.h"
#include "util/linklist.h"
#include "literals.h"
#include "felis_probes.h"

namespace mem {

constexpr size_t kNrCorePerNode = 8;

enum MemAllocType {
  GenericMemory,
  EpochQueueItem,
  EpochQueuePromise,
  Txn,
  Promise,
  Epoch,
  ContentionManagerPool,
  EntityPool,
  VhandlePool,
  IndexInfoPool,
  RegionPool,
  Coroutine,
  TransientPool,
  PersistentPool,
  NumMemTypes,
};

const std::string kMemAllocTypeLabel[] = {
  "generic",
  "epoch queue item",
  "epoch queue promise",
  "txn input and state",
  "promise",
  "epoch",
  "^pool:contention manager",
  "^pool:row entity",
  "^pool:vhandle",
  "^pool:index info",
  "^pool:region",
  "coroutine",
  "^pool:transient mem",
  "^pool:persistent mem",
};

struct PoolStatistics {
  long long used;
  long long watermark;
};

class WeakPool {
 protected:
  friend class ParallelPool;
  friend class ParallelRegion;
  friend void PrintMemStats();
  uintptr_t *freelist_dram; //shirley: if pool is allocating pmem, store free list separately.
  void *data;
  size_t len;
  void * head; //shirley: this is used differently depending if we have freelist_dram or not.
  size_t capacity;
  MemAllocType alloc_type;
  bool need_unmap;
  PoolStatistics stats;

 public:
  WeakPool() : data(nullptr), len(0), head(nullptr), capacity(0), need_unmap(false) {}

  WeakPool(MemAllocType alloc_type, size_t chunk_size, size_t cap, int numa_node = -1, bool use_pmem = false);
  WeakPool(MemAllocType alloc_type, size_t chunk_size, size_t cap, void *data, bool use_pmem = false);
  WeakPool(const WeakPool &rhs) = delete;
  WeakPool(WeakPool &&rhs) {
    data = rhs.data;
    capacity = rhs.capacity;
    len = rhs.len;
    alloc_type = rhs.alloc_type;
    head = rhs.head;
    stats = rhs.stats;
    need_unmap = rhs.need_unmap;
    freelist_dram = rhs.freelist_dram;

    rhs.data = nullptr;
    rhs.capacity = 0;
    rhs.head = nullptr;
    rhs.need_unmap = false;
    rhs.freelist_dram = nullptr;
  }
  ~WeakPool();

  WeakPool &operator=(WeakPool &&rhs) {
    if (this != &rhs) {
      this->~WeakPool();
      new (this) WeakPool(std::move(rhs));
    }
    return *this;
  }

  void *Alloc();
  void Free(void *ptr);

  size_t total_capacity() const { return capacity; }
  void *data_ptr() const { return data; }
  bool is_full() const { return head == nullptr; }
  bool is_empty() const { return stats.used == 0; }

  void Register();
};

// This checks ownership of the pointer
class BasicPool : public WeakPool {
  // shirley: set suppress_warning as static constexpr bc size of basic pool exceeds cache size
  static constexpr bool suppress_warning = false;
 public:
  using WeakPool::WeakPool;

  long CheckPointer(void *ptr);
  void *Alloc();
  void Free(void *ptr);

  void set_suppress_warning(bool suppress_warning) {
    // shirley: removed this variable bc size of basic pool exceeding cache size
    // this->suppress_warning = suppress_warning;
  }
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

static_assert(sizeof(BasicPool) <= CACHE_LINE_SIZE);

void InitTotalNumberOfCores(int nr_cores);

// Before we implement a region allocator, we need to implement a Slab
// allocator. Slab allocator is to make memory from different pools shared at
// the page granularity to reduce memory fragmentation. This is extremely useful
// for the partitioned skewed workload, where one core allocate all the memory.

void InitSlab(size_t mem);

// SlabPool can take care of chunks <= 512_K or chunks <= 16_M. For chunks larger
// than 512_K, SlabPool will ask for memory from the large metaslabs. These are
// 64_M in page size.
class Slab;
class SlabMemory;
class SlabPool {
  friend class ParallelRegion;
  util::GenericListNode<Slab> empty;
  util::GenericListNode<Slab> half_full;
  MemAllocType alloc_type;
  unsigned int numa_node;
  unsigned int nr_empty;
  unsigned int nr_buffer;
  unsigned int chunk_size;

  SlabMemory *slabmem_ptr;

  unsigned int slabmem_size;

  PoolStatistics stats;
 public:
  SlabPool(MemAllocType alloc_type, unsigned int chunk_size,
           unsigned int nr_buffer, int numa_node, SlabMemory *slab_mem_ptr = nullptr, unsigned int slab_mem_size = 0);

  void *Alloc();
  void Free(void *ptr);
  void Register();

  static constexpr size_t kSlabPageSize = 2_M;
  static constexpr size_t kLargeSlabPageSize = 64_M;
  static size_t PageSize(bool large_slab) {
    return large_slab ? kLargeSlabPageSize : kSlabPageSize;
  }

  static size_t PageSize(size_t chunk_size) {
    return PageSize(chunk_size >= 512_K);
  }

  bool is_large_slab() const { return chunk_size >= 512_K; }
  size_t metaslab_page_size() const { return PageSize(is_large_slab()); }

 private:
  Slab *RefillSlab();
  void ReturnSlab();
};

class ParallelAllocationPolicy {
 protected:
  static thread_local int g_affinity;
 public:
  static int g_nr_cores;
  static int g_core_shifting;
  static std::mutex *g_core_locks;

  static constexpr int kMaxNrPools = 64;
  // Affinity can override the current thread id. However, this has to be
  // exclusive among different cores. That's why we need the maximum number of
  // cores upfront.
  //
  // SetCurrentAffinity() will acquire a lock before setting the affinity for
  // the current thread. This is essential to keep the ParallelPool safe.
  static void SetCurrentAffinity(int aff);
  static int CurrentAffinity();
};

template <typename PoolType>
class ParallelAllocator : public ParallelAllocationPolicy {
 protected:
  struct ConsolidateFreeList {
    uint64_t dice = 0;
    uint64_t bitmap = 0;
    std::array<uintptr_t, kMaxNrPools> heads = {};
  };
  std::array<PoolType *, kMaxNrPools> pools;
  std::array<uintptr_t *, kMaxNrPools> free_lists;
  std::array<uintptr_t *, kMaxNrPools> free_tails;
  std::array<ConsolidateFreeList *, kMaxNrPools> csld_free_lists;
  size_t chunk_size;
  size_t total_cap;
  MemAllocType alloc_type;

  static const size_t kHeaderSize = sizeof(PoolType)
                                    + 2 * kMaxNrPools * sizeof(uintptr_t)
                                    + sizeof(ConsolidateFreeList);
 public:
  ParallelAllocator() : total_cap(0) {
    pools.fill(nullptr);
    free_lists.fill(nullptr);
    free_tails.fill(nullptr);
    csld_free_lists.fill(nullptr);
  }
  ParallelAllocator(const ParallelAllocator<PoolType>& rhs) = delete;
  ParallelAllocator(ParallelAllocator<PoolType> &&rhs) {
    pools = rhs.pools;
    free_lists = rhs.free_lists;
    free_tails = rhs.free_tails;
    csld_free_lists = rhs.csld_free_lists;
    chunk_size = rhs.chunk_size;
    total_cap = rhs.total_cap;
    alloc_type = rhs.alloc_type;

    rhs.total_cap = -1;
    rhs.pools.fill(nullptr);
    rhs.free_lists.fill(nullptr);
    rhs.free_tails.fill(nullptr);
    rhs.csld_free_lists.fill(nullptr);
  }

  size_t capacity() const { return total_cap; }
  PoolType *get_pool(int idx) const { return pools[idx]; }

  void Register() {
    for (auto i = 0; i < g_nr_cores; i++) pools[i]->Register();
  }
  void *Alloc() {
    auto cur = CurrentAffinity();
    auto csld = csld_free_lists[cur];
    auto &dice = csld->dice;
    if (csld->bitmap != 0) {
      auto n = csld->bitmap >> dice;
      dice = (n == 0) ? __builtin_ctzll(csld->bitmap) : __builtin_ctzll(n) + dice;

      auto &head = csld->heads[dice];
      auto p = (void *) head;
      head = *(uintptr_t *) head;

      if (head == 0) csld->bitmap &= ~(1 << dice);
      __builtin_prefetch((void *) head);
      return p;
    }

    return pools[cur]->Alloc();
  }

  //only used for ParallelBrk pool bc it takes in size as input
  void *Alloc(size_t sz) {
    auto cur = CurrentAffinity();
    auto csld = csld_free_lists[cur];
    auto &dice = csld->dice;
    if (csld->bitmap != 0) {
      auto n = csld->bitmap >> dice;
      dice = (n == 0) ? __builtin_ctzll(csld->bitmap) : __builtin_ctzll(n) + dice;

      auto &head = csld->heads[dice];
      auto p = (void *) head;
      head = *(uintptr_t *) head;

      if (head == 0) csld->bitmap &= ~(1 << dice);
      __builtin_prefetch((void *) head);
      return p;
    }
    return pools[cur]->Alloc(sz);
  }

  void Free(void *ptr, int alloc_core) {
    auto cur = CurrentAffinity();
    if (alloc_core < 0 || alloc_core >= kMaxNrPools) {
      fprintf(stderr, "alloc_core error, is %d. (shirley: maybe region_id was wrong for dram cache values)\n", alloc_core);
      std::abort();
    }
    // Trying to free to an extra pool. Then you must be on that core to free to
    // this pool!
    if (alloc_core >= g_nr_cores && cur != alloc_core) {
      fprintf(stderr, "alloc_core is not current core, is %d\n", alloc_core);
      std::abort();
    }
    if (cur == alloc_core) {
      pools[cur]->Free(ptr);
    } else {
      if (free_lists[cur][alloc_core] == 0)
        free_tails[cur][alloc_core] = (uintptr_t) ptr;
      *(uintptr_t *) ptr = free_lists[cur][alloc_core];
      free_lists[cur][alloc_core] = (uintptr_t) ptr;
    }
  }
  void Quiescence() {
    auto cur = CurrentAffinity();
    // We do not want to free them back to the pool right now, because the
    // objects in the list are cold now.
    auto csld = csld_free_lists[cur];
    for (int i = 0; i < g_nr_cores; i++) {
      uintptr_t tail = free_tails[i][cur];
      if (tail) {
        *(uintptr_t *) tail = csld->heads[i];
        csld->heads[i] = free_lists[i][cur];
        free_lists[i][cur] = free_tails[i][cur] = 0;
        csld->bitmap |= 1 << i;
      }
    }
  }
};

class ParallelPool : public ParallelAllocator<BasicPool> {
 public:
  ParallelPool() : ParallelAllocator() {}
  ParallelPool(MemAllocType alloc_type, size_t chunk_size, size_t total_cap);
  ~ParallelPool();
  ParallelPool(ParallelPool &&rhs) : ParallelAllocator(std::move(rhs)) {}

  ParallelPool &operator=(ParallelPool &&rhs) {
    if (&rhs != this) {
      this->~ParallelPool();
      new (this) ParallelPool(std::move(rhs));
    }
    return *this;
  }

  // You can add a dedicate pool.
  void AddExtraBasicPool(int core, size_t cap = 0, int node = -1);
};

class ParallelSlabPool : public ParallelAllocator<SlabPool> {
 public:
  ParallelSlabPool() : ParallelAllocator() {}
  ParallelSlabPool(MemAllocType alloc_type, size_t chunk_size, unsigned int buffer, bool use_pmem = false);
  ~ParallelSlabPool();
  ParallelSlabPool(ParallelSlabPool &&rhs) : ParallelAllocator(std::move(rhs)) {}

  ParallelSlabPool &operator=(ParallelSlabPool &&rhs) {
    if (&rhs != this) {
      this->~ParallelSlabPool();
      new (this) ParallelSlabPool(std::move(rhs));
    }
    return *this;
  }
};

class ParallelRegion {
  static const int kMaxPools = 20;
  // static const int kMaxPools = 12;
  ParallelSlabPool pools[32];
  size_t proposed_caps[32];
 public:
  ParallelRegion();
  ParallelRegion(const ParallelRegion &) = delete;

  static int SizeToClass(size_t sz) {
    int idx = 64 - __builtin_clzl(sz - 1) - 5;
    if (__builtin_expect(idx >= kMaxPools, 0)) {
      fprintf(stderr, "Requested invalid size class %d %lu\n", idx, sz);
      return -1;
    }
    return idx < 0 ? 0 : idx;
  }

  void ApplyFromConf(json11::Json conf);

  void set_pool_capacity(size_t sz, size_t cap) {
    int k = SizeToClass(sz);
    if (k < 0) std::abort();
    proposed_caps[k] = cap;
  }

  void InitPools(bool use_pmem = false);

  void *Alloc(size_t sz);
  void Free(void *ptr, int alloc_core, size_t sz);
  void Quiescence();

  void PrintUsageEachClass();
};

ParallelRegion &GetDataRegion(bool use_pmem = false);

class Brk;
class ParallelBrk;
class Brk {
  std::atomic_size_t offset;
  size_t limit;
  uint8_t *data;

  bool thread_safe;
  
  bool use_pmem;

 public:
  Brk() : offset(0), limit(0), data(nullptr), thread_safe(false), use_pmem(false) {}
  Brk(void *p, size_t limit, bool use_pmem = false) : offset(0), limit(limit), data((uint8_t *) p), thread_safe(false), use_pmem(use_pmem) {}
  ~Brk() {}

  Brk(Brk &&rhs) {
    data = rhs.data;
    limit = rhs.limit;
    offset.store(rhs.offset.load(std::memory_order_relaxed), std::memory_order_relaxed);
    thread_safe = rhs.thread_safe;
    use_pmem = rhs.use_pmem;

    rhs.offset = 0;
    rhs.limit = 0;
    rhs.data = nullptr;
  }

  Brk &operator =(Brk &&rhs) {
    if (this != &rhs) {
      this->~Brk();
      new (this) Brk(std::move(rhs));
    }
    return *this;
  }

  void set_thread_safe(bool safe) {
    thread_safe = safe;
  }

  // This is a special New() function. It avoids memory allocation.
  static Brk *New(void *buf, size_t sz) {
    auto *p = (uint8_t *) buf;
    auto hdr_size = util::Align(sizeof(Brk), 16);
    return new (p) Brk(p + hdr_size, sz - hdr_size);
  }

  bool Check(size_t s) { return offset + s <= limit; }
  void Reset() 
  { 
    offset = 0; 
  }

  void *Alloc(size_t s);
  uint8_t *ptr() const { return data; }
  size_t current_size() const { return offset; }
};

ParallelBrk &GetTransientPool();
ParallelRegion &GetPersistentPool();
void InitTransientPool(size_t t_mem);

class ParallelBrk : public ParallelAllocator<Brk> {

 public:
  ParallelBrk() : ParallelAllocator() {}
  // change parameters for this function
  ParallelBrk(size_t brk_pool_size, bool use_pmem = false);
  ~ParallelBrk();
  ParallelBrk(ParallelBrk &&rhs) : ParallelAllocator(std::move(rhs)) {}

  void Reset();
  ParallelBrk &operator=(ParallelBrk &&rhs) {
    if (&rhs != this) {
      this->~ParallelBrk();
      new (this) ParallelBrk(std::move(rhs));
    }
    return *this;
  }
};


#define NewStackBrk(sz) mem::Brk::New(alloca(sz), sz)
#define INIT_ROUTINE_BRK(sz) go::RoutineScopedData _______(NewStackBrk(sz));

void *AllocFromRoutine(size_t sz);

PoolStatistics GetMemStats(MemAllocType alloc_type);
void PrintMemStats();
void *AllocMemory(mem::MemAllocType alloc_type, size_t length,
                  int numa_node = -1, bool on_demand = false);

void *AllocPersistentMemory(mem::MemAllocType alloc_type, size_t length,
                  int numa_node = -1, bool on_demand = false);
long TotalMemoryAllocated();

}

std::string MemTypeToString(mem::MemAllocType alloc_type);

#endif /* MEM_H */
