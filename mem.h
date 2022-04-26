#ifndef MEM_H
#define MEM_H

#include <cstdlib>
#include <string>
#include <mutex>
#include <cstdio>
#include <array>
#include <sys/mman.h>
#include <unordered_map>

#include <immintrin.h>

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
  VhandleFreelistPool,
  ExternalPmemPool,
  ExternalPmemFreelistPool,
  IndexInfoPool,
  RegionPool,
  Coroutine,
  TransientPool,
  TransientPmemPool,
  PersistentPool,
  PmemInfo,
  TxnInputLog,
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
  "^pool:vhandle_freelist",
  "^pool:external_pmem",
  "^pool:external_pmem_freelist",
  "^pool:index info",
  "^pool:region",
  "coroutine",
  "^pool:transient mem",
  "^pool:transient pmem",
  "^pool:persistent mem",
  "pmem_persist_info",
  "txn_input_log",
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
void TestSlabMmapAddress();

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
  void *Alloc(bool exception = false) {
    auto cur = CurrentAffinity();
    // shirley hack: for ParallelBrkWFree we have our own freelist & lock, don't need this csld_free_lists.
    if (exception) {
      return pools[cur]->Alloc();
    }
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

  void Free(void *ptr, int alloc_core, bool exception = false) {
    // shirley hack: for ParallelBrkWFree we can free from any core bc we have our own freelist & lock.
    if (exception) {
      pools[alloc_core]->Free(ptr);
      return;
    }
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
ParallelBrk &GetTransientPmemPool();
ParallelRegion &GetPersistentPool();
void InitTransientPool(size_t t_mem);
void InitTransientPmemPool(size_t t_mem);

class ParallelBrk : public ParallelAllocator<Brk> {

 public:
  ParallelBrk() : ParallelAllocator() {}
  // change parameters for this function
  ParallelBrk(MemAllocType alloc_type, size_t brk_pool_size,
              bool use_pmem = false);
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

struct PmemPersistInfo {
  uint64_t largest_sid = 0;
  uint64_t largest_epoch_logging = 0;
  uint64_t dptree_pmlog_ep = 0;
  uint64_t dptree_gv_ep = 0;
  uint64_t auto_inc_cnt[171] = {0};
  uint64_t auto_inc_cnt_2[171] = {0};
};

PmemPersistInfo *GetPmemPersistInfo();
void InitPmemPersistInfo();
void FlushPmemPersistInfo();

uint8_t **GetTxnInputLog();
void InitTxnInputLog();

class BrkWFree;
class ParallelBrkWFree;

class BrkWFree {
  util::MCSSpinLock lock_ring_buffer;
  uint8_t *data; // shirley: this should be calculated based on core id and fixe mmap address
  size_t offset; // shirley: cache this. move this to pmem file (in front of data)
  size_t limit; // shirley: cache this. move this to pmem file (in front of data)
  size_t block_size; // shirley: cache this. shirley: move this to pmem file (in front of data)
  
  uint64_t *ring_buffer; // shirley: this should be calculated based on core id and fixe mmap address 
  size_t initial_offset_freelist; // shirley: cache the initial_offset that we can't free beyond
  size_t initial_offset_pending_freelist; // shirley: cache the initial_offset that we can't alloc beyond
  // head of free list
  size_t offset_freelist; // shirley: cache this. move this to pmem file (in front of ring buffer)
  // tail of free list
  size_t offset_pending_freelist; // shirley: cache this. move this to pmem file (in front of ring buffer)
  size_t limit_ring_buffer; // shirley: cache this. move this to pmem file (in front of ring buffer)
  std::unordered_map<void *, bool> *freelist_hash = nullptr; // shirley: used for duplicate detection during recovery
  bool persist_pending_freelist = false;
  bool is_gc = false;
  
  // shirley: try metadata size of 256 so vhandles are in the same pmem granularity.
  static constexpr size_t metadata_size = 256; // 32; // 32 bytes metadata in front of data. offset 1 & 2, limit, block_size.
  static constexpr size_t metadata_size_ring_buffer = 24; // 24 bytes metadata in front of ring buffer. offset 1 & 2, limit.
  static constexpr size_t metadata_size_ring_buffer_persist_pending = 32; // 32 bytes metadata in front of ring buffer. offset 1 & 2, limit, offset_current.

  size_t *get_offset(bool first_slot = true) const {
    if (first_slot) {
      return (size_t *)data;
    }
    else {
      return (size_t *)(data + 8);
    }
  }
  size_t *get_limit() { return (size_t*) (data + 16); }
  size_t *get_block_size() { return (size_t*) (data + 24); }
  size_t *get_offsets_ring_buffer(bool first_slot = true) {
    if (first_slot) {
      return (size_t *)ring_buffer;
    }
    else {
      return (size_t *)((uint8_t *)ring_buffer + 8);
    }
  }
  size_t *get_limit_ring_buffer() { return (size_t *)((uint8_t*)ring_buffer + 16); }
  size_t *get_current_offset_pending_freelist() {
    if (!persist_pending_freelist) {
      printf("Not persisting pending freelist but trying to access current offset of pending freelist in pmem?!\n");
      std::abort();
    }
    return (size_t *)((uint8_t*)ring_buffer + 24);
  }
  

public:
  BrkWFree() : data(nullptr), ring_buffer(nullptr), persist_pending_freelist(false), is_gc(false), freelist_hash(nullptr) {}
  BrkWFree(void *d, void *rb, size_t limit, size_t limit_ring_buffer,
           size_t block_size, bool persist_pending_freelist = false,
           bool is_recovery = false)
      : data((uint8_t *)d), ring_buffer((uint64_t *)rb),
        persist_pending_freelist(persist_pending_freelist), is_gc(false),
        offset(0), limit(limit), block_size(block_size), offset_freelist(0),
        offset_pending_freelist(0), initial_offset_freelist(0),
        initial_offset_pending_freelist(0), freelist_hash(nullptr) {
    // shirley: initialize the inlined metadata.
    if (persist_pending_freelist) {
      this->limit_ring_buffer = limit_ring_buffer - 4;
    }
    else {
      this->limit_ring_buffer = limit_ring_buffer - 3;
    }
    // shirley: handle case if is recovery
    if (is_recovery) {
      uint64_t largest_sid = mem::GetPmemPersistInfo()->largest_sid;
      uint64_t last_epoch_nr = largest_sid >> 32;
      bool first_slot = !(last_epoch_nr % 2);
      offset = *get_offset(first_slot);
      size_t offsets_ring_buffer = *get_offsets_ring_buffer(first_slot);
      offset_freelist = offsets_ring_buffer >> 32;
      if (persist_pending_freelist) {
        size_t gc_tail = *get_current_offset_pending_freelist();
        if ((gc_tail >> 32) <= last_epoch_nr) {
          offset_pending_freelist = offsets_ring_buffer & (0x00000000FFFFFFFF);
        } else {
          offset_pending_freelist = gc_tail & 0x00000000FFFFFFFF;
        }
      }
      else {
        offset_pending_freelist = offsets_ring_buffer & (0x00000000FFFFFFFF);
      }
      initial_offset_freelist = offset_freelist;
      initial_offset_pending_freelist = offset_pending_freelist;
      
      // create hash for duplicate detection
      if (persist_pending_freelist) {
        freelist_hash = new std::unordered_map<void *, bool>;
        size_t freelist_duplicate_it = offsets_ring_buffer & (0x00000000FFFFFFFF);
        while (freelist_duplicate_it != offset_pending_freelist) {
          freelist_hash->insert({(void *)(*(get_ring_buffer() + freelist_duplicate_it)), true});
          freelist_duplicate_it++;
          if (freelist_duplicate_it == this->limit_ring_buffer) {
            freelist_duplicate_it = 0;
          }
        }
      }
      // printf("Recovering initial offsets (%lu, %lu)\n", initial_offset_freelist, initial_offset_pending_freelist);
    }
    else {
      *get_offset() = (size_t)0;
      *get_offset(false) = (size_t)0;
      *get_limit() = limit;
      *get_block_size() = block_size;

      *get_offsets_ring_buffer() = (size_t)0;
      *get_offsets_ring_buffer(false) = (size_t)0;
      *get_limit_ring_buffer() = this->limit_ring_buffer;
      if (persist_pending_freelist) {
        *get_current_offset_pending_freelist() = (size_t)0;
      }
      // shirley pmem shirley test : flush initial metadata
      _mm_clwb(data);
      _mm_clwb(ring_buffer);
    }
  }
  ~BrkWFree() {}

  BrkWFree(BrkWFree &&rhs) {
    data = rhs.data;
    ring_buffer = rhs.ring_buffer;
    persist_pending_freelist = rhs.persist_pending_freelist;
    
    offset = rhs.offset;
    limit = rhs.limit;
    block_size = rhs.block_size;
    
    offset_freelist = rhs.offset_freelist;
    offset_pending_freelist = rhs.offset_pending_freelist;
    initial_offset_freelist = rhs.initial_offset_freelist;
    initial_offset_pending_freelist = rhs.initial_offset_pending_freelist;
    limit_ring_buffer = rhs.limit_ring_buffer;

    rhs.data = nullptr;
    rhs.ring_buffer = nullptr;
  }

  BrkWFree &operator =(BrkWFree &&rhs) {
    if (this != &rhs) {
      this->~BrkWFree();
      new (this) BrkWFree(std::move(rhs));
    }
    return *this;
  }

  // void Reset() 
  // {
  //   offset = 0;
  //   offset_freelist = 0;
  //   // *get_offset() = 0;
  //   // *get_offset_freelist() = 0;
  // }

  // at end of epoch, persist head and tail offsets
  void persistOffsets(bool first_slot = true) {
    // persist brk offsets
    *get_offset(first_slot) = offset;
    // shirley pmem shirley test
    _mm_clwb(data);

    // persist freelist & pending freelist offsets
    size_t new_offsets = ((offset_freelist << 32) & 0xFFFFFFFF00000000) |
                          (offset_pending_freelist & 0x00000000FFFFFFFF);
    *get_offsets_ring_buffer(first_slot) = new_offsets;
    // shirley pmem shirley test
    _mm_clwb(ring_buffer);

    // printf("end of epoch updating initial offsets (%lu, %lu) -> (%lu, %lu)\n",
    //        initial_offset_freelist, initial_offset_pending_freelist,
    //        offset_freelist, offset_pending_freelist);
    // shirley: also update the initial offset (freelist / pending freelist) for next epoch in DRAM
    this->initial_offset_freelist = offset_freelist;
    this->initial_offset_pending_freelist = offset_pending_freelist;
  }

  // after major GC, update tail in DRAM (optional: PMem) to match GC current tail.
  void persistOffsetsGC() {
    if (!persist_pending_freelist) {
      printf("not persisting pending frelist but trying to persist its offset??\n");
      std::abort();
    }
    size_t current_offset_pending_freelist = (*get_current_offset_pending_freelist()) & 0x00000000FFFFFFFF;
    offset_pending_freelist = current_offset_pending_freelist;
    // shirley: we update the initial offset pending freelist here
    // bc GC deletes are committed to free list and can be re-allocated now.
    // printf("after GC updating initial offsets (%lu, %lu) -> (%lu, %lu)\n",
    //        initial_offset_freelist, initial_offset_pending_freelist,
    //        initial_offset_freelist, offset_pending_freelist);
    initial_offset_pending_freelist = offset_pending_freelist;
  }

  // at beginning of epoch, update GC current tail to match tail in DRAM.
  // NOTE: after recovery, we should update tail in DRAM based on GC current tail's ep number
  void updateOffsetsGC (uint64_t cur_ep) {
    if (!persist_pending_freelist) {
      printf("not persisting pending frelist but trying to update its offset??\n");
      std::abort();
    }
    size_t new_offset_gc = (cur_ep << 32) | (offset_pending_freelist & 0x00000000FFFFFFFF);
    *get_current_offset_pending_freelist() = new_offset_gc;
    //shirley pmem shirley test
    _mm_clwb(ring_buffer);
    _mm_sfence();
    // printf("beginning of epoch updating GC offset -> (%lu, %lu)\n", cur_ep, offset_pending_freelist & 0x00000000FFFFFFFF);
  }

  void *Alloc();
  void Free(void *ptr);
  uint8_t *ptr() const { return get_data(); }
  size_t current_size() const {
    return offset;
    // return *get_offset();
  }

  void set_is_gc(bool gc) {
    is_gc = gc;
  }

  uint8_t *get_data() const { return data + metadata_size; }
  uint64_t *get_ring_buffer() {
    if (persist_pending_freelist) {
      return (uint64_t *)((uint8_t *)ring_buffer + metadata_size_ring_buffer_persist_pending);
    }
    else {
      return (uint64_t *)((uint8_t *)ring_buffer + metadata_size_ring_buffer);
    }
  }
  size_t get_cached_offset() { return offset; }
  size_t get_cached_limit() { return limit; }
  size_t get_cached_block_size() { return block_size; }
  size_t get_cached_offset_freelist() { return offset_freelist; }
  size_t get_cached_offset_pending_freelist() { return offset_pending_freelist; }
  size_t get_cached_initial_offset_freelist() { return initial_offset_freelist; }
  size_t get_cached_initial_offset_pending_freelist() { return initial_offset_pending_freelist; }
  size_t get_cached_limit_ring_buffer() { return limit_ring_buffer; }
  // size_t get_cached_use_pmem() { return true; }
  // size_t get_cached_use_pmem_freelist() { return true; }
  bool get_cached_persist_pending_freelist() { return persist_pending_freelist; }
};

// shirley: this is the old implementation.
// class BrkWFree {
//   util::MCSSpinLock lock_freelist;
//   uint8_t *data; // shirley: this should be calculated based on core id and fixe mmap address
//   size_t offset; // shirley: cache this. move this to pmem file (in front of data)
//   size_t limit; // shirley: cache this. move this to pmem file (in front of data)
//   size_t block_size; // shirley: cache this. shirley: move this to pmem file (in front of data)
  
//   uint64_t *freelist; // shirley: this should be calculated based on core id and fixe mmap address 
//   // shirley: add limit and offset of freelist to pmem file (in front of freelist)
//   size_t offset_freelist; // shirley: cache this. move this to pmem file (in front of freelist)
//   size_t limit_freelist; // shirley: cache this. move this to pmem file (in front of freelist)
  
//   bool use_pmem; // data in pmem or dram
//   bool use_pmem_freelist; // freelist in pmem or dram
//   static constexpr size_t metadata_size = 32; // 12 bytes metadata in front of data. offset 1 & 2, limit, block_size.
//   static constexpr size_t metadata_size_freelist = 24; // 8 bytes metadata in front of freelist. offset 1 & 2, limit.

//   size_t *get_offset(bool first_slot = true) const {
//     if (first_slot) {
//       return (size_t *)data;
//     }
//     else {
//       return (size_t *)(data + 8);
//     }
//   }
//   size_t *get_limit() { return (size_t*) (data + 16); }
//   size_t *get_block_size() { return (size_t*) (data + 24); }
//   size_t *get_offset_freelist(bool first_slot = true) {
//     if (first_slot) {
//       return (size_t *)freelist;
//     }
//     else {
//       return (size_t *)((uint8_t *)freelist + 8);
//     }
//   }
//   size_t *get_limit_freelist() { return (size_t *)((uint8_t*)freelist + 16); }
  

// public:
//   BrkWFree() : data(nullptr), freelist(nullptr), use_pmem(false), use_pmem_freelist(false) {}
//   BrkWFree(void *d, void *f, size_t limit, size_t limit_freelist,
//            size_t block_size, bool use_pmem = false,
//            bool use_pmem_freelist = false, bool is_recovery = false)
//       : data((uint8_t *)d), freelist((uint64_t *)f), use_pmem(use_pmem),
//         use_pmem_freelist(use_pmem_freelist), offset(0), limit(limit),
//         block_size(block_size), limit_freelist(limit_freelist),
//         offset_freelist(0) {
//     // shirley: initialize the inlined metadata.
//     // shirley: handle case if is recovery
//     if (is_recovery) {
//       uint64_t largest_sid = mem::GetPmemPersistInfo()->largest_sid;
//       uint64_t last_epoch_nr = largest_sid >> 32;
//       bool first_slot = !(last_epoch_nr % 2);
//       offset = *get_offset(first_slot);
//       offset_freelist = *get_offset_freelist(first_slot);
//     }
//     else {
//       *get_offset() = (size_t)0;
//       *get_offset(false) = (size_t)0;
//       *get_limit() = limit;
//       *get_block_size() = block_size;
//       *get_offset_freelist() = (size_t)0;
//       *get_offset_freelist(false) = (size_t)0;
//       *get_limit_freelist() = limit_freelist;
//       // shirley pmem shirley test : flush initial metadata
//       // _mm_clwb(data);
//       // _mm_clwb(freelist);
//     }
//   }
//   ~BrkWFree() {}

//   BrkWFree(BrkWFree &&rhs) {
//     data = rhs.data;
//     freelist = rhs.freelist;
//     use_pmem = rhs.use_pmem;
//     use_pmem_freelist = rhs.use_pmem_freelist;

//     rhs.data = nullptr;
//     rhs.freelist = nullptr;
//   }

//   BrkWFree &operator =(BrkWFree &&rhs) {
//     if (this != &rhs) {
//       this->~BrkWFree();
//       new (this) BrkWFree(std::move(rhs));
//     }
//     return *this;
//   }

//   bool Check(size_t s) { 
//     // return (*get_offset() + s <= *get_limit());
//     return offset + s <= limit;
//   }
//   void Reset() 
//   {
//     offset = 0;
//     offset_freelist = 0;
//     // *get_offset() = 0;
//     // *get_offset_freelist() = 0;
//   }

//   void persistOffsets(bool first_slot = true) {
//     if (use_pmem) {
//       *get_offset(first_slot) = offset;
//       // shirley pmem shirley test
//       // _mm_clwb(data);
//     }
//     if (use_pmem_freelist) {
//       *get_offset_freelist(first_slot) = offset_freelist;
//       // shirley pmem shirley test
//       // _mm_clwb(freelist);
//     }
//   }

//   void *Alloc();
//   void Free(void *ptr);
//   uint8_t *ptr() const { return get_data(); }
//   size_t current_size() const {
//     return offset;
//     // return *get_offset();
//   }

//   uint8_t *get_data() const { return data + metadata_size; }
//   uint64_t *get_freelist() {
//     return (uint64_t *)((uint8_t *)freelist + metadata_size_freelist);
//   }
//   size_t get_cached_offset() { return offset; }
//   size_t get_cached_limit() { return limit; }
//   size_t get_cached_block_size() { return block_size; }
//   size_t get_cached_offset_freelist() { return offset_freelist; }
//   size_t get_cached_limit_freelist() { return limit_freelist; }
//   size_t get_cached_use_pmem() { return use_pmem; }
//   size_t get_cached_use_pmem_freelist() { return use_pmem_freelist; }
// };

class ParallelBrkWFree : public ParallelAllocator<BrkWFree> {

 public:
  ParallelBrkWFree() : ParallelAllocator() {}
  // change parameters for this function
  ParallelBrkWFree(MemAllocType alloc_type, MemAllocType freelist_alloc_type,
                   void *fixed_mmap_addr, size_t brk_pool_size,
                   size_t block_size, bool persist_pending_freelist = false, bool is_recovery = false);
  ~ParallelBrkWFree();
  ParallelBrkWFree(ParallelBrkWFree &&rhs) : ParallelAllocator(std::move(rhs)) {}

  // void Reset();

  void setIsGC(bool is_gc) {
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      pools[i]->set_is_gc(is_gc);
    }
  }

  // shirley: flush the offsets from cache to pmem file
  void persistOffsets(bool first_slot = true) {
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      pools[i]->persistOffsets(first_slot);
    }
  }

  void persistOffsetsGC() {
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      pools[i]->persistOffsetsGC();
    }
  }

  void updateOffsetsGC (uint64_t cur_ep) {
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      pools[i]->updateOffsetsGC(cur_ep);
    }
  }

  size_t TotalPoolSize() {
    size_t my_total = 0;
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      my_total += pools[i]->current_size();
    }
    return my_total;
  }

  ParallelBrkWFree &operator=(ParallelBrkWFree &&rhs) {
    if (&rhs != this) {
      this->~ParallelBrkWFree();
      new (this) ParallelBrkWFree(std::move(rhs));
    }
    return *this;
  }
};

ParallelBrkWFree &GetExternalPmemPool();
void InitExternalPmemPool();
void PersistExternalPmemPoolOffsets(bool first_slot = true);

PoolStatistics GetMemStats(MemAllocType alloc_type);
void PrintMemStats();
void *AllocMemory(mem::MemAllocType alloc_type, size_t length,
                  int numa_node = -1, bool on_demand = false);

void *AllocPersistentMemory(mem::MemAllocType alloc_type, size_t length,
                            int core_id = 0, int numa_node = -1,
                            void *addr = nullptr, bool on_demand = false);

void MapPersistentMemory(mem::MemAllocType alloc_type, int core_id, size_t length, void *addr = nullptr);
long TotalMemoryAllocated();

}

std::string MemTypeToString(mem::MemAllocType alloc_type);

#endif /* MEM_H */
