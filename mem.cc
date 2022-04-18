#include <cassert>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "mem.h"
#include "json11/json11.hpp"
#include "log.h"
#include "util/linklist.h"
#include "util/locks.h"
#include "util/arch.h"
#include "util/os.h"
#include "gopp/gopp.h"
#include "literals.h"

#include "opts.h"

namespace mem {

  static std::atomic_llong g_mem_tracker[NumMemTypes];
  static std::mutex g_ps_lock;
  static std::vector<PoolStatistics *> g_ps[NumMemTypes];

  // keep track of number of pmem files we have for each type
  static std::atomic_uint memAllocTypeCount[NumMemTypes];

  WeakPool::WeakPool(MemAllocType alloc_type, size_t chunk_size, size_t cap,
                     int numa_node, bool use_pmem)
      : WeakPool(alloc_type, chunk_size, cap, AllocMemory(alloc_type, cap * chunk_size, numa_node))
  {
    need_unmap = true;
  }

  WeakPool::WeakPool(MemAllocType alloc_type, size_t chunk_size, size_t cap, void *data, bool use_pmem)
      : data(data), len(cap * chunk_size), capacity(cap), alloc_type(alloc_type), need_unmap(false)
  {
    head = data;

#if 0
  fprintf(stderr, "Initializing memory pool %s, %lu objs, each %lu bytes\n",
          kMemAllocTypeLabel[alloc_type].c_str(),
          cap, chunk_size);
#endif

    // shirley FIX: freelist is in pmem, each chunk points to next (i.e. freelist stored within free blocks)
    if (!use_pmem) {
      freelist_dram = nullptr;
      for (size_t i = 0; i < cap; i++) {
        uintptr_t p = (uintptr_t) head + i * chunk_size;
        uintptr_t next = p + chunk_size;
        if (i == cap - 1) next = 0;
        *(uintptr_t *) p = next;
      }
    }
    else {
      freelist_dram = (uintptr_t*)AllocMemory(GenericMemory, cap * sizeof(uintptr_t));
      if (!freelist_dram) {
        printf("WeakPool: couldn't allocate freelist in dram!\n");
        std::abort();
      }
      head = freelist_dram;
      for (size_t i = 0; i < cap; i++) {
        freelist_dram[i] = (uintptr_t) &freelist_dram[i+1];
        if (i == cap - 1) freelist_dram[i] = 0;
      }
    }
    memset(&stats, 0, sizeof(PoolStatistics));
  }

  WeakPool::~WeakPool()
  {
    if (need_unmap)
      munmap(data, len);
  }

  void WeakPool::Register()
  {
    std::lock_guard _(g_ps_lock);
    g_ps[int(alloc_type)].emplace_back(&stats);
  }

  void *WeakPool::Alloc()
  {
    if (!freelist_dram) {
      void *r = nullptr, *next = nullptr;

      r = head;
      if (r == nullptr) {
        return r;
      }

      next = (void *)*(uintptr_t *)r;
      head = next;

      stats.used += len / capacity;
      stats.watermark = std::max(stats.used, stats.watermark);

      return r;
    }
    else {
      void *r = nullptr, *next = nullptr;

      r = head;
      if (r == nullptr) {
        return r;
      }

      next = (void *)*(uintptr_t *)r;
      head = next;

      stats.used += len / capacity;
      stats.watermark = std::max(stats.used, stats.watermark);

      unsigned int chunk_size = len / capacity;
      unsigned int chunk_position = (uintptr_t *)r-(uintptr_t *)freelist_dram;
      void *alloc_chunk = ((char*)data) + (((uintptr_t *)r-(uintptr_t *)freelist_dram)*chunk_size);
      return alloc_chunk;
    }
  }

  void WeakPool::Free(void *ptr)
  {
    int chunk_size = len / capacity;

    if (!freelist_dram) {
      *(uintptr_t *)ptr = (uintptr_t)head;
      head = ptr;
    } 
    else {
      int i = ((char*)ptr - (char*)data) / chunk_size;
      freelist_dram[i] = (uintptr_t)head;
      head = &freelist_dram[i];
    }

    stats.used -= chunk_size;
  }

  long BasicPool::CheckPointer(void *ptr)
  {
    if (ptr == nullptr) {
      if (!suppress_warning) fprintf(stderr, "pointer ptr is nullptr\n");
      return -1;
    }
    if (ptr < data || ptr >= (uint8_t *) data + len) {
      if (!suppress_warning)
        fprintf(stderr, "%p is out of bounds %p - %p\n",
                ptr, data, (uint8_t *) data + len);
      printf("freelist_dram: %p\n", freelist_dram);
      std::abort();
    }
    size_t off = (uint8_t *) ptr - (uint8_t *) data;
    size_t maxnr = len / capacity;
    if (off % maxnr != 0) {
      if (!suppress_warning)
        fprintf(stderr, "%p is not aligned. %p with chunk_size %lu\n",
                ptr, data, len / capacity);
      std::abort();
    }
    return off / maxnr;
  }

  void *BasicPool::Alloc()
  {
    void *ptr = WeakPool::Alloc();
    if (!suppress_warning && ptr == nullptr)
      fprintf(stderr, "%s memory pool is full, returning nullptr\n",
              kMemAllocTypeLabel[alloc_type].c_str());
    CheckPointer(ptr);
    return ptr;
  }

  void BasicPool::Free(void *ptr)
  {
    CheckPointer(ptr);
    WeakPool::Free(ptr);
  }

  thread_local int ParallelAllocationPolicy::g_affinity = -1;

  int ParallelAllocationPolicy::g_nr_cores = 0;
  int ParallelAllocationPolicy::g_core_shifting = 0;
  std::mutex * ParallelAllocationPolicy::g_core_locks;

  void InitTotalNumberOfCores(int nr_cores)
  {
    ParallelAllocationPolicy::g_nr_cores = nr_cores;
    ParallelAllocationPolicy::g_core_locks = new std::mutex[nr_cores];
  }

  void ParallelAllocationPolicy::SetCurrentAffinity(int aff)
  {
    if (g_affinity != -1) {
      g_core_locks[g_affinity].unlock();
    }
    if (aff >= g_nr_cores || aff < -1) {
      std::abort();
    }
    if (aff != -1) {
      g_core_locks[aff].lock();
    }
    g_affinity = aff;
  }

  int ParallelAllocationPolicy::CurrentAffinity()
  {
    auto aff = g_affinity == -1 ? go::Scheduler::CurrentThreadPoolId() - 1 : g_affinity;
    if (aff < 0 || aff >= kMaxNrPools) {
      std::abort();
    }
    return aff;
  }

  class Slab : public util::GenericListNode<Slab> {
    friend class SlabPool;
    BasicPool pool;

    Slab(util::GenericListNode<Slab> *qhead, MemAllocType alloc_type, size_t chunk_size, void *p, bool use_pmem = false) {
      InsertAfter(qhead);
      pool = BasicPool(alloc_type, chunk_size, SlabPool::PageSize(chunk_size) / chunk_size, p, use_pmem);
    }
  };

  class MetaSlab : public util::GenericListNode<MetaSlab> {
    friend class SlabPool;
    friend class SlabMemory;
    bool large_slab = false;
    uint32_t alloc_bitmap;
    uint8_t *ptr;
    uint8_t slabs[32 * sizeof(Slab)];

    MetaSlab(uint8_t *ptr) : ptr(ptr), alloc_bitmap(0) {}

    void *AllocSlab();
    void FreeSlab(void *ptr);
  };

  static_assert(SlabPool::kLargeSlabPageSize / SlabPool::kSlabPageSize == 32);

  struct SlabMemory {
    Pool pool;
    uint8_t *p; //whole memory allocated
    uint64_t data_offset;
    uint64_t data_len;
    uint64_t page_size;
    util::SpinLock half_full_lock;
    util::GenericListNode<MetaSlab> half_full;

    bool Contains(void *ptr) {
      return ptr > p && ptr < p + data_len;
    }

    MetaSlab *GetMetaSlab(void *slab) {
      auto idx = ((uint8_t *) slab - p) / sizeof(MetaSlab);
      return ((MetaSlab *) p) + idx;
    }

    Slab *GetSlab(void *ptr, bool large_slab) {
      size_t off = ((uint8_t *) ptr - p) - data_offset;
      auto idx = off / SlabPool::kLargeSlabPageSize;
      auto slab_idx = large_slab ? 0 : (off % SlabPool::kLargeSlabPageSize) / SlabPool::kSlabPageSize;
      return ((Slab *) ((MetaSlab *) p + idx)->slabs) + slab_idx;
    }

    MetaSlab *NewMetaSlab();
    void DestroyMetaSlab(MetaSlab *);

    void *AllocSlab(bool large_slab, void *&data_ptr);
    void FreeSlab(void *ptr);
  };

  static SlabMemory *g_slabmem;
  static SlabMemory *g_slabpmem;
  uint8_t *pmem_addr_fixed = (uint8_t *)0x7e0000000000; // nullptr; // (uint8_t *)0x7e0000000000;

  void InitSlab(size_t memsz)
  {
    auto nr_numa_nodes = ParallelAllocationPolicy::g_nr_cores / kNrCorePerNode;
    g_slabmem = new SlabMemory[nr_numa_nodes];
    //shirley: maybe we don't need nr_numa_nodes for pmem
    g_slabpmem = new SlabMemory[nr_numa_nodes];
    memsz /= nr_numa_nodes;

    std::vector<std::thread> tasks;
    for (int n = 0 ; n < nr_numa_nodes; n++) {
      tasks.emplace_back(
          [memsz, n]() {
            auto &m = g_slabmem[n];
            //shirley: can try changing this to alloc persistent memory
            //m.p = (uint8_t *) AllocPersistentMemory(mem::GenericMemory, memsz, n);
            m.p = (uint8_t *)AllocMemory(mem::GenericMemory, memsz, n);
            auto nr_metaslabs = ((memsz - 1) / SlabPool::kLargeSlabPageSize + 1);
            m.data_offset = util::Align(nr_metaslabs * sizeof(MetaSlab), SlabPool::kLargeSlabPageSize);
            m.data_len = memsz;

            nr_metaslabs -= m.data_offset / SlabPool::kLargeSlabPageSize;
            m.pool = Pool(mem::GenericMemory, sizeof(MetaSlab), nr_metaslabs, m.p);
            m.pool.set_suppress_warning(true);
            m.half_full.Initialize();

            printf("Initialized %lu metaslabs on numa node %d, memsz = %lu bytes\n",
                   nr_metaslabs, n, memsz);

            //shirley: also init pmem
            auto &m_pmem = g_slabpmem[n];
            m_pmem.p = (uint8_t *)AllocPersistentMemory(mem::GenericMemory, memsz, n, n, pmem_addr_fixed);
            nr_metaslabs = ((memsz - 1) / SlabPool::kLargeSlabPageSize + 1);
            m_pmem.data_offset = util::Align(nr_metaslabs * sizeof(MetaSlab), SlabPool::kLargeSlabPageSize);
            m_pmem.data_len = memsz;

            nr_metaslabs -= m_pmem.data_offset / SlabPool::kLargeSlabPageSize;
            m_pmem.pool = Pool(mem::GenericMemory, sizeof(MetaSlab), nr_metaslabs, m_pmem.p, true);
            m_pmem.pool.set_suppress_warning(true);
            m_pmem.half_full.Initialize();

            printf("Initialized %lu metaslabs for PMEM on numa node %d, memsz = %lu bytes\n",
                   nr_metaslabs, n, memsz);
          });
    }

    for (auto &t: tasks)
      t.join();
  }

  void TestSlabMmapAddress() {
    // shirley check: print address of mmap
    auto nr_numa_nodes = ParallelAllocationPolicy::g_nr_cores / kNrCorePerNode;
    printf("SHIRLEY: TEST, PRINTING ADDRESS OF MMAP FOR G_SLABPMEM\n");
    for (int n = 0 ; n < nr_numa_nodes; n++) {
      printf("%d: %p\n", n, g_slabpmem[n].p);
    }
    printf("\n");
    return;
  }

  static SlabMemory *FindSlabMemory(void *ptr, int default_numa_node)
  {
    auto n = default_numa_node;
    if (g_slabmem[n].Contains(ptr)) {
      return &g_slabmem[n];
    }
    //also check pmem
    if (g_slabpmem[n].Contains(ptr)) {
      return &g_slabpmem[n];
    }
    int nr_numa_node = ParallelAllocationPolicy::g_nr_cores / kNrCorePerNode;
    for (n = 0; n < nr_numa_node; n++) {
      if (g_slabmem[n].Contains(ptr))
        return &g_slabmem[n];
    }
    //also check pmem
    for (n = 0; n < nr_numa_node; n++) {
      if (g_slabpmem[n].Contains(ptr))
        return &g_slabpmem[n];
    }

    fprintf(stderr, "Cannot find slab memory for ptr 0x%p\n", ptr);
    return nullptr;
  }

  MetaSlab *SlabMemory::NewMetaSlab()
  {
    auto mp = (uint8_t *) pool.Alloc();
    if (mp == nullptr)
      return nullptr;
    auto idx = (mp - p) / sizeof(MetaSlab);
    // printf("new metaslab idx %lu\n", idx);
    return new (mp) MetaSlab(p + data_offset + idx * SlabPool::kLargeSlabPageSize);
  }

  void SlabMemory::DestroyMetaSlab(MetaSlab *metaslab)
  {
    metaslab->~MetaSlab();
    pool.Free(metaslab);
  }

  void *SlabMemory::AllocSlab(bool large_slab, void *&data_ptr)
  {
    MetaSlab *metaslab;
    if (large_slab) {
      metaslab = NewMetaSlab();
      if (metaslab == nullptr) return nullptr;
      metaslab->large_slab = true;
      data_ptr = metaslab->ptr;
      return metaslab->slabs;
    }

    util::Guard<util::SpinLock> _(half_full_lock);
    if (half_full.empty()) {
      metaslab = NewMetaSlab();
      if (metaslab == nullptr) return nullptr;
      metaslab->InsertAfter(&half_full);
    }
    metaslab = half_full.next->object();
    auto idx = __builtin_ctz(~metaslab->alloc_bitmap);
    metaslab->alloc_bitmap |= uint32_t(1) << idx;
    if (~metaslab->alloc_bitmap == 0) {
      metaslab->Remove(); // Remove from the half-full
    }
    data_ptr = metaslab->ptr + idx * SlabPool::kSlabPageSize;
    return metaslab->slabs + idx * sizeof(Slab);
  }

  void SlabMemory::FreeSlab(void *slab)
  {
    auto metaslab = GetMetaSlab(slab);
    if (!metaslab->large_slab) {
      util::Guard<util::SpinLock> _(half_full_lock);

      auto slab_idx = ((uint8_t *) slab - metaslab->slabs) / sizeof(Slab);
      if (~metaslab->alloc_bitmap == 0)
        metaslab->InsertAfter(&half_full);
      metaslab->alloc_bitmap &= ~(uint32_t(1) << slab_idx);
      if (metaslab->alloc_bitmap != 0)
        return;
      metaslab->Remove(); // Remove from the half-full
    }
    DestroyMetaSlab(metaslab);
  }

  SlabPool::SlabPool(MemAllocType alloc_type, unsigned int chunk_size,
                     unsigned int nr_buffer, int numa_node,
                     SlabMemory *slab_mem_ptr, unsigned int slab_mem_size)
      : alloc_type(alloc_type), numa_node(numa_node), chunk_size(chunk_size),
        nr_empty(0), nr_buffer(nr_buffer)
  {
    if (!slab_mem_ptr) {
      //using default g_slabmem
      slabmem_ptr = g_slabmem;
      auto nr_numa_nodes = ParallelAllocationPolicy::g_nr_cores / kNrCorePerNode;
      slabmem_size = nr_numa_nodes;
    }
    else {
      slabmem_ptr = slab_mem_ptr;
      slabmem_size = slab_mem_size;
    }
    stats.used = stats.watermark = 0;
    // shirley FIX: causes big overhead for slabpool alloc?
    empty.Initialize();
    half_full.Initialize();
    while (nr_empty < nr_buffer) {
      RefillSlab();
    }
  }

  void SlabPool::Register()
  {
    std::lock_guard _(g_ps_lock);
    g_ps[int(alloc_type)].emplace_back(&stats);
  }

  Slab *SlabPool::RefillSlab()
  {
    auto n = numa_node;
    void *p = nullptr;
    //auto s = g_slabmem[n].AllocSlab(is_large_slab(), p);
    auto s = slabmem_ptr[n].AllocSlab(is_large_slab(), p);

    int nr_numa_node = ParallelAllocationPolicy::g_nr_cores / kNrCorePerNode;
    if (s != nullptr)
      goto found;

    for (n = 0; n < nr_numa_node; n++) {
      //s = g_slabmem[n].AllocSlab(is_large_slab(), p);
      s = slabmem_ptr[n].AllocSlab(is_large_slab(), p);
      if (s != nullptr)
        goto found;
    }

    fprintf(stderr, "Cannot find any memory from global slabs! chunk_size %u\n", chunk_size);
    std::abort();
  found:
    g_mem_tracker[alloc_type].fetch_add(metaslab_page_size());

    nr_empty++;
    //shirley: if use_pmem, slab will use separate dram freelist for its pool.
    bool use_pmem = false;
    if (slabmem_ptr == g_slabpmem) {
      use_pmem = true;
    }
    return new (s) Slab(&empty, alloc_type, chunk_size, p, use_pmem);
  }

  void SlabPool::ReturnSlab()
  {
    auto slab = empty.prev->object();
    slab->Remove();

    auto m = FindSlabMemory(slab, numa_node);
    slab->~Slab();
    m->FreeSlab(slab);
    nr_empty--;

    g_mem_tracker[alloc_type].fetch_sub(metaslab_page_size());
  }

  void *SlabPool::Alloc()
  {
    if (chunk_size == 0) return nullptr;

    stats.used += chunk_size;
    stats.watermark = std::max(stats.used, stats.watermark);

    if (!half_full.empty()) {
      auto slab = half_full.next->object();
      void *o = slab->pool.Alloc();
      if (slab->pool.is_full()) {
        slab->Remove();
      }
      return o;
    } else {
      auto slab = empty.empty()
                      ? RefillSlab() : empty.next->object();
      void *o = slab->pool.Alloc();
      slab->Remove();
      slab->InsertAfter(&half_full);
      nr_empty--;
      return o;
    }
  }

  void SlabPool::Free(void *ptr)
  {
    stats.used -= chunk_size;

    auto m = FindSlabMemory(ptr, numa_node);
    auto slab = m->GetSlab(ptr, is_large_slab());
    slab->pool.Free(ptr);
    if (slab->is_detached()) {
      slab->InsertAfter(&half_full);
    }
    if (slab->pool.is_empty()) {
      slab->Remove();
      slab->InsertAfter(&empty);
      nr_empty++;
    }

    if (nr_empty > nr_buffer)
      ReturnSlab();
  }

  ParallelPool::ParallelPool(MemAllocType alloc_type, size_t chunk_size, size_t total_cap)
  {
    this->chunk_size = chunk_size;
    this->total_cap = total_cap;
    this->alloc_type = alloc_type;
    std::vector<std::thread> tasks;
    auto cap = 1 + (total_cap - 1) / g_nr_cores;
    for (int node = g_core_shifting / kNrCorePerNode;
         node < (g_core_shifting + g_nr_cores) / kNrCorePerNode;
         node++) {
      tasks.emplace_back(
          [alloc_type, chunk_size, cap, this, node]() {
            fprintf(stderr, "allocating %lu on node %d\n",
                    (kHeaderSize + chunk_size * cap) * kNrCorePerNode, node);
            auto mem = (uint8_t *) AllocMemory(
                alloc_type, (kHeaderSize + chunk_size * cap) * kNrCorePerNode, node);
            int offset = node * kNrCorePerNode - g_core_shifting;
            for (int i = offset; i < offset + kNrCorePerNode; i++) {
              auto p = mem + (i - offset) * (kHeaderSize + chunk_size * cap);
              auto pool_mem = p + kHeaderSize;

              pools[i] = new (p) BasicPool(
                  alloc_type, chunk_size, cap,
                  pool_mem);

              p += sizeof(BasicPool);
              free_lists[i] = (uintptr_t *) p;

              p += kMaxNrPools * sizeof(uintptr_t);
              free_tails[i] = (uintptr_t *) p;

              p += kMaxNrPools * sizeof(uintptr_t);
              csld_free_lists[i] = new (p) ConsolidateFreeList();

              std::fill(free_lists[i], free_lists[i] + kMaxNrPools, 0);
              std::fill(free_tails[i], free_tails[i] + kMaxNrPools, 0);
            }
          });
    }

    for (auto &th: tasks) {
      th.join();
    }
  }

  ParallelPool::~ParallelPool()
  {
    // TODO: unmap() and delete stuff
  }

  void ParallelPool::AddExtraBasicPool(int core, size_t cap, int node)
  {
    if (cap == 0) cap = total_cap / g_nr_cores;
    pools[core] = new BasicPool(alloc_type, chunk_size, cap, node);
  }

  ParallelSlabPool::ParallelSlabPool(MemAllocType alloc_type, size_t chunk_size, unsigned int buffer, bool use_pmem)
  {
    this->alloc_type = alloc_type;
    this->chunk_size = chunk_size;
    this->total_cap = buffer * ParallelAllocationPolicy::g_nr_cores;

    uint8_t *mem = nullptr;
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      auto numa_node = i / kNrCorePerNode;
      auto numa_offset = i % kNrCorePerNode;
      if (numa_offset == 0) {
        // note: we'll always keep the info in dram, only pool memory in pmem if required
        mem = (uint8_t *)AllocMemory(alloc_type, kHeaderSize * kNrCorePerNode);
        // if (use_pmem) //((alloc_type == VhandlePool) || (alloc_type == RegionPool))
        // {
        //   mem = (uint8_t *)AllocPersistentMemory(alloc_type, kHeaderSize * kNrCorePerNode);
        // }
        // else
        // {
        //   mem = (uint8_t *)AllocMemory(alloc_type, kHeaderSize * kNrCorePerNode);
        // }
      }

      auto p = mem + numa_offset * kHeaderSize;

      auto nr_numa_nodes = ParallelAllocationPolicy::g_nr_cores / kNrCorePerNode;
      if (use_pmem) //((alloc_type == VhandlePool) || (alloc_type == RegionPool))
      {
        pools[i] = new (p) SlabPool(alloc_type, chunk_size, buffer, numa_node, g_slabpmem, nr_numa_nodes);
      }
      else
      {
        pools[i] = new (p) SlabPool(alloc_type, chunk_size, buffer, numa_node);
      }

      p += sizeof(SlabPool);
      free_lists[i] = (uintptr_t *) p;

      p += kMaxNrPools * sizeof(uintptr_t);
      free_tails[i] = (uintptr_t *) p;

      p += kMaxNrPools * sizeof(uintptr_t);
      csld_free_lists[i] = new (p) ConsolidateFreeList();

      std::fill(free_lists[i], free_lists[i] + kMaxNrPools, 0);
      std::fill(free_tails[i], free_tails[i] + kMaxNrPools, 0);
    }
  }

  ParallelSlabPool::~ParallelSlabPool()
  {
    // TODO: unmap the pools buffer.
  }

  ParallelRegion::ParallelRegion()
  {
#if 0
  // default?
  for (int i = 0; i < kMaxPools; i++) {
    if (i < 16) {
      proposed_caps[i] = 32 << (20 - 5 - i);
    } else {
      proposed_caps[i] = 32;
    }
  }
#endif
    memset(proposed_caps, 0, sizeof(size_t) * kMaxPools);
  }

  void *ParallelRegion::Alloc(size_t sz)
  {
    int k = SizeToClass(sz);
    if (k < 0) return nullptr;

    auto &p = pools[k];
    void *r = nullptr;

    r = p.Alloc();
    if (r == nullptr) goto error;
    return r;
  error:
    fprintf(stderr, "size %ld on class %d has no more memory preallocated\n", sz, SizeToClass(sz));
    std::abort();
  }

  void ParallelRegion::Free(void *ptr, int alloc_core, size_t sz)
  {
    if (ptr == nullptr) return;
    int k = SizeToClass(sz);
    if (k < 0) std::abort();
    pools[k].Free(ptr, alloc_core);
  }

  void ParallelRegion::ApplyFromConf(json11::Json conf_doc)
  {
    auto json_map = conf_doc.object_items();
    for (auto it = json_map.begin(); it != json_map.end(); ++it) {
      set_pool_capacity(atoi(it->first.c_str()), size_t(it->second.number_value() * 1024));
    }
  }

  void ParallelRegion::InitPools(bool use_pmem)
  {
    std::vector<std::thread> tasks;
    for (int i = 0; i < kMaxPools; i++) {
      tasks.emplace_back(
          [this, i, use_pmem] {
            size_t chunk_size = 1ULL << (i + 5);
            size_t nr_buffer = proposed_caps[i] * chunk_size / SlabPool::PageSize(chunk_size);
            printf("chunk_size %lu nr_buffer %lu\n", chunk_size, nr_buffer);
            if (use_pmem) {
              pools[i] = ParallelSlabPool(mem::PersistentPool, chunk_size, nr_buffer, use_pmem);
            }
            else {
              pools[i] = ParallelSlabPool(mem::RegionPool, chunk_size, nr_buffer, use_pmem);
            }
          });
    }
    for (auto &th: tasks) {
      th.join();
    }
    for (int i = 0; i < kMaxPools; i++) {
      pools[i].Register();
    }
  }

  void ParallelRegion::Quiescence()
  {
    for (int i = 0; i < kMaxPools; i++) {
      pools[i].Quiescence();
    }
  }

  void ParallelRegion::PrintUsageEachClass()
  {
    for (int i = 0; i < kMaxPools; i++) {
      if (proposed_caps[i] == 0) continue;
      auto &pool = pools[i];
      size_t used = 0;
      for (int j = 0; j < ParallelAllocationPolicy::g_nr_cores; j++) {
        used += pool.get_pool(j)->stats.used;
      }
      auto chk_size = 32UL << i;
      printf("RegionInfo: class %d size %lu mem %lu/%lu\n", i, chk_size, used,
             chk_size * pool.capacity());
    }
  }

  static ParallelRegion g_data_region;
  //shirley: old. not used anymore.
  static ParallelRegion g_data_region_pmem;
  ParallelRegion &GetDataRegion(bool use_pmem) { 
    if (use_pmem) return g_data_region_pmem;
    return g_data_region;
  }

  // transient and persistent pools
  static ParallelBrk g_transient_pool;
  static ParallelBrk g_transient_pmem_pool;
  static ParallelRegion g_persistent_pool;

  ParallelBrk &GetTransientPool() { 
    return g_transient_pool;
  }
  ParallelBrk &GetTransientPmemPool() { 
    return g_transient_pmem_pool;
  }
  ParallelRegion &GetPersistentPool() { 
    return g_persistent_pool;
  }

  void InitTransientPool(size_t t_mem) {
    //shirley pmem: when on pmem machine, set default to false. when on our machine, set to true
    bool use_pmem = false;
    if (felis::Options::kPmemNaive) {
      use_pmem = true;
    }
    g_transient_pool = ParallelBrk(mem::TransientPool, t_mem, use_pmem);
  }

  void InitTransientPmemPool(size_t t_mem) {
    if (felis::Options::kEnableZen) {
      g_transient_pmem_pool = ParallelBrk(mem::TransientPmemPool, t_mem, true);
    }
  }

  static ParallelBrkWFree g_external_pmem_pool;
  // shirley test: for DRAM only, set to 472 MB for tpcc.
  static size_t kExternalPmemPoolSize = ((size_t)1024)*1024*1024;// ((size_t)1280)*1024*1024; // ((size_t)472)*1024*1024; // 472 MB // ((size_t)2)*1024*1024*1024; // 1 GB
  static size_t kExternalPmemValuesSize = 1024; // 1 KB
  ParallelBrkWFree &GetExternalPmemPool() { return g_external_pmem_pool; }

  void InitExternalPmemPool() {
    void *fixed_mmap_addr = (void *) 0x7F0000000000; // nullptr;
    g_external_pmem_pool = mem::ParallelBrkWFree(
        mem::ExternalPmemPool, mem::ExternalPmemFreelistPool, fixed_mmap_addr,
        kExternalPmemPoolSize, kExternalPmemValuesSize, true, felis::Options::kRecovery);
  }

  void PersistExternalPmemPoolOffsets(bool first_slot) {
    g_external_pmem_pool.persistOffsets(first_slot);
  }

  static PmemPersistInfo *g_pmem_info;
  PmemPersistInfo *GetPmemPersistInfo() { return g_pmem_info; }
  void InitPmemPersistInfo() {
    void *fixed_mmap_addr = (void *) 0x7FD800000000; // nullptr;
    if (felis::Options::kRecovery) {
      MapPersistentMemory(MemAllocType::PmemInfo, 0, sizeof(PmemPersistInfo), fixed_mmap_addr);
      g_pmem_info = (PmemPersistInfo *) fixed_mmap_addr;
    }
    else {
      // shirley test
      g_pmem_info = (PmemPersistInfo *) AllocPersistentMemory(MemAllocType::PmemInfo, sizeof(PmemPersistInfo), 0, -1, fixed_mmap_addr, false);
      // g_pmem_info = (PmemPersistInfo *) AllocMemory(MemAllocType::PmemInfo, sizeof(PmemPersistInfo), -1);
      memset(g_pmem_info, 0, sizeof(PmemPersistInfo));
      FlushPmemPersistInfo();
    }
  }

  void FlushPmemPersistInfo() {
    // shirley pmem shirley test
    for (unsigned int i = 0; i < sizeof(PmemPersistInfo); i += 64) {
      _mm_clwb(((uint8_t *)g_pmem_info) + i);
    }
  }

  static uint8_t **g_txn_input_log; // pointer to an array of input logs
  static uint64_t txn_input_log_size = 32*1024*1024; // 32 MB
  uint8_t **GetTxnInputLog() { return g_txn_input_log; }
  void InitTxnInputLog() {
    if (!felis::Options::kLogInput && !felis::Options::kRecovery) {
      return;
    }
    void *fixed_mmap_addr = (void *) 0x7F4000000000; // nullptr; 
    int num_cores = ParallelAllocationPolicy::g_nr_cores;
    g_txn_input_log = (uint8_t **) malloc(num_cores * sizeof(void*));

    if (felis::Options::kRecovery) {
      for (int i = 0; i < num_cores; i++) {
        void *fixaddr = ((uint8_t *)fixed_mmap_addr) + i * txn_input_log_size; // shirley: might need to add some offset
        MapPersistentMemory(TxnInputLog, i, txn_input_log_size, fixaddr);
        g_txn_input_log[i] = (uint8_t *)fixaddr;
      }
    }
    else {
      for (int i = 0; i < num_cores; i++) {
        g_txn_input_log[i] = (uint8_t *) AllocPersistentMemory(TxnInputLog, txn_input_log_size, i, -1, nullptr, false);
      }
    }
  }

  void *Brk::Alloc(size_t s) {
    s = util::Align(s, 16);
    size_t off = 0;
    if (!thread_safe) {
      off = offset.load(std::memory_order_relaxed);
      offset.store(off + s, std::memory_order_relaxed);
    } else {
      off = offset.fetch_add(s, std::memory_order_seq_cst);
    }

    if (__builtin_expect(off + s > limit, 0)) {
      fprintf(stderr, "Brk of limit %lu is not large enough!\n", limit);
      std::abort();
    }
    uint8_t *p = data + off;
    return p;
  }

  ParallelBrk::ParallelBrk(MemAllocType alloc_type, size_t brk_pool_size, bool use_pmem)
  {
    uint8_t *mem = nullptr;
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      auto numa_node = i / kNrCorePerNode;
      auto numa_offset = i % kNrCorePerNode;
      if (numa_offset == 0) {
        // note: we'll always keep the info in dram, only pool memory in pmem if required
        mem = (uint8_t *)AllocMemory(alloc_type, kHeaderSize * kNrCorePerNode);
      }

      auto p = mem + numa_offset * kHeaderSize;
      uint8_t *p_buf;
      if (use_pmem) {
        p_buf = (uint8_t *)AllocPersistentMemory(alloc_type, brk_pool_size, i);
      }
      else {
        p_buf = (uint8_t *)AllocMemory(alloc_type, brk_pool_size);
      }
      
      pools[i] = new (p) Brk(p_buf, brk_pool_size, use_pmem);
      pools[i]->set_thread_safe(false); //shirley: false means don't need to be thread safe

      p += sizeof(Brk);
      free_lists[i] = (uintptr_t *) p;

      p += kMaxNrPools * sizeof(uintptr_t);
      free_tails[i] = (uintptr_t *) p;

      p += kMaxNrPools * sizeof(uintptr_t);
      csld_free_lists[i] = new (p) ConsolidateFreeList();

      std::fill(free_lists[i], free_lists[i] + kMaxNrPools, 0);
      std::fill(free_tails[i], free_tails[i] + kMaxNrPools, 0);
    }
  }

  void ParallelBrk::Reset()
  {
    //shirley: transient_size var is used for probing.
    // size_t transient_size = 0;
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      // transient_size += pools[i]->current_size();
      pools[i]->Reset();
    }
    // felis::probes::MemAllocParallelBrkPool{transient_size}();
  }

  ParallelBrk::~ParallelBrk()
  {
    // TODO
  }

  static Brk *BrkFromRoutine()
  {
    auto sched = go::Scheduler::Current();
    if (!sched) {
      fprintf(stderr, "Failed %s, not running on coroutines!\n", __FUNCTION__);
      return nullptr;
    }
    auto *r = sched->current_routine();
    if (!r || !r->userdata()) {
      fprintf(stderr, "Failed %s, current routine or brk isn't available\n", __FUNCTION__);
      return nullptr;
    }
    return (Brk *) r->userdata();
  }

  void *AllocFromRoutine(size_t sz)
  {
    return BrkFromRoutine()->Alloc(sz);
  }

  void *BrkWFree::Alloc() {
    if (offset_freelist != initial_offset_pending_freelist){
      // shirley: can allocate from freelist instead.
      // auto _ = util::Guard(lock_freelist);
      // shirley: don't need lock here because alloc is done at own core.
      auto off = offset_freelist;
      offset_freelist++;
      if (offset_freelist == limit_ring_buffer) {
        offset_freelist = 0;
      }
      void *ptr = (uint64_t *) *(get_ring_buffer() + off);
      return ptr;
    }
    // shirley: else, allocate from brk
    size_t off = offset; // *get_offset();
    size_t lmt = limit; // *get_limit();
    size_t blk_sz = block_size;

    offset = off + blk_sz;
    // *get_offset() = off + blk_sz;

    if (__builtin_expect(off + blk_sz > lmt, 0)) {
      fprintf(stderr, "BrkWFree of limit %lu is not large enough!\n", lmt);
      std::abort();
    }
    uint8_t *p = get_data() + off;
    return p;
  }

  void BrkWFree::Free(void *ptr) {
    if (felis::Options::kRecovery) {
      if (is_gc) {
        if (freelist_hash->find(ptr) != freelist_hash->end()) {
          // is a duplicate.
          return;
        }
      }
    }
    // shirley todo: add to freelist
    // auto _ = util::Guard(lock_freelist);
    util::MCSSpinLock::QNode qnode;
    lock_ring_buffer.Acquire(&qnode);
    size_t off;

    if (!is_gc) {
      off = offset_pending_freelist;
      offset_pending_freelist++;
      if (offset_pending_freelist == limit_ring_buffer) {
        offset_pending_freelist = 0;
      }
    } else {
      off = (*get_current_offset_pending_freelist()) & 0x00000000FFFFFFFF;
    }

    // shirley: assume our ring buffer is large enough so we won't run out of space to free.
    // if (off == initial_offset_freelist && /*we have deleted in this epoch already*/) {
    //   printf("We run out of space in pending freelist!! We are running into freelist area\n");
    //   std::abort();
    // }

    if (!is_gc) {
      // can release right after incrementing offset
      lock_ring_buffer.Release(&qnode);
    }
    
    uint64_t *rb = get_ring_buffer();
    *(rb + off) = (uint64_t)ptr;

    // shirley note: can optimize by flushing only every cacheline / 256 bytes
    // shirley pmem shirley test
    _mm_clwb(rb + off);

    if (is_gc) {
      // shirley pmem shirley test
      _mm_sfence();

      size_t old_off = *get_current_offset_pending_freelist();
      size_t new_off = off + 1;
      if (new_off == limit_ring_buffer) {
        new_off = 0;
      }
      *get_current_offset_pending_freelist() = ((old_off & 0xFFFFFFFF00000000) | (new_off & 0x00000000FFFFFFFF));
      // shirley pmem shirley test
      _mm_clwb(ring_buffer);
      _mm_sfence();

      lock_ring_buffer.Release(&qnode);
    }
  }

  ParallelBrkWFree::ParallelBrkWFree(MemAllocType alloc_type,
                                     MemAllocType freelist_alloc_type,
                                     void *fixed_mmap_addr,
                                     size_t brk_pool_size, size_t block_size,
                                     bool persist_pending_freelist,
                                     bool is_recovery) {
    uint8_t *mem = nullptr;
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      auto numa_node = i / kNrCorePerNode;
      auto numa_offset = i % kNrCorePerNode;
      if (numa_offset == 0) {
        // note: we'll always keep the info in dram, only pool memory in pmem if required
        mem = (uint8_t *)AllocMemory(alloc_type, kHeaderSize * kNrCorePerNode);
      }

      auto p = mem + numa_offset * kHeaderSize;
      uint8_t *p_buf;
      uint8_t *p_buf_freelist;
      size_t freelist_size = 16384; // brk_pool_size / block_size;
      if (alloc_type == mem::ExternalPmemPool) {
        // shirley test: might need to reduce freelist size (if all inlined)
        freelist_size = ((size_t)2) * 1024 * 1024;
      }
      // shirley todo: if is recovery, don't alloc, just mmap file to fixed address
      // alloc brks
      void *hint_addr = fixed_mmap_addr ? ((uint8_t*)fixed_mmap_addr + (i * brk_pool_size)) : nullptr;
      if (is_recovery) {
        MapPersistentMemory(alloc_type, i, brk_pool_size, hint_addr);
        p_buf = (uint8_t *) hint_addr;
      }
      else {
        // shirley test
        p_buf = (uint8_t *)AllocPersistentMemory(alloc_type, brk_pool_size, i, -1, hint_addr);
        printf("ParallelBrkWFree type %d core %d p_buf = %p\n", alloc_type, i, p_buf);
        // p_buf = (uint8_t *)AllocMemory(alloc_type, brk_pool_size);
      }
      
      // alloc ring buffers
      void *hint_addr_freelist = (fixed_mmap_addr) ? 
          ((uint8_t *)fixed_mmap_addr +
          (ParallelAllocationPolicy::g_nr_cores * brk_pool_size) +
          (i * freelist_size * 8) + 1024*1024*1024) : nullptr; // shirley: add 1G to leave some space in between for mmap
      if (is_recovery) {
        MapPersistentMemory(freelist_alloc_type, i, freelist_size * 8, hint_addr_freelist);
        p_buf_freelist = (uint8_t *)hint_addr_freelist;
      }
      else {
        // shirley test
        p_buf_freelist = (uint8_t *)AllocPersistentMemory(freelist_alloc_type, freelist_size * 8, i, -1, hint_addr_freelist);
        // p_buf_freelist = (uint8_t *)AllocMemory(freelist_alloc_type, freelist_size * 8);
      }
        
      pools[i] = new (p) BrkWFree(p_buf, p_buf_freelist, brk_pool_size, freelist_size, block_size, persist_pending_freelist, is_recovery);
      
      p += sizeof(BrkWFree);
      free_lists[i] = (uintptr_t *) p;

      p += kMaxNrPools * sizeof(uintptr_t);
      free_tails[i] = (uintptr_t *) p;

      p += kMaxNrPools * sizeof(uintptr_t);
      csld_free_lists[i] = new (p) ConsolidateFreeList();

      std::fill(free_lists[i], free_lists[i] + kMaxNrPools, 0);
      std::fill(free_tails[i], free_tails[i] + kMaxNrPools, 0);
    }
  }

  // void ParallelBrkWFree::Reset()
  // {
  //   for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
  //     pools[i]->Reset();
  //   }
  // }

  ParallelBrkWFree::~ParallelBrkWFree()
  {
    // TODO
  }

  std::string MemTypeToString(MemAllocType alloc_type) {
    return kMemAllocTypeLabel[alloc_type];
  }

  static PoolStatistics GetMemStatsNoLock(MemAllocType alloc_type)
  {
    PoolStatistics stat;
    memset(&stat, 0, sizeof(PoolStatistics));
    for (auto ps: g_ps[int(alloc_type)]) {
      stat.used += ps->used;
      stat.watermark += ps->watermark;
    }
    return stat;
  }

  PoolStatistics GetMemStats(MemAllocType alloc_type)
  {
    std::lock_guard _(g_ps_lock);
    return GetMemStatsNoLock(alloc_type);
  }

  void PrintMemStats() {
    puts("General memory statistics:");
    for (int i = 0; i < ContentionManagerPool; i++) {
      auto bucket = static_cast<MemAllocType>(i);
      auto size = g_mem_tracker[i].load();
      printf("   %s: %llu MB\n", MemTypeToString(bucket).c_str(),
             size / 1024 / 1024);
    }

    puts("Pool usage statistics:");

    auto N = static_cast<int>(NumMemTypes);
    PoolStatistics stats[N];

    {
      std::lock_guard _(g_ps_lock);
      for (int i = ContentionManagerPool; i < N; i++) {
        stats[i] = GetMemStatsNoLock((MemAllocType)i);
      }
    }

    for (int i = ContentionManagerPool; i < N; i++) {
      auto bucket = static_cast<MemAllocType>(i);
      printf("    %s: %llu/%llu MB used (max %llu MB)\n",
             MemTypeToString(bucket).c_str(), stats[i].used / 1024 / 1024,
             g_mem_tracker[bucket].load() / 1024 / 1024,
             stats[i].watermark / 1024 / 1024);
    }
  }

  void *AllocMemory(mem::MemAllocType alloc_type, size_t length, int numa_node, bool on_demand)
  {
    void *p = util::OSMemory::g_default.Alloc(length, numa_node, on_demand);
    if (p == nullptr) {
      printf("Allocation of %s failed, length = %zu\n", MemTypeToString(alloc_type).c_str(), length);
      PrintMemStats();
      return nullptr;
    }
    g_mem_tracker[alloc_type].fetch_add(length);
    return p;
  }

  void *AllocPersistentMemory(mem::MemAllocType alloc_type, size_t length, int core_id, int numa_node, void *addr, bool on_demand)
  {
    //file name
    char pmem_file_name[50];
    //shirley pmem: when on pmem machine, use /mnt/pmem0. when on our machine, use ../temp_files
    // sprintf(pmem_file_name, "/mnt/pmem0/m%s_%d", MemTypeToString(alloc_type).c_str(), memAllocTypeCount[alloc_type].fetch_add(1));
    // sprintf(pmem_file_name, "../temp_files/m%s_%d", MemTypeToString(alloc_type).c_str(), memAllocTypeCount[alloc_type].fetch_add(1));

    sprintf(pmem_file_name, "/mnt/pmem0/m%s_%d", MemTypeToString(alloc_type).c_str(), core_id);
    // sprintf(pmem_file_name, "../temp_files/m%s_%d", MemTypeToString(alloc_type).c_str(), core_id);

    void *p = util::OSMemory::g_default.PmemAlloc(pmem_file_name, length, numa_node, addr, on_demand);

    if (p == nullptr) {
      printf("Pmem Allocation of %s failed\n", MemTypeToString(alloc_type).c_str());
      PrintMemStats();
      return nullptr;
    }
    g_mem_tracker[alloc_type].fetch_add(length);
    return p;
  }

  void MapPersistentMemory(mem::MemAllocType alloc_type, int core_id, size_t length, void *addr) {
    // file name
    char pmem_file_name[50];
    // shirley pmem: when on pmem machine, use /mnt/pmem0. when on our machine, use ../temp_files
    sprintf(pmem_file_name, "/mnt/pmem0/m%s_%d", MemTypeToString(alloc_type).c_str(), core_id);
    // sprintf(pmem_file_name, "../temp_files/m%s_%d", MemTypeToString(alloc_type).c_str(), core_id);
    bool success = util::OSMemory::g_default.PmemMap(pmem_file_name, length, addr);
    if (!success) {
      printf("MapPersistentMemory failed!\n");
      std::abort();
    }
  }

#if 0
void *MemMapAlloc(mem::MemAllocType alloc_type, size_t length, int numa_node)
{
  int flags = MAP_ANONYMOUS | MAP_PRIVATE;
  if (length >= 2 << 20) {
    flags |= MAP_HUGETLB;
    length = util::Align(length, 2 << 20);
  } else {
    length = util::Align(length, 4096);
  }
  void *data = MemMap(alloc_type, nullptr, length,
                      PROT_READ | PROT_WRITE, flags, -1, 0);

  unsigned long nodemask = 0;

  if (numa_node == -1) {
    for (auto n = ParallelAllocationPolicy::g_core_shifting / kNrCorePerNode;
         n < ParallelAllocationPolicy::g_nr_cores / kNrCorePerNode;
         n++)
      nodemask |= 1 << n;
  } else {
    nodemask = 1 << numa_node;
  }
  if (nodemask != 0) {
    if (syscall(
            __NR_mbind,
            data, length,
            2 /* MPOL_BIND */,
            &nodemask,
            sizeof(unsigned long) * 8,
            1 << 0 /* MPOL_MF_STRICT */) < 0) {
      fprintf(stderr, "Fail to mbind on address %p length %lu mask %lx\n",
              data, length, nodemask);
      std::abort();
    }
  }

  if (mlock(data, length) < 0) {
    fprintf(stderr, "WARNING: mlock() failed\n");
    perror("mlock");
  }
  return data;
}

void *MemMap(MemAllocType alloc_type, void *addr, size_t length, int prot, int flags,
             int fd, off_t offset) {
  void *mem = mmap(addr, length, prot, flags, fd, offset);

  g_mem_tracker[alloc_type].fetch_add(length);

  if (mem == MAP_FAILED) {
    perror(MemTypeToString(alloc_type).c_str());
    PrintMemStats();
    std::abort();
  }

  return mem;
}
#endif

  long TotalMemoryAllocated()
  {
    long s = 0;
    for (auto i = 0; i < NumMemTypes; i++) {
      s += g_mem_tracker[i].load();
    }
    return s;
  }

}
