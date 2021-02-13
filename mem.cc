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

namespace mem {

  static std::atomic_llong g_mem_tracker[NumMemTypes];
  static std::mutex g_ps_lock;
  static std::vector<PoolStatistics *> g_ps[NumMemTypes];

  // keep track of number of pmem files we have for each type
  static std::atomic_uint memAllocTypeCount[NumMemTypes];

  WeakPool::WeakPool(MemAllocType alloc_type, size_t chunk_size, size_t cap,
                     int numa_node)
      : WeakPool(alloc_type, chunk_size, cap, AllocMemory(alloc_type, cap * chunk_size, numa_node))
  {
    need_unmap = true;
  }

  WeakPool::WeakPool(MemAllocType alloc_type, size_t chunk_size, size_t cap, void *data)
      : data(data), len(cap * chunk_size), capacity(cap), alloc_type(alloc_type), need_unmap(false)
  {
    head = data;

#if 0
  fprintf(stderr, "Initializing memory pool %s, %lu objs, each %lu bytes\n",
          kMemAllocTypeLabel[alloc_type].c_str(),
          cap, chunk_size);
#endif

    for (size_t i = 0; i < cap; i++) {
      uintptr_t p = (uintptr_t) head + i * chunk_size;
      uintptr_t next = p + chunk_size;
      if (i == cap - 1) next = 0;
      *(uintptr_t *) p = next;
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
    void *r = nullptr, *next = nullptr;

    r = head;
    if (r == nullptr) {
      return r;
    }

    next = (void *) *(uintptr_t *) r;
    head = next;

    stats.used += len / capacity;
    stats.watermark = std::max(stats.used, stats.watermark);

    return r;
  }

  void WeakPool::Free(void *ptr)
  {
    *(uintptr_t *) ptr = (uintptr_t) head;
    head = ptr;

    stats.used -= len / capacity;
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

    Slab(util::GenericListNode<Slab> *qhead, MemAllocType alloc_type, size_t chunk_size, void *p) {
      InsertAfter(qhead);
      pool = BasicPool(alloc_type, chunk_size, SlabPool::PageSize(chunk_size) / chunk_size, p);
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
    uint8_t *p;
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
            m_pmem.p = (uint8_t *)AllocPersistentMemory(mem::GenericMemory, memsz, n);
            nr_metaslabs = ((memsz - 1) / SlabPool::kLargeSlabPageSize + 1);
            m_pmem.data_offset = util::Align(nr_metaslabs * sizeof(MetaSlab), SlabPool::kLargeSlabPageSize);
            m_pmem.data_len = memsz;

            nr_metaslabs -= m_pmem.data_offset / SlabPool::kLargeSlabPageSize;
            m_pmem.pool = Pool(mem::GenericMemory, sizeof(MetaSlab), nr_metaslabs, m_pmem.p);
            m_pmem.pool.set_suppress_warning(true);
            m_pmem.half_full.Initialize();

            printf("Initialized %lu metaslabs for PMEM on numa node %d, memsz = %lu bytes\n",
                   nr_metaslabs, n, memsz);
          });
    }

    for (auto &t: tasks)
      t.join();
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
    return new (s) Slab(&empty, alloc_type, chunk_size, p);
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
  static ParallelRegion g_data_region_pmem;
  ParallelRegion &GetDataRegion(bool use_pmem) { 
    if (use_pmem) return g_data_region_pmem;
    return g_data_region;
  }

  // transient and persistent pools
  static ParallelBrk g_transient_pool;
  static ParallelRegion g_persistent_pool;

  ParallelBrk &GetTransientPool() { 
    return g_transient_pool;
  }
  ParallelRegion &GetPersistentPool() { 
    return g_persistent_pool;
  }

  void InitTransientPool(size_t t_mem) {
    g_transient_pool = ParallelBrk(t_mem, true); //remember to change this to false!
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

  ParallelBrk::ParallelBrk(size_t brk_pool_size, bool use_pmem)
  {
    uint8_t *mem = nullptr;
    for (unsigned int i = 0; i < ParallelAllocationPolicy::g_nr_cores; i++) {
      auto numa_node = i / kNrCorePerNode;
      auto numa_offset = i % kNrCorePerNode;
      if (numa_offset == 0) {
        // note: we'll always keep the info in dram, only pool memory in pmem if required
        mem = (uint8_t *)AllocMemory(TransientPool, kHeaderSize * kNrCorePerNode);
      }

      auto p = mem + numa_offset * kHeaderSize;
      uint8_t *p_buf;
      if (use_pmem) {
        p_buf = (uint8_t *)AllocPersistentMemory(PersistentPool, brk_pool_size);
      }
      else {
        p_buf = (uint8_t *)AllocMemory(TransientPool, brk_pool_size);
      }
      
      pools[i] = new (p) Brk(p_buf, brk_pool_size, use_pmem);

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
      printf("Allocation of %s failed\n", MemTypeToString(alloc_type).c_str());
      PrintMemStats();
      return nullptr;
    }
    g_mem_tracker[alloc_type].fetch_add(length);
    return p;
  }

  void *AllocPersistentMemory(mem::MemAllocType alloc_type, size_t length, int numa_node, bool on_demand)
  {
    //file name
    char pmem_file_name[50];
    //sprintf(pmem_file_name, "/mnt/pmem0/m%s_%d", MemTypeToString(alloc_type).c_str(), memAllocTypeCount[alloc_type].fetch_add(1));
    sprintf(pmem_file_name, "../temp_files/m%s_%d", MemTypeToString(alloc_type).c_str(), memAllocTypeCount[alloc_type].fetch_add(1));

    void *p = util::OSMemory::g_default.PmemAlloc(pmem_file_name, length, numa_node, on_demand);

    if (p == nullptr) {
      printf("Pmem Allocation of %s failed\n", MemTypeToString(alloc_type).c_str());
      PrintMemStats();
      return nullptr;
    }
    g_mem_tracker[alloc_type].fetch_add(length);
    return p;
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
