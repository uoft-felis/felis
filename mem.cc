#include "mem.h"

#include <sys/types.h>
#include <cassert>
#include <cstring>

#ifndef DISABLE_NUMA
#include <syscall.h>
#endif

#include <fstream>

#include "json11/json11.hpp"

#include "log.h"
#include "util.h"
#include "gopp/gopp.h"

namespace mem {

struct PoolStatistics {
  long long used;
  long long watermark;
};

static long long g_mem_tracker[NumMemTypes];
static PoolStatistics g_pool_tracker[NumMemTypes];

static ThreadLocalRegion *regions;
static size_t nr_regions;

void InitThreadLocalRegions(int tot)
{
  nr_regions = tot;
  regions = new ThreadLocalRegion[tot];
}

ThreadLocalRegion &GetThreadLocalRegion(int idx)
{
  assert(idx < nr_regions && idx >= 0);
  return regions[idx];
}

static __thread int gAffinity = -1;

void SetThreadLocalAllocAffinity(int h)
{
  gAffinity = h;
}

int CurrentAllocAffinity()
{
  if (gAffinity != -1) return gAffinity;
  else return go::Scheduler::CurrentThreadPoolId() - 1;
}

std::atomic_ulong BasicPool::g_total_page_mem = 0;
std::atomic_ulong BasicPool::g_total_hugepage_mem = 0;

BasicPool::BasicPool(MemAllocType alloc_type, size_t chunk_size, size_t cap, int numa_node)
    : len(cap * chunk_size), capacity(cap), alloc_type(alloc_type)
{
  if (cap == 0) return;

  if (len > (2 << 20)) {
    g_total_hugepage_mem.fetch_add(len);
  } else {
    g_total_page_mem.fetch_add(len);
  }

  data = MemMapAlloc(alloc_type, len);

#ifndef DISABLE_NUMA
  if (numa_node >= 0) {
    unsigned long nodemask = 1 << numa_node;
    if (syscall(__NR_mbind,
                data, len, 2 /* MPOL_BIND */, &nodemask, sizeof(unsigned long) * 8,
                1 << 0 /* MPOL_MF_STRICT */) < 0) {
      perror("mbind");
      std::abort();
    }
  }
#endif

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
}

BasicPool::~BasicPool()
{
  if (__builtin_expect(data != nullptr, true))
    munmap(data, len);
}

void *BasicPool::Alloc()
{
  void *r = nullptr, *next = nullptr;

  r = head;
  if (r == nullptr) {
    fprintf(stderr, "%s memory pool is full, returning nullptr\n",
            kMemAllocTypeLabel[alloc_type].c_str());
    return r;
  }

  if (r < data || r >= (uint8_t *) data + len) {
    fprintf(stderr, "0x%p is out of bounds 0x%p - 0x%p\n",
            r, data, (uint8_t *) data + len);
    std::abort();
  }

  next = (void *) *(uintptr_t *) r;
  head = next;

  // Statistics tracking is not done atomically for speed.
  g_pool_tracker[alloc_type].used += len / capacity;
  if (g_pool_tracker[alloc_type].watermark < g_pool_tracker[alloc_type].used) {
    g_pool_tracker[alloc_type].watermark = g_pool_tracker[alloc_type].used;
  }

  return r;
}

void BasicPool::Free(void *ptr)
{
  if (ptr < data || ptr >= (uint8_t *) data + len) {
    fprintf(stderr, "Cannot free out of bounds pointer! 0x%p\n", ptr);
    std::abort();
  }

  *(uintptr_t *) ptr = (uintptr_t) head;
  head = ptr;

  g_pool_tracker[alloc_type].used -= len / capacity;
}

void *Region::Alloc(size_t sz)
{
  void * r = pools[SizeToClass(sz)].Alloc();
  if (r == nullptr) {
    fprintf(stderr, "size %ld on class %d has no more memory preallocated\n", sz, SizeToClass(sz));
    std::abort();
  }
  return r;
}

void Region::Free(void *ptr, size_t sz)
{
  if (ptr == nullptr) return;
  pools[SizeToClass(sz)].Free(ptr);
}

void Region::ApplyFromConf(json11::Json conf_doc)
{
  auto json_map = conf_doc.object_items();
  for (auto it = json_map.begin(); it != json_map.end(); ++it) {
    set_pool_capacity(atoi(it->first.c_str()), it->second.int_value() << 10);
  }
}

void *Brk::Alloc(size_t s)
{
  s = util::Align(s, 16);
  size_t off = 0;
  if (ord == std::memory_order_relaxed) {
    off = offset.load(ord);
    offset.store(off + s, ord);
  } else {
    off = offset.fetch_add(s, ord);
  }

  if (__builtin_expect(off + s > limit, 0)) {
    fprintf(stderr, "Brk of limit %lu is not large enough!\n", limit);
    std::abort();
  }
  uint8_t *p = data + off;
  return p;
}

void *Brk::Alloc(size_t s, std::function<void (void *)> deleter)
{
  auto d = (Deleter *) Alloc(sizeof(Deleter));
  new (d) Deleter();
  d->next = deleters;
  d->del_f = deleter;
  d->p = Alloc(s);
  deleters = d;
  return d->p;
}

Brk::~Brk()
{
  while (deleters) {
    deleters->del_f(deleters->p);
    deleters->~Deleter();
    deleters = deleters->next;
  }
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
void *AllocFromRoutine(size_t sz, std::function<void (void *)> deleter)
{
  return BrkFromRoutine()->Alloc(sz, deleter);
}

std::string MemTypeToString(MemAllocType alloc_type) {
  return kMemAllocTypeLabel[alloc_type];
}

void PrintMemStats() {
  logger->info("General memory statistics:");
  for (int i = 0; i < EpochQueuePool; i++) {
    auto bucket = static_cast<MemAllocType>(i);
    auto size = g_mem_tracker[i];
    logger->info("   {}: {} MB", MemTypeToString(bucket), size / 1024 / 1024);
  }

  logger->info("Pool usage statistics:");
  for (int i = EpochQueuePool; i < NumMemTypes; i++) {
    auto bucket = static_cast<MemAllocType>(i);
    auto const &stats = g_pool_tracker[bucket];
    auto used = stats.used < 0 ? 0 : stats.used;
    logger->info("    {}: {}/{} MB used (max {} MB)", MemTypeToString(bucket),
                 used / 1024 / 1024, g_mem_tracker[bucket] / 1024 / 1024,
                 stats.watermark / 1024 / 1024);
  }
}

void *MemMap(MemAllocType alloc_type, void *addr, size_t length, int prot, int flags,
             int fd, off_t offset) {
  void *mem = mmap(addr, length, prot, flags, fd, offset);

  g_mem_tracker[alloc_type] += length;

  if (mem == MAP_FAILED) {
    perror(MemTypeToString(alloc_type).c_str());
    std::abort();
  }

  return mem;
}

}
