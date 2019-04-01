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

static std::atomic_long g_mem_tracker[NumMemTypes];
static PoolStatistics g_pool_tracker[NumMemTypes];

WeakPool::WeakPool(MemAllocType alloc_type, size_t chunk_size, size_t cap,
                   int numa_node)
    : WeakPool(alloc_type, chunk_size, cap, MemMapAlloc(alloc_type, cap * chunk_size, numa_node))
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
}

WeakPool::~WeakPool()
{
  if (need_unmap)
    munmap(data, len);
}

void *WeakPool::Alloc()
{
  void *r = nullptr, *next = nullptr;

  r = head;
  if (r == nullptr) {
    fprintf(stderr, "%s memory pool is full, returning nullptr\n",
            kMemAllocTypeLabel[alloc_type].c_str());
    return r;
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

void WeakPool::Free(void *ptr)
{
  *(uintptr_t *) ptr = (uintptr_t) head;
  head = ptr;

  g_pool_tracker[alloc_type].used -= len / capacity;
}

long BasicPool::CheckPointer(void *ptr)
{
  if (ptr == nullptr) {
    fprintf(stderr, "pointer ptr is nullptr\n");
    return -1;
  }
  if (ptr < data || ptr >= (uint8_t *) data + len) {
    fprintf(stderr, "%p is out of bounds %p - %p\n",
            ptr, data, (uint8_t *) data + len);
    std::abort();
  }
  auto r = std::div((uint8_t *) ptr - (uint8_t *) data, (long long) len / capacity);
  if (r.rem != 0) {
    fprintf(stderr, "%p is not aligned. %p with chunk_size %lu\n",
            ptr, data, len / capacity);
    std::abort();
  }
  return r.quot;
}

void *BasicPool::Alloc()
{
  void *ptr = WeakPool::Alloc();
  CheckPointer(ptr);
  return ptr;
}

void BasicPool::Free(void *ptr)
{
  CheckPointer(ptr);
  WeakPool::Free(ptr);
}

thread_local int ParallelPool::g_affinity = -1;
int ParallelPool::g_nr_cores = 0;
int ParallelPool::g_cores_per_node = 8;
int ParallelPool::g_core_shifting = 0;

ParallelPool::ParallelPool(MemAllocType alloc_type, size_t chunk_size, size_t total_cap)
    : pools(new BasicPool[g_nr_cores]),
      free_nodes(new uintptr_t[g_nr_cores * g_nr_cores])
{
  std::vector<std::thread> tasks;
  auto cap = 1 + (total_cap - 1) / g_nr_cores;
  for (int i = 0; i < g_nr_cores; i++) {
    auto &pool = pools[i];
    auto node = (i + g_core_shifting) / g_cores_per_node;
    tasks.emplace_back(
        [&pool, alloc_type, chunk_size, cap, node]() {
          pool.move(WeakPool(alloc_type, chunk_size,
                             cap, node));
        });
  }
  memset(free_nodes, 0, g_nr_cores * g_nr_cores * sizeof(uintptr_t));
  for (auto &th: tasks) {
    th.join();
  }
}

ParallelPool::~ParallelPool()
{
  delete [] pools;
  delete [] free_nodes;
}

void *ParallelPool::Alloc()
{
  auto cur = CurrentAffinity();
  return pools[cur].Alloc();
}

void ParallelPool::Free(void *ptr, int alloc_core)
{
  auto cur = CurrentAffinity();
  if (alloc_core < 0 || alloc_core >= g_nr_cores) {
    std::abort();
  }
  if (cur == alloc_core) {
    pools[cur].Free(ptr);
  } else {
    *(uintptr_t *)ptr = free_nodes[cur * g_nr_cores + alloc_core];
    free_nodes[cur * g_nr_cores + alloc_core] = (uintptr_t)ptr;
  }
}

void ParallelPool::Quiescence()
{
  auto cur = CurrentAffinity();
  auto &pool = pools[cur];
  for (int i = 0; i < g_nr_cores; i++) {
    uintptr_t head = free_nodes[i * g_nr_cores + cur];
    while (head) {
      uintptr_t *ptr = (uintptr_t *) head;
      head = *ptr;
      pool.Free(ptr);
    }
    free_nodes[i * g_nr_cores + cur] = 0;
  }
}

static std::mutex *g_core_locks;

void ParallelPool::InitTotalNumberOfCores(int nr_cores, int core_shifting)
{
  g_nr_cores = nr_cores;
  g_core_shifting = core_shifting;
  g_core_locks = new std::mutex[nr_cores];
}

void ParallelPool::SetCurrentAffinity(int aff)
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

int ParallelPool::CurrentAffinity()
{
  auto aff = g_affinity == -1 ? go::Scheduler::CurrentThreadPoolId() - 1 : g_affinity;
  if (aff < 0 || aff >= g_nr_cores) {
    std::abort();
  }
  return aff;
}

ParallelRegion::ParallelRegion()
{
  for (int i = 0; i < kMaxPools; i++) {
    if (i < 16) {
      proposed_caps[i] = 32 << (20 - 5 - i);
    } else {
      proposed_caps[i] = 32;
    }
  }
}

void *ParallelRegion::Alloc(size_t sz)
{
  void * r = pools[SizeToClass(sz)].Alloc();
  if (r == nullptr) {
    fprintf(stderr, "size %ld on class %d has no more memory preallocated\n", sz, SizeToClass(sz));
    std::abort();
  }
  return r;
}

void ParallelRegion::Free(void *ptr, int alloc_core, size_t sz)
{
  if (ptr == nullptr) return;
  pools[SizeToClass(sz)].Free(ptr, alloc_core);
}

void ParallelRegion::ApplyFromConf(json11::Json conf_doc)
{
  auto json_map = conf_doc.object_items();
  for (auto it = json_map.begin(); it != json_map.end(); ++it) {
    set_pool_capacity(atoi(it->first.c_str()), it->second.int_value() << 10);
  }
}

void ParallelRegion::InitPools()
{
  std::vector<std::thread> tasks;
  for (int i = 0; i < kMaxPools; i++) {
    tasks.emplace_back(
        [this, i]{
          new (&pools[i]) ParallelPool(mem::RegionPool, 1 << (i + 5), proposed_caps[i]);
        });
  }
  for (auto &th: tasks) {
    th.join();
  }
}

void ParallelRegion::Quiescence()
{
  for (int i = 0; i < kMaxPools; i++) {
    pools[i].Quiescence();
  }
}

static ParallelRegion g_data_region;
ParallelRegion &GetDataRegion() { return g_data_region; }

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
    auto size = g_mem_tracker[i].load();
    logger->info("   {}: {} MB", MemTypeToString(bucket), size / 1024 / 1024);
  }

  logger->info("Pool usage statistics:");
  for (int i = EpochQueuePool; i < NumMemTypes; i++) {
    auto bucket = static_cast<MemAllocType>(i);
    auto const &stats = g_pool_tracker[bucket];
    auto used = stats.used < 0 ? 0 : stats.used;
    logger->info("    {}: {}/{} MB used (max {} MB)", MemTypeToString(bucket),
                 used / 1024 / 1024, g_mem_tracker[bucket].load() / 1024 / 1024,
                 stats.watermark / 1024 / 1024);
  }
}


void *MemMapAlloc(mem::MemAllocType alloc_type, size_t length, int numa_node)
{
  int flags = MAP_ANONYMOUS | MAP_PRIVATE;
  if (length >= 2 >> 20)
    flags |= MAP_HUGETLB;
  void *data = MemMap(alloc_type, nullptr, length,
                      PROT_READ | PROT_WRITE, flags, -1, 0);

#ifndef DISABLE_NUMA
  if (numa_node >= 0 && length >= 2 << 20) {
    unsigned long nodemask = 1 << numa_node;
    if (syscall(__NR_mbind,
                data, length, 2 /* MPOL_BIND */, &nodemask, sizeof(unsigned long) * 8,
                1 << 0 /* MPOL_MF_STRICT */) < 0) {
      perror("mbind");
      std::abort();
    }
  }
#endif

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

long TotalMemoryAllocated()
{
  long s = 0;
  for (auto i = 0; i < NumMemTypes; i++) {
    s += g_mem_tracker[i].load();
  }
  return s;
}

}
