#include "mem.h"

#include <sys/types.h>
#include <sys/mman.h>
#include <cassert>
#include <cstring>
#include <numaif.h>
#include <fstream>

#include "json11/json11.hpp"

#include "util.h"
#include "gopp/gopp.h"

namespace mem {

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

Pool::Pool(size_t chunk_size, size_t cap, int numa_node)
    : len(cap * chunk_size), capacity(cap)
{
  if (cap == 0) return;

  int flags = MAP_ANONYMOUS | MAP_PRIVATE;
  if (len > (2 << 20)) {
    flags |= MAP_HUGETLB;
    len = util::Align(len, 2 << 20);
  } else {
    len = util::Align(len, 4 << 10);
  }

  data = mmap(nullptr, len, PROT_READ | PROT_WRITE, flags, -1, 0);
  if (data == (void *) -1) {
    perror("mmap");
    std::abort();
  }
  if (numa_node >= 0) {
    unsigned long nodemask = 1 << numa_node;
    if (mbind(data, len, MPOL_BIND, &nodemask, sizeof(unsigned long) * 8,
              MPOL_MF_STRICT) < 0) {
      perror("mbind");
      std::abort();
    }
  }
#ifdef NDEBUG
  // manually prefault
  size_t pgsz = 4096;
  if (flags & MAP_HUGETLB) {
    pgsz = (2 << 20);
  }
  for (volatile uint8_t *p = (uint8_t *) data; p < (uint8_t *) data + len; p += pgsz) {
    /*
    fprintf(stderr, "prefaulting %s %lu%%\r",
            (flags & MAP_HUGETLB) ? "hugepage" : "        ",
            (p - (uint8_t *) data) * 100 / len);
    */
    (*p) = 0;
  }
#endif
  head = data;
  for (size_t i = 0; i < cap; i++) {
    uintptr_t p = (uintptr_t) head.load() + i * chunk_size;
    uintptr_t next = p + chunk_size;
    if (i == cap - 1) next = 0;
    *(uintptr_t *) p = next;
  }
}

Pool::~Pool()
{

  if (__builtin_expect(data != nullptr, true))
    munmap(data, len);
}

void *Pool::Alloc()
{
  void *r = nullptr, *next = nullptr;
again:
  r = head.load();
  if (r == nullptr) return nullptr;

  if (r < (uint8_t *) data || r >= (uint8_t *) data + len) {
    fprintf(stderr, "memory pool not large enough!");
    std::abort();
  }

  next = (void *) *(uintptr_t *) head.load();

  if (!head.compare_exchange_strong(r, next)) goto again;
  return r;
}

void Pool::Free(void *ptr)
{
  void *r = nullptr;
again:
  r = head;
  *(uintptr_t *) ptr = (uintptr_t) r;

  if (!head.compare_exchange_strong(r, ptr)) goto again;
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
    set_pool_capacity(atoi(it->first.c_str()), it->second.int_value() << 20);
  }
}

void *Brk::Alloc(size_t s)
{
  s = util::Align(s, 8);
  if (__builtin_expect(offset + s > limit, 0)) {
    fprintf(stderr, "Brk of limit %lu is not large enough!\n", limit);
    std::abort();
  }
  uint8_t *p = data + offset;
  offset += s;
  return p;
}

void *Brk::Alloc(size_t s, std::function<void (void *)> deleter)
{
  auto d = (Deleter *) Alloc(sizeof(Deleter));
  new (d) Deleter ();
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


}
