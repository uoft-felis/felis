#ifndef MEM_H
#define MEM_H

#include <sys/types.h>
#include <sys/mman.h>
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <string>
#include <numaif.h>
#include <mutex>
#include <atomic>

namespace mem {

const int kNrCorePerNode = 8;

template <bool LockRequired = false>
class Pool {
  void *data;
  size_t len;
  void *head;
  size_t capacity;
  size_t consumed;

  std::mutex m;
public:
  Pool() : data(nullptr), len(0) {}

  Pool(size_t chunk_size, size_t cap, int numa_node = -1)
    : len(cap * chunk_size), capacity(cap), consumed(0) {

    int flags = MAP_ANONYMOUS | MAP_PRIVATE;
    if (len > (2 << 20))
      flags |= MAP_HUGETLB;

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
    if (len > (2 << 20))
      pgsz = (2 << 20);
    for (volatile uint8_t *p = (uint8_t *) data; p < (uint8_t *) data + len; p += pgsz) {
      (*p) = 0;
    }
#endif
    head = data;
    for (size_t i = 0; i < cap; i++) {
      uintptr_t p = (uintptr_t) head + i * chunk_size;
      uintptr_t ptr = p + chunk_size;
      if (i == cap - 1) ptr = 0;
      *(uintptr_t *) p = ptr;
    }
  }

  Pool(const Pool &rhs) = delete;

  ~Pool() {
    if (__builtin_expect(data != nullptr, true))
      munmap(data, len);
  }

  void *Alloc() {
    std::unique_lock<std::mutex> l(m, std::defer_lock);
    if (LockRequired) l.lock();

    void *r = head;
    if (r == nullptr) return nullptr;

    uintptr_t ptr = *(uintptr_t *) head;
    head = (void *) ptr;
    consumed++;
    return r;
  }

  void Free(void *ptr) {
    std::unique_lock<std::mutex> l(m, std::defer_lock);
    if (LockRequired) l.lock();

    assert(ptr >= data && ptr < (uint8_t *) data + len);
    *(uintptr_t *) ptr = (uintptr_t) head;
    head = ptr;
    consumed--;
  }

  size_t nr_consumed() const { return consumed; }
};

template <bool LockRequired = false>
class Region {
  typedef Pool<LockRequired> PoolType;
  static const int kMaxPools = 20;
  PoolType pools[32];
  size_t proposed_caps[32];
public:
  Region() {
    for (int i = 0; i < kMaxPools; i++) {
      proposed_caps[i] = (128 << 20) / (1 << (6 + i));
    }
  }

  Region(const Region &) = delete;

  static int SizeToClass(size_t sz) {
    int idx = 64 - __builtin_clzl(sz - 1) - 6;
    assert(idx < kMaxPools);
    return idx < 0 ? 0 : idx;
  }

  void set_pool_capacity(size_t sz, size_t cap) {
    proposed_caps[SizeToClass(sz)] = cap;
  }

  void InitPools(int node = -1) {
    for (int i = 0; i < kMaxPools; i++) {
      new (&pools[i]) PoolType(1 << (i + 6), proposed_caps[i], node);
    }
  }

  void *Alloc(size_t sz) {
    void * r = pools[SizeToClass(sz)].Alloc();
    if (r == nullptr) {
      fprintf(stderr, "size %ld on class %d has no more memory preallocated\n", sz, SizeToClass(sz));
      std::abort();
    }
    return r;
  }

  void Free(void *ptr, size_t sz) {
    if (ptr == nullptr) return;
    pools[SizeToClass(sz)].Free(ptr);
  }
};

typedef Region<true> ThreadLocalRegion;

void InitThreadLocalRegions(int tot);
ThreadLocalRegion &GetThreadLocalRegion(int idx);

int CurrentAllocAffinity();
void SetThreadLocalAllocAffinity(int h);

}

#endif /* MEM_H */
