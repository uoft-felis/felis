#ifndef MEM_H
#define MEM_H

#include <sys/types.h>
#include <sys/mman.h>
#include <string>
#include <numaif.h>
#include <mutex>

namespace mem {

const int kNrCorePerNode = 8;

template <bool LockRequired = false>
class Pool {
  void *data;
  size_t len;
  void *head;
  std::mutex m;
public:
  Pool() : data(nullptr), len(0) {}

  Pool(size_t chunk_size, size_t cap, int numa_node = -1) : len(cap * chunk_size) {
    auto flags = MAP_ANONYMOUS | MAP_PRIVATE;
    if (len > (2 << 20))
      flags |= MAP_HUGETLB;

    data = mmap(nullptr, len, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (data == nullptr) {
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
    // manually prefault
#if 0
    for (volatile uint8_t *p = (uint8_t *) data; p < (uint8_t *) data + len; p += 4096) {
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
    return r;
  }

  void Free(void *ptr) {
    std::unique_lock<std::mutex> l(m, std::defer_lock);
    if (LockRequired) l.lock();

    *(uintptr_t *) ptr = (uintptr_t) head;
    head = ptr;
  }
};

template <bool LockRequired = false>
class Region {
  typedef Pool<LockRequired> PoolType;
  PoolType pools[16];
  size_t proposed_caps[16];
public:
  Region() {
    for (int i = 0; i < 16; i++) {
      proposed_caps[i] = (16 << 20) / (1 << (6 + i));
    }
  }

  Region(const Region &) = delete;

  static int SizeToClass(size_t sz) {
    int idx = 64 - __builtin_clzl(sz - 1) - 6;
    assert(idx < 16);
    return idx < 0 ? 0 : idx;
  }

  void set_pool_capacity(size_t sz, size_t cap) {
    fprintf(stderr, "capacity %d = %ld\n", SizeToClass(sz), cap);
    proposed_caps[SizeToClass(sz)] = cap;
  }

  void InitPools(int node = -1) {
    for (int i = 64; i <= (2 << 20); i *= 2) {
      int idx = SizeToClass(i);

      assert(idx < 16);
      fprintf(stderr, "init pools %d: %d %d on node %d\n", idx, i, proposed_caps[idx], node);
      new (&pools[idx]) PoolType(i, proposed_caps[idx], node);
    }
  }

  void *Alloc(size_t sz) {
    assert(SizeToClass(sz) < 16 && SizeToClass(sz) >= 0);
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

typedef Region<false> ThreadLocalRegion;

void InitThreadLocalRegions(int tot);
ThreadLocalRegion &GetThreadLocalRegion(int idx);

}

#endif /* MEM_H */
