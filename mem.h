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
#include <fstream>

#include "json11/json11.hpp"

namespace mem {

const int kNrCorePerNode = 8;

template <bool LockRequired = false>
class Pool {
  void *data;
  size_t len;
  std::atomic<void *> head;
  size_t capacity;
  unsigned char __pad__[32];

public:
  Pool() : data(nullptr), len(0) {}

  Pool(size_t chunk_size, size_t cap, int numa_node = -1)
    : len(cap * chunk_size), capacity(cap) {
    if (cap == 0) return;

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
    if (flags & MAP_HUGETLB) {
      pgsz = (2 << 20);
    }
    for (volatile uint8_t *p = (uint8_t *) data; p < (uint8_t *) data + len; p += pgsz) {
      fprintf(stderr, "prefaulting %s %lu%%\r",
	      (flags & MAP_HUGETLB) ? "hugepage" : "        ",
	      (p - (uint8_t *) data) * 100 / len);
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

  Pool(const Pool &rhs) = delete;

  ~Pool() {
    if (__builtin_expect(data != nullptr, true))
      munmap(data, len);
  }

  // TODO: optimize this?
  void *Alloc() {
    void *r = nullptr, *next = nullptr;
  again:
    r = head.load();
    if (r == nullptr) return nullptr;

    if (r < (uint8_t *) data || r >= (uint8_t *) data + len) {
      fprintf(stderr, "memory pool not large enough!");
      std::abort();
    }

    next = (void *) *(uintptr_t *) head.load();

    if (LockRequired) {
      if (!head.compare_exchange_strong(r, next)) goto again;
    } else {
      if (!head.compare_exchange_weak(r, next)) goto again;
    }

    return r;
  }

  void Free(void *ptr) {
    void *r = nullptr;
  again:
    r = head;
    *(uintptr_t *) ptr = (uintptr_t) r;
    if (LockRequired) {
      if (!head.compare_exchange_strong(r, ptr)) goto again;
    } else {
      if (!head.compare_exchange_weak(r, ptr)) goto again;
    }
  }
};

static_assert(sizeof(Pool<true>) == 64, "Pool object size is not 64 bytes");

template <bool LockRequired = false>
class Region {
  typedef Pool<LockRequired> PoolType;
  static const int kMaxPools = 20;
  // static const int kMaxPools = 12;
  PoolType pools[32];
  size_t proposed_caps[32];
public:
  Region() {
    for (int i = 0; i < kMaxPools; i++) {
      proposed_caps[i] = 1 << (27 - 5 - i);
    }
  }

  Region(const Region &) = delete;

  static int SizeToClass(size_t sz) {
    int idx = 64 - __builtin_clzl(sz - 1) - 5;
    assert(idx < kMaxPools);
    return idx < 0 ? 0 : idx;
  }

  void ApplyFromConf(std::string filename) {
    std::ifstream fin(filename);
    std::string conf_text {std::istreambuf_iterator<char>(fin),
	std::istreambuf_iterator<char>()};
    std::string err;
    json11::Json conf_doc = json11::Json::parse(conf_text, err);

    if (!err.empty()) {
      fprintf(stderr, "%s\n", err.c_str());
      return;
    }

    auto json_map = conf_doc.object_items();
    for (auto it = json_map.begin(); it != json_map.end(); ++it) {
      set_pool_capacity(atoi(it->first.c_str()), it->second.int_value() << 20);
    }
  }

  void set_pool_capacity(size_t sz, size_t cap) {
    proposed_caps[SizeToClass(sz)] = cap;
    fprintf(stderr, "%lu, bin %d, cap %lu, estimate size %.1lfMB\n", sz,
	    SizeToClass(sz), cap,
	    (1 << (SizeToClass(sz) + 5)) * cap * 1. / 1024 / 1024);
  }

  void InitPools(int node = -1) {
    for (int i = 0; i < kMaxPools; i++) {
      new (&pools[i]) PoolType(1 << (i + 5), proposed_caps[i], node);
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
