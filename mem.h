#ifndef MEM_H
#define MEM_H

#include <sys/types.h>
#include <sys/mman.h>
#include <string>

namespace mem {

template <size_t ChunkSize>
class LargePool {
  void *data;
  void *head;
  size_t cap;
public:
  LargePool(size_t capacity) : cap(capacity) {
    data = mmap(nullptr, ChunkSize * cap, PROT_READ | PROT_WRITE,
		MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_POPULATE,
		-1, 0);
    head = data;
    for (int i = 0; i < cap; i++) {
      uintptr_t p = (uintptr_t) head + i * ChunkSize;
      uintptr_t ptr = p + ChunkSize;
      if (i == cap - 1) ptr = 0;
      *(uintptr_t *) p = ptr;
    }
  }
  ~LargePool() {
    munmap(data, ChunkSize * cap);
  }

  void *Alloc() {
    void *r = head;
    uintptr_t ptr = *(uintptr_t *) head;
    head = (void *) ptr;
    return r;
  }

  void Free(void *ptr) {
    *(uintptr_t *) ptr = (uintptr_t) head;
    head = ptr;
  }
};

}

#endif /* MEM_H */
