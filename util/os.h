#ifndef UTIL_OS_H
#define UTIL_OS_H

#include <cstddef>
#include <cstdint>

namespace util {

class Cpu {
  void *os_cpuset; // OS specific
  size_t nr_processors;
 public:
  Cpu();
  ~Cpu();

  void set_affinity(int cpu);
  void Pin();

  size_t get_nr_processors() const { return nr_processors; }

  // TODO: maybe add NUMA information here?
};

class OSMemory {
  intptr_t mem_map_desc;
  static size_t AlignLength(size_t length);
 public:
  OSMemory();
  // TODO: constructor if we want to write to NVM backed file?

  void *Alloc(size_t length, int numa_node = -1, bool on_demand = false);
  void *PmemAlloc(char* filename, size_t length, int numa_node = -1, void *addr = nullptr, bool on_demand = false);
  bool PmemMap(char* filename, size_t length, void *addr = nullptr);

  void Free(void *p, size_t length);

  static void BindMemory(void *p, size_t length, int numa_node);
  static void LockMemory(void *p, size_t length);

  static OSMemory g_default;
};

}

#endif
