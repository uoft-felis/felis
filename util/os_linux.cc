#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <cstdio>
#include <cstdlib>
#include <sys/mman.h>

#include <syscall.h>

#include "util/arch.h"
#include "os.h"

namespace util {

Cpu::Cpu()
{
  auto s = new cpu_set_t;
  CPU_ZERO(s);
  os_cpuset = s;
  nr_processors = sysconf(_SC_NPROCESSORS_CONF);
}

Cpu::~Cpu() {}

void Cpu::set_affinity(int cpu)
{
  if (cpu >= nr_processors) {
    fprintf(stderr, "Cannot set processor affinity %d, total number of processors %lu\n",
            cpu, nr_processors);
  }
  CPU_SET(cpu, (cpu_set_t *) os_cpuset);
}

void Cpu::Pin()
{
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), (cpu_set_t *) os_cpuset);
  sched_yield();
}

OSMemory::OSMemory()
    : mem_map_desc(-1)
{}

size_t OSMemory::AlignLength(size_t length)
{
  if (length >= 2 << 20) {
    length = util::Align(length, 2 << 20);
  } else {
    length = util::Align(length, 4 << 10);
  }
  return length;
}

void *OSMemory::Alloc(size_t length, int numa_node, bool on_demand)
{
  int flags = MAP_ANONYMOUS | MAP_PRIVATE;
  int prot = PROT_READ | PROT_WRITE;
  length = AlignLength(length);

  if (length >= 2 << 20) flags |= MAP_HUGETLB;

  void *mem = mmap(nullptr, length, prot, flags, (int) mem_map_desc, 0);
  if (mem == MAP_FAILED)
    return nullptr;

  if (numa_node != -1) BindMemory(mem, length, numa_node);
  if (!on_demand) LockMemory(mem, length);

  return mem;
}

void OSMemory::Free(void *p, size_t length)
{
  length = AlignLength(length);
  munmap(p, length);
}

void OSMemory::LockMemory(void *p, size_t length)
{
  if (mlock(p, length) < 0) {
    fprintf(stderr, "WARNING: mlock() failed\n");
    perror("mlock");
    std::abort();
  }
}

void OSMemory::BindMemory(void *p, size_t length, int numa_node)
{
  int nodemask = 1 << numa_node;
  if (syscall(
          __NR_mbind,
          p, length,
          2 /* MPOL_BIND */,
          &nodemask,
          sizeof(unsigned long) * 8,
          1 << 0 /* MPOL_MF_STRICT */) < 0) {
    fprintf(stderr, "Fail to mbind on address %p length %lu numa_node %d\n",
            p, length, numa_node);
    std::abort();
  }
}

OSMemory OSMemory::g_default;

}
