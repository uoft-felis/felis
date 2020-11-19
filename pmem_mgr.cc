#include <cstdlib>
#include <cstdio>
#include <sys/mman.h>
#include "pmem_mgr.h"

#define FOUR_GB 4294967296

namespace felis {

PmemManager::PmemManager()
{
  //not using PmemManager...
  std::printf("WARNING!!! PmemManager Disabled!!!\n");
  pmem_addr = (uint8_t *)0;//(uint8_t *)mmap(nullptr, FOUR_GB, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  total_bytes_used = 0;
}

void * PmemManager::Alloc(uint16_t num_bytes)
{
    //align to 4 bytes
    uint8_t * alloc_addr = pmem_addr + total_bytes_used;
    if (num_bytes % 4)
    {
        num_bytes += 4 - (num_bytes%4);
    }
    total_bytes_used += num_bytes;
    if (total_bytes_used > (int)FOUR_GB)
    {
        std::printf("PmemManager not enough memory!\n");
        std::abort();
    }
    return alloc_addr;
}


void PmemManager::Free(void *ptr)
{
    // implement later. ignore for now
    return;
}

}

namespace util {
using namespace felis;
InstanceInit<PmemManager>::InstanceInit()
{
  instance = new PmemManager();
}

}
