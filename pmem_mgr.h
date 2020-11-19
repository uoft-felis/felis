#include "util/locks.h"
#include "util/objects.h"
namespace felis {

class PmemManager {
 util::SpinLock pmem_lock;
 public:
  uint8_t * pmem_addr;
  int total_bytes_used;
  PmemManager();
  void *Alloc(uint16_t num_bytes);
  void Free(void *ptr);

};

}


namespace util {
template <> struct InstanceInit<felis::PmemManager> {
  static constexpr bool kHasInstance = true;
  static inline felis::PmemManager *instance;
  InstanceInit();
};

}