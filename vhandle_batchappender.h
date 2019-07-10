#ifndef VHANDLE_BATCHAPPENDER_H
#define VHANDLE_BATCHAPPENDER_H

#include "vhandle.h"

namespace felis {

struct VersionBufferHandle {
  uint8_t *prealloc_ptr;
  long pos;

  void Append(VHandle *handle, uint64_t sid, uint64_t epoch_nr);
  void FlushIntoNoLock(VHandle *handle, uint64_t epoch_nr);
};

struct VersionBufferHead;

class BatchAppender {
  friend class VersionBufferHead;
  std::array<VersionBufferHead *, NodeConfiguration::kMaxNrThreads> buffer_heads;
 public:
  BatchAppender();
  VersionBufferHandle GetOrInstall(VHandle *handle);
  void FinalizeFlush(uint64_t epoch_nr);
  void Reset();
};

}

namespace util {

template <> struct InstanceInit<felis::BatchAppender> {
  static constexpr bool kHasInstance = true;
  static inline felis::BatchAppender *instance;
  InstanceInit();
};

}

#endif
