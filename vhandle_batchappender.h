#ifndef VHANDLE_BATCHAPPENDER_H
#define VHANDLE_BATCHAPPENDER_H

#include "vhandle.h"

namespace felis {

struct VersionBufferHandle {
  uint8_t *prealloc_ptr;
  long pos;

  void Append(VHandle *handle, uint64_t sid, uint64_t epoch_nr);
  void FlushIntoNoLock(VHandle *handle, uint64_t epoch_nr, unsigned int end);
};

struct VersionBufferHead;

class BatchAppender {
  friend class VersionBufferHead;
  std::array<VersionBufferHead *, NodeConfiguration::kMaxNrThreads> buffer_heads;
  uint64_t cw_begin, cw_end;

 public:
  BatchAppender();
  VersionBufferHandle GetOrInstall(VHandle *handle);
  void FinalizeFlush(uint64_t epoch_nr);
  void Reset();
  uint64_t contention_weight_begin() const { return cw_begin; }
  uint64_t contention_weight_end() const { return cw_end; }
  int GetRowContentionAffinity(VHandle *row) const;
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
