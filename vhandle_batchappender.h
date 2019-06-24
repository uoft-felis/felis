#ifndef VHANDLE_BATCHAPPENDER_H
#define VHANDLE_BATCHAPPENDER_H

#include "vhandle.h"

namespace felis {

struct VersionBuffer {
  static constexpr size_t kMaxBatch = 15;
  size_t buf_cnt;
  uint64_t versions[kMaxBatch];

  void Append(uint64_t sid) { versions[buf_cnt++] = sid; }
  void FlushInto(VHandle *handle, uint64_t epoch_nr);
};

struct VersionBufferHead;

class BatchAppender {
  std::array<VersionBufferHead *, NodeConfiguration::kMaxNrThreads> buffer_heads;
 public:
  BatchAppender();
  VersionBuffer *GetOrInstall(VHandle *handle);
  void FinalizeFlush(uint64_t epoch_nr);
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
