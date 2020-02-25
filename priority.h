#ifndef PRIORITY_H
#define PRIORITY_H

#include "masstree_index_impl.h"

namespace felis {

class PriorityTxnService {
 private:
  // per-core progress, the maximum piece sid each core has started executing
  std::array<uint64_t*, NodeConfiguration::kMaxNrThreads> exec_progress;

 public:
  PriorityTxnService();

  inline bool UpdateProgress(int core_id, uint64_t progress) {
    abort_if(exec_progress[core_id] == nullptr, "priority service init failure");
    if (progress > *exec_progress[core_id])
      *exec_progress[core_id] = progress;
    return true;
  }

  uint64_t GetMaxProgress(void) {
    uint64_t max = 0;
    for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i)
      max = (*exec_progress[i] > max) ? *exec_progress[i] : max;
    return max;
  }

  bool HasProgressPassed(uint64_t sid) {
    for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
      if (*exec_progress[i] > sid)
        return true;
    }
    return false;
  }

  void PrintProgress(void) {
    for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
      printf("progress on core %2d: node_id %lu, epoch %lu, txn sequence %lu\n",
             i, *exec_progress[i] & 0x000000FF, *exec_progress[i] >> 32,
             *exec_progress[i] >> 8 & 0xFFFFFF);
    }
  }

 private:
  uint64_t GetSIDLowerBound();
 public:
  uint64_t GetAvailableSID();
};


class PriorityTxn {
 public:
  PriorityTxn() : sid(-1), initialized(false) {}
  virtual bool Run() = 0;

 private:
  bool initialized; // meaning the registered VHandles would be valid
  std::vector<VHandle*> update_handles;
  uint64_t sid;

 protected: // APIs for subclass txn to implement the workload
  uint64_t serial_id() { return sid; }
  template <typename Table>
  bool InitRegisterUpdate(std::vector<typename Table::Key> keys,
                          std::vector<VHandle*>& handles);

  template <typename Table>
  bool InitRegisterInsert(std::vector<typename Table::Key> keys,
                          std::vector<VHandle*>& handles);

  bool Init();


  template <typename T>
  T Read(VHandle* handle) {
    if (!initialized)
      std::abort(); // you must call Init() before you use the VHandle
    return handle->ReadWithVersion(this->sid)->ToType<T>();
  }

  template <typename T>
  bool Write(VHandle* handle, const T &o) {
    if (!initialized)
      std::abort();
    return handle->WriteWithVersion(sid, o.Encode(), sid >> 32);
  }


  // if doing OCC, check write set and commit and stuff
  bool Commit() {
    if (!initialized)
      std::abort();
    return true;
  }
};

} // namespace felis

namespace util {

template <>
struct InstanceInit<felis::PriorityTxnService> {
  static constexpr bool kHasInstance = true;
  static inline felis::PriorityTxnService *instance;

  InstanceInit() {
    instance = new felis::PriorityTxnService();
  }
};

}

#endif /* PRIORITY_H */
