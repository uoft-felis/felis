#ifndef PRIORITY_H
#define PRIORITY_H

#include "masstree_index_impl.h"

namespace felis {

class PriorityTxn {
 public:
  PriorityTxn() : sid(-1), epoch_nr(-1), initialized(false) {}
  virtual bool Run() = 0;

 private:
  bool initialized;
  std::vector<VHandle*> update_handles;

 protected: // in principal these two could be private
  uint64_t epoch_nr;
  uint64_t sid;

 private:
  static uint64_t GetDistance(bool backoff = false);
  bool FindAvailableSID();

 protected: // APIs for subclass txn to implement the workload
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
    return handle->WriteWithVersion(sid, o.Encode(), epoch_nr);
  }


  // if doing OCC, check write set and commit and stuff
  bool Commit() {
    if (!initialized)
      std::abort();
    return true;
  }
};

class PriorityTxnService {
 public:
  PriorityTxnService();

  inline bool UpdateProgress(int core_id, uint64_t progress) {
    abort_if(exec_progress[core_id] == nullptr, "priority service init failure");
    if (progress > *exec_progress[core_id])
      *exec_progress[core_id] = progress;
    return true;
  }

  void PrintProgress(void) {
    for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
      printf("progress on core %d: node_id %lu, epoch %lu, txn sequence %lu\n",
             i,
             *exec_progress[i] & 0x000000FF,
             *exec_progress[i] >> 32,
             *exec_progress[i] >> 8 & 0xFFFFFF);
    }
  }

 private:
  // per-core progress, the maximum piece sid each core has started executing
  std::array<uint64_t*, NodeConfiguration::kMaxNrThreads> exec_progress;
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
