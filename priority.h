#ifndef PRIORITY_H
#define PRIORITY_H

#include "txn.h"
#include "masstree_index_impl.h"

namespace felis {

static std::array<uint64_t*, NodeConfiguration::kMaxNrThreads> g_exec_progress;

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

}

#endif /* PRIORITY_H */
