#ifndef PRIORITY_H
#define PRIORITY_H

#include "epoch.h"
#include "masstree_index_impl.h"

namespace felis {

class PriorityTxn;

class PriorityTxnService {
 private:
  // per-core progress, the maximum piece sid each core has started executing
  std::array<uint64_t*, NodeConfiguration::kMaxNrThreads> exec_progress;
  std::atomic_int core;

 public:
  static size_t g_queue_length;

  PriorityTxnService();
  void PushTxn(PriorityTxn* txn);

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
 private:
  bool (*callback)(PriorityTxn *);
  bool initialized; // meaning the registered VHandles would be valid
  std::vector<VHandle*> update_handles;
  uint64_t sid;

 public:
  PriorityTxn(bool (*func)(PriorityTxn *)): sid(-1), initialized(false),
                                            update_handles(), callback(func) {}
  PriorityTxn() : PriorityTxn(nullptr) {}

  bool Run() {
    return this->callback(this);
  }

 public: // APIs for the callback to use
  uint64_t serial_id() { return sid; }

  // find the VHandle in Masstree, store it, return success or not
  template <typename Table>
  bool InitRegisterUpdate(std::vector<typename Table::Key> keys,
                          std::vector<VHandle*>& handles) {
    if (this->initialized)
      return false;

    for (auto key : keys) {
      int table = static_cast<int>(Table::kTable);
      auto &rel = util::Instance<RelationManager>()[table];
      auto keyVarStr = key.Encode();

      auto handle = rel.SearchOrDefault(keyVarStr, [] { std::abort(); return nullptr; });
      // it's an update, you should always find it

      this->update_handles.push_back(handle);
      handles.push_back(handle);
    }
    return true;
  }

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


  template <typename T, typename Lambda>
  void IssuePromise(T input, Lambda lambda) {
    auto capture = std::make_tuple(input);
    auto empty_input = felis::VarStr::FromAlloca(alloca(sizeof(felis::VarStr)), sizeof(felis::VarStr));

    // convert non-capture lambda to constexpr func ptr
    constexpr void (*native_func)(std::tuple<T> capture) = lambda;

    // this static func is what the worker will actually run.
    //   (in ExecutionRoutine::RunPromiseRoutine())
    // it decodes routine->capture_data into capture, and pass it back to lambda
    using serializer = sql::Serializer<std::tuple<T>>;
    auto static_func =
        [](felis::PromiseRoutine *routine, felis::VarStr input) {
            std::tuple<T> capture;
            serializer::DecodeFrom(&capture, routine->capture_data);
            native_func(capture);
        };

    // construct PromiseRoutine
    auto routine = PromiseRoutine::CreateFromCapture(serializer::EncodeSize(&capture));
    routine->callback = static_func;
    routine->sched_key = this->serial_id();
    serializer::EncodeTo(routine->capture_data, &capture);

    // put PromiseRoutineWithInput into PQ
    EpochClient::g_workload_client->completion_object()->Increment(1);
    PromiseRoutineWithInput tuple = std::make_tuple(routine, *empty_input);
    int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    util::Impl<felis::PromiseRoutineDispatchService>().Add(core_id, &tuple, 1);
    logger->info("I think I have put the lambda into the PQ!");
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
