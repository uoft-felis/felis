#ifndef PRIORITY_H
#define PRIORITY_H

#include "epoch.h"
#include "masstree_index_impl.h"
#include "sqltypes.h"
#include "json11/json11.hpp"

namespace felis {

class PriorityTxn;

class PriorityTxnService {
  friend class PriorityTxn;
  // per-core progress, the maximum piece sid each core has started executing
  std::array<uint64_t*, NodeConfiguration::kMaxNrThreads> exec_progress;
  std::array<uint64_t*, NodeConfiguration::kMaxNrThreads> local_last_sid;
  std::atomic_int core;
  std::atomic_ulong epoch_nr;
  uint64_t global_last_sid;
  util::SpinLock lock; // for global_last_sid

 public:
  class Bitmap {
    std::vector<bool> bitset;
    size_t size;
   public:
    static const int extra_strip = 1000;
    Bitmap() = delete;
    Bitmap(const Bitmap& rhs) = delete;
    Bitmap(size_t _size) {
      size = _size + extra_strip;
      bitset.resize(size, false);
    }
    void clear() {
      bitset.clear();
      bitset.resize(size, false);
    }
    void set(int idx, bool value) {
      abort_if(idx < 0 || idx >= size, "bitmap access out of bound, idx={}, size={}", idx, size);
      bitset[idx] = value;
    }
    // find first element after idx that is false, set it to true
    int set_first_unset_idx(int idx) {
      abort_if(idx < 0 || idx >= size, "bitmap access out of bound, idx={}, size={}", idx, size);
      for (int i = idx; i < size; ++i) {
        if (bitset[i] == false) {
          set(i, true);
          return i;
        }
      }
      return -1;
    }
  };
  std::array<Bitmap*, NodeConfiguration::kMaxNrThreads> seq_bitmap;

  int idx2seq(int idx, int core_id)
  {
    // idx [0, nr_batch_txns - 1], core_id [0, nr_cores - 1]
    size_t nr_batch_txns = EpochClient::g_txn_per_epoch;
    size_t nr_cores = NodeConfiguration::g_nr_threads;
    if (idx < 0 || idx >= nr_batch_txns + Bitmap::extra_strip || core_id < 0 || core_id >= nr_cores) {
      logger->critical("idx2seq access out of bound, idx {}, core_id {}", idx, core_id);
      std::abort();
    }
    /*
      e.g. kStripBatched = 2, kStripPriority = nr_cores = 32, then
        \  core_id |   batched    0   1   2   ...   30  31
      idx  \       |
      -------------
            0           1   2     3   4   5   ...   33  34
            1           35  36    37  38  39  ...   67  68
      (seq starts with 1)
    */
    int k = g_strip_batched + g_strip_priority;
    return idx * k + g_strip_batched + core_id + 1;
  }

  int seq2idx(int seq)
  {
    int k = g_strip_batched + g_strip_priority;
    size_t seq_max = k * (EpochClient::g_txn_per_epoch / g_strip_batched + Bitmap::extra_strip) +
                     EpochClient::g_txn_per_epoch % g_strip_batched;
    if (seq < 1 || seq > seq_max) {
      logger->critical("seq2idx access out of bound, seq {}", seq);
      std::abort();
    }
    return (seq - 1) / k;
  }

  // total number of priority txn queue length
  static size_t g_queue_length;

  static size_t g_nr_priority_txn;       // number of priority txns per epoch
  static size_t g_interval_priority_txn; // interval of priority txn, in nanoseconds

  // for example, if strip_batched = 8 and strip_priority = 8, then
  // txn 1 - 8 are batched txns,  9-16 are priority txn slots,
  // txn 17-24 are batched txns, 25-32 are priority txn slots, ...
  static size_t g_strip_batched;
  static size_t g_strip_priority;

  static bool g_sid_global_inc;
  static bool g_sid_local_inc;
  static bool g_sid_bitmap;

  static bool g_read_bit;             // mark read bit
  static bool g_conflict_read_bit;    // use read bit info to detect conflict
  static bool g_sid_read_bit;         // use read bit info to acquire SID
  static bool g_sid_forward_read_bit; // use read bit info for SID, direction forward in time

  static int g_backoff_distance; // how many distance we are going to backoff, could be + or -
  static bool g_distance_exponential_backoff; /// only works if g_backoff_distance is -
  static bool g_fastest_core;

  PriorityTxnService();
  void PushTxn(PriorityTxn* txn);
  void UpdateProgress(int core_id, uint64_t progress);
  void PrintProgress(void);
  uint64_t GetMaxProgress(void);
  int GetFastestCore(void);
  uint64_t GetProgress(int core_id);
  bool MaxProgressPassed(uint64_t sid);
  static bool isPriorityTxn(uint64_t sid);
  void ClearBitMap(void);

 private:
  uint64_t SIDLowerBound();
  uint64_t GetSID(PriorityTxn *txn, VHandle **handles, int size);
  uint64_t GetNextSIDSlot(uint64_t sequence);

 public:
  static void PrintStats();
  static unsigned long long g_tsc;
};

struct BaseInsertKey {
  BaseInsertKey() { }
  virtual VHandle* Insert(uint64_t sid) = 0;
};

template <typename Table>
struct InsertKey : public BaseInsertKey {
  typename Table::Key key;
  InsertKey(typename Table::Key _key) : key(_key) { }

  virtual VHandle* Insert(uint64_t sid) final override{
    int table = static_cast<int>(Table::kTable);
    auto rel = util::Instance<TableManager>().GetTable(table);
    return rel->PriorityInsert(key.Encode(), sid);
  }
};

class PriorityTxn {
 friend class BasePieceCollection::ExecutionRoutine;
 friend class PriorityTxnService;
 private:
  uint64_t sid;
  bool (*callback)(PriorityTxn *);
  bool initialized; // meaning the registered VHandles would be valid

 public:
  int backoff_distance;
  std::atomic_int piece_count;
  uint64_t epoch; // pre-generate. which epoch is this txn in
  uint64_t delay; // pre-generate. tsc delay w.r.t. the start of execution, in tsc
  uint64_t measure_tsc;
  uint64_t min_sid; // input, this txn cannot serialize before min_sid
  PriorityTxn(bool (*func)(PriorityTxn *)): sid(-1), initialized(false),
                                            piece_count(0), callback(func) {}
  PriorityTxn& operator=(const PriorityTxn& rhs) {
    if (&rhs == this)
      return *this;

    // assign only happens when the rhs is never Run(), so set init value
    this->piece_count.store(0);
    this->min_sid = 0;
    this->backoff_distance = PriorityTxnService::g_backoff_distance;

    this->sid = rhs.sid;
    this->initialized = rhs.initialized;
    this->callback = rhs.callback;
    this->epoch = rhs.epoch;
    this->delay = rhs.delay;
    this->measure_tsc = rhs.measure_tsc;
    // logger->info("from {:p} to {:p}", (void*)&rhs, (void*)this);
    return *this;
  }
  PriorityTxn() : PriorityTxn(nullptr) {}
  void SetCallback(bool (*func)(PriorityTxn *)) { this->callback = func; }

  bool Run() {
    return this->callback(this);
  }

  template <typename Table>
  VHandle* SearchExistingRow(typename Table::Key key) {
    int table = static_cast<int>(Table::kTable);
    auto rel = util::Instance<TableManager>().GetTable(table);
    auto keyVarStr = key.Encode();
    return rel->Search(keyVarStr->ToView(), this->sid);
  }

  bool CheckUpdateConflict(VHandle* handle);

 public: // APIs for the callback to use
  uint64_t serial_id() { return sid; }

  template <typename Table>
  bool InitRegisterUpdate(typename Table::Key key, VHandle*& handle) {
    if (this->initialized)
      std::abort();
    handle = SearchExistingRow<Table>(key);
    abort_if(!handle, "Registring Update: found nullptr");
    return true;
  }

  template <typename Table>
  bool InitRegisterInsert(typename Table::Key key, BaseInsertKey*& ptr) {
    ptr = new felis::InsertKey<Table>(key);
    return true;
  }

  bool Init(VHandle **update_handles, int usize, BaseInsertKey **insert_ikeys, int isize, VHandle **insert_handles);

  void Rollback(VHandle **update_handles, int update_cnt, VHandle **insert_handles, int insert_cnt);

  template <typename T>
  T Read(VHandle* handle) {
    // you must call Init() before you use the VHandle
    abort_if(!initialized, "before Read, vhandle must be initialized, this {:p}", (void*) this)
    return handle->ReadWithVersion(this->sid)->ToType<T>();
  }

  template <typename T>
  bool Write(VHandle* handle, const T &o) {
    abort_if(!initialized, "before Write, vhandle must be initialized, this {:p}", (void*) this)
    return handle->WriteWithVersion(sid, o.Encode(), sid >> 32);
  }

  bool Delete(VHandle* handle) {
    abort_if(!initialized, "before Delete, vhandle must be initialized, this {:p}", (void*) this)
    return handle->WriteWithVersion(sid, nullptr, sid >> 32);
  }

  template <typename T, typename Lambda>
  void IssuePromise(T input, Lambda lambda, int core_id = -1) {
    if (core_id == -1) {
      if (PriorityTxnService::g_fastest_core)
        core_id = util::Instance<felis::PriorityTxnService>().GetFastestCore();
      else
        core_id = go::Scheduler::CurrentThreadPoolId() - 1; // issue to current core
    }
    auto capture = std::make_tuple(input);

    // convert non-capture lambda to constexpr func ptr
    constexpr void (*native_func)(std::tuple<T> capture) = lambda;

    // this static func is what the worker will actually run.
    //   (in ExecutionRoutine::RunPromiseRoutine())
    // it decodes routine->capture_data into capture, and pass it back to lambda
    using serializer = sql::Serializer<std::tuple<T>>;
    auto static_func =
        [](PieceRoutine *routine) {
            std::tuple<T> capture;
            serializer::DecodeFrom(&capture, routine->capture_data);
            native_func(capture);
        };

    // construct PromiseRoutine
    auto routine = PieceRoutine::CreateFromCapture(serializer::EncodeSize(&capture));
    routine->callback = static_func;
    routine->sched_key = this->serial_id();
    routine->affinity = core_id;
    serializer::EncodeTo(routine->capture_data, &capture);

    // put PromiseRoutineWithInput into PQ
    EpochClient::g_workload_client->completion_object()->Increment(1);
    util::Impl<felis::PromiseRoutineDispatchService>().Add(core_id, &routine, 1);
  }

  // if doing OCC, check write set and commit and stuff
  bool Commit() {
    abort_if(!initialized, "before Commit, vhandle must be initialized, this {:p}", (void*) this)
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
