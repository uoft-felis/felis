#ifndef EPOCH_H
#define EPOCH_H

#include <cstdint>
#include <array>
#include <map>
#include "node_config.h"
#include "util.h"
#include "mem.h"
#include "completion.h"
#include "shipping.h"

namespace felis {

class Epoch;
class BaseTxn;
class EpochClient;

using TxnMemberFunc = void (BaseTxn::*)();

class EpochClientBaseWorker : public go::Routine {
 protected:
  static constexpr int kBlock = 32;
  int t;
  int nr_threads;
  EpochClient *client;
 public:
  EpochClientBaseWorker(int t, EpochClient *client)
      : t(t), nr_threads(NodeConfiguration::g_nr_threads), client(client) {
    set_reuse(true);
  }
  void Reset() { Init(); ctx = nullptr; }
};

class RunTxnPromiseWorker : public EpochClientBaseWorker {
 public:
  using EpochClientBaseWorker::EpochClientBaseWorker;
  void Run() override final;
};

class CallTxnsWorker : public EpochClientBaseWorker {
  TxnMemberFunc mem_func;
 public:
  using EpochClientBaseWorker::EpochClientBaseWorker;
  void Run() override final;
  void set_function(TxnMemberFunc func) { mem_func = func; }
};

struct EpochWorkers {
  RunTxnPromiseWorker run_worker;
  CallTxnsWorker call_worker;

  EpochWorkers(int t, EpochClient *client)
      : run_worker(t, client), call_worker(t, client) {}
};

enum EpochPhase : int {
  CallInsert,
  Insert,
  CallInitialize,
  Initialize,
  CallExecute,
  Execute,
};

class EpochCallback {
  friend class EpochClient;
  PerfLog perf;
  EpochClient *client;
  const char *label;
  EpochPhase phase;
 public:
  EpochCallback(EpochClient *client) : client(client), label(nullptr) {}
  void operator()();
  void RunBackgroundWork();
};

class EpochClient {
  friend class EpochCallback;
  friend class RunTxnPromiseWorker;
  friend class CallTxnsWorker;
  PerfLog perf;
  EpochWorkers *workers[NodeConfiguration::kMaxNrThreads];
 public:
  static EpochClient *g_workload_client;

  EpochClient() noexcept;
  virtual ~EpochClient() {}

  void GenerateBenchmarks();
  void Start();

  auto completion_object() { return &completion; }

  virtual unsigned int LoadPercentage() = 0;
  unsigned long NumberOfTxns() {
    return LoadPercentage() * kTxnPerEpoch / 100;
  };

  static constexpr size_t kTxnPerEpoch = 100000;
  static constexpr size_t kMaxEpoch = 50;
 protected:
  friend class BaseTxn;
  friend class EpochCallback;

  void InitializeEpoch();
  void ExecuteEpoch();

  uint64_t GenerateSerialId(uint64_t epoch_nr, uint64_t sequence);

  virtual BaseTxn *CreateTxn(uint64_t serial_id) = 0;

 private:
  void RunTxnPromises(const char *label);
  void CallTxns(uint64_t epoch_nr, TxnMemberFunc func);
  void CallTxnsOnComplete(bool sync = true);

  void OnCallInsertComplete();
  void OnInsertComplete();
  void OnCallInitializeComplete();
  void OnInitializeComplete();
  void OnCallExecuteComplete();
  void OnExecuteComplete();

 protected:
  EpochCallback callback;
  CompletionObject<EpochCallback &> completion;

  BaseTxn **all_txns;
  BaseTxn **txns;
  unsigned long total_nr_txn;
  unsigned long *per_core_cnts[NodeConfiguration::kMaxNrThreads];

  bool disable_load_balance;

  NodeConfiguration &conf;
};

class EpochManager {
  mem::Pool *pool;

  template <typename T> friend struct util::InstanceInit;
  // std::array<Epoch *, kMaxConcurrentEpochs> concurrent_epochs;
  util::OwnPtr<Epoch> cur_epoch;
  uint64_t cur_epoch_nr;

  EpochManager(mem::Pool *pool);
 public:
  Epoch *epoch(uint64_t epoch_nr) const;
  uint8_t *ptr(uint64_t epoch_nr, int node_id, uint64_t offset) const;

  uint64_t current_epoch_nr() const { return cur_epoch_nr; }
  Epoch *current_epoch() const { return epoch(cur_epoch_nr); }

  void DoAdvance(EpochClient *client);
};

class EpochObject {
  friend class Epoch;
 protected:
  uint64_t epoch_nr;
  int node_id;
  uint64_t offset;
 public:
  EpochObject(uint64_t epoch_nr, int node_id, uint64_t offset) : epoch_nr(epoch_nr), node_id(node_id), offset(offset) {}
  EpochObject() : epoch_nr(0), node_id(0), offset(0) {}

  int origin_node_id() const { return node_id; }
  uint64_t nr() const { return epoch_nr; }
};

template <typename T>
class GenericEpochObject : public EpochObject {
  friend class Epoch;
 public:
  using EpochObject::EpochObject;

  GenericEpochObject(const EpochObject &obj) : EpochObject(obj) {}

  operator T*() const {
    return this->operator->();
  }

  T *operator->() const {
    return (T *) util::Instance<EpochManager>().ptr(epoch_nr, node_id, offset);
  }

  template <typename P>
  GenericEpochObject<P> Convert(P *ptr) {
    uint8_t *p = (uint8_t *) ptr;
    uint8_t *self = util::Instance<EpochManager>().ptr(epoch_nr, node_id, offset);
    int64_t off = p - self;
    return GenericEpochObject<P>(epoch_nr, node_id, offset + off);
  }
};

// This where we store objects across the entire cluster. Note that these
// objects are replicated but not synchronized. We need to make these objects
// perfectly partitioned.
//
// We mainly use this to store the transaction execution states, but we could
// store other types of POJOs as well.
//
// The allocator is simply an brk. Objects were replicated by replicating the
// node_id and the offset.
class EpochMemory {
 protected:
  mem::Brk brks[NodeConfiguration::kMaxNrNode];
  mem::Pool *pool;

  friend class EpochManager;
  EpochMemory(mem::Pool *pool);
  ~EpochMemory();
 public:
};

class Epoch : public EpochMemory {
 protected:
  uint64_t epoch_nr;
  EpochClient *client;
  friend class EpochManager;

  // std::array<int, NodeConfiguration::kMaxNrNode> counter;
 public:
  Epoch(uint64_t epoch_nr, EpochClient *client, mem::Pool *pool) : epoch_nr(epoch_nr), client(client), EpochMemory(pool) {}
  template <typename T>
  GenericEpochObject<T> AllocateEpochObject(int node_id) {
    auto off = brks[node_id - 1].current_size();
    brks[node_id - 1].Alloc(util::Align(sizeof(T)));
    return GenericEpochObject<T>(epoch_nr, node_id, off);
  }

  template <typename T>
  GenericEpochObject<T> AllocateEpochObjectOnCurrentNode() {
    return AllocateEpochObject<T>(util::Instance<NodeConfiguration>().node_id());
  }

  uint64_t id() const { return epoch_nr; }

  EpochClient *epoch_client() const { return client; }
};


// For scheduling transactions during execution
class EpochExecutionDispatchService : public PromiseRoutineDispatchService {
  template <typename T> friend T &util::Instance() noexcept;
  EpochExecutionDispatchService();

  using ExecutionRoutine = BasePromise::ExecutionRoutine;

  struct CompleteCounter {
    ulong completed;
    CompleteCounter() : completed(0) {}
  };
 public:

  enum PriorityQueueLinkListEnum {
    kHashList,
    kValueList,
  };

  struct PriorityQueueValue : public util::GenericListNode<PriorityQueueValue> {
    PromiseRoutineWithInput promise_routine;
    BasePromise::ExecutionRoutine *state;
  };

  struct PriorityQueueHashEntry : public util::GenericListNode<PriorityQueueHashEntry> {
    util::GenericListNode<PriorityQueueValue> values;
    uint64_t key;
  };

  static constexpr size_t kPriorityQueuePoolElementSize =
      std::max(sizeof(PriorityQueueValue), sizeof(PriorityQueueHashEntry));

  static_assert(kPriorityQueuePoolElementSize < 64);

  using PriorityQueueHashHeader = util::GenericListNode<PriorityQueueHashEntry>;
  struct PriorityQueueHeapEntry {
    uint64_t key;
    PriorityQueueHashEntry *ent;
  };

  static unsigned int Hash(uint64_t key) { return key >> 8; }

 private:
  // This is not a normal priority queue because lots of priorities are
  // duplicates! Therefore, we use a hashtable to deduplicate them.
  struct PriorityQueue {
    PriorityQueueHeapEntry *q; // Heap
    PriorityQueueHashHeader *ht; // Hashtable. First item is a sentinel
    struct {
      PromiseRoutineWithInput *q;
      std::atomic_ulong start;
      std::atomic_ulong end;
    } pending; // Pending inserts into the heap and the hashtable
    mem::BasicPool pool;
    size_t len;
  };

  struct ZeroQueue {
    PromiseRoutineWithInput *q;
    std::atomic_ulong end;
    size_t start;
  };

  struct State {
    PromiseRoutineWithInput current;
    CompleteCounter complete_counter;
    std::atomic_bool running;

    State() : current({nullptr, VarStr()}), running(false) {}
  };

  struct Queue {
    PriorityQueue pq;
    ZeroQueue zq;
    util::SpinLock lock;
    State state;
  };

  static const size_t kMaxItem;
  static const size_t kHashTableSize;
  static constexpr size_t kMaxNrThreads = NodeConfiguration::kMaxNrThreads;

  std::array<Queue *, kMaxNrThreads> queues;
  std::atomic_ulong tot_bubbles;

 private:
  bool AddToPriorityQueue(PriorityQueue &q, PromiseRoutineWithInput &r);
  void ProcessPending(PriorityQueue &q);

 public:
  void Add(int core_id, PromiseRoutineWithInput *routines, size_t nr_routines) final override;
  void AddBubble() final override;
  bool Peek(int core_id, DispatchPeekListener &should_pop) final override;
  bool Preempt(int core_id, bool force) final override;
  void Reset() final override;
  void Complete(int core_id) final override;
  void PrintInfo() final override;
  bool IsRunning(int core_id) final override {
    return queues[core_id]->state.running.load(std::memory_order_acquire);
  }
};

// We use thread-local brks to reduce the memory allocation cost for all
// promises within an epoch. After an epoch is done, we can reclaim all of them.
class EpochPromiseAllocationService : public PromiseAllocationService {
  template <typename T> friend T &util::Instance() noexcept;
  EpochPromiseAllocationService();

  mem::Brk *brks[NodeConfiguration::kMaxNrThreads + 1];
  mem::Brk *minibrks[NodeConfiguration::kMaxNrThreads + 1]; // for mini objects
 public:
  void *Alloc(size_t size) final override;
  void Reset() final override;
};

}

namespace util {

template <> struct InstanceInit<felis::EpochManager> {
  static constexpr bool kHasInstance = true;
  static felis::EpochManager *instance;
  InstanceInit();
};

} // namespace util

#endif /* EPOCH_H */
