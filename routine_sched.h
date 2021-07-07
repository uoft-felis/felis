#ifndef ROUTINE_SCHED_H
#define ROUTINE_SCHED_H

#include "piece.h"
#include "util/objects.h"
#include "util/linklist.h"
#include "node_config.h"

namespace felis {

struct PriorityQueueValue : public util::GenericListNode<PriorityQueueValue> {
  PieceRoutine *routine;
  BasePieceCollection::ExecutionRoutine *state;
};

struct WaitState {
  BasePieceCollection::ExecutionRoutine *state;
  uint64_t sched_key;
  uint64_t preempt_ts;
};

struct PriorityQueueHashEntry : public util::GenericListNode<PriorityQueueHashEntry> {
  util::GenericListNode<PriorityQueueValue> values;
  uint64_t key;
};

static constexpr size_t kPriorityQueuePoolElementSize =
    std::max(sizeof(PriorityQueueValue), sizeof(PriorityQueueHashEntry));

static_assert(kPriorityQueuePoolElementSize < 64);

// Interface for priority scheduling
class PrioritySchedulingPolicy {
 friend class EpochExecutionDispatchService;
 protected:
  size_t len = 0;
 public:
  virtual ~PrioritySchedulingPolicy() {}

  bool empty() { return len == 0; }

  // Before we try to schedule from this scheduling policy, should we double
  // check the zero queue?
  virtual bool ShouldRetryBeforePick(std::atomic_ulong *zq_start, std::atomic_ulong *zq_end,
                                     std::atomic_uint *pq_start, std::atomic_uint *pq_end) {
    return false;
  }
  // Would you pick this key according to the current situation?
  virtual bool ShouldPickWaiting(const WaitState &ws) = 0;
  // Pick a value without consuming it.
  virtual PriorityQueueValue *Pick() = 0;
  // Consume (delete) the value from the scheduling structure.
  virtual void Consume(PriorityQueueValue *value) = 0;

  virtual void IngestPending(PriorityQueueHashEntry *hent, PriorityQueueValue *value) = 0;

  virtual void Reset() {}
};

// For scheduling transactions during execution
class EpochExecutionDispatchService : public PromiseRoutineDispatchService {
  template <typename T> friend T &util::Instance() noexcept;
  EpochExecutionDispatchService();

  using ExecutionRoutine = BasePieceCollection::ExecutionRoutine;

  struct CompleteCounter {
    ulong completed;
    CompleteCounter() : completed(0) {}
  };
 public:
  static unsigned int Hash(uint64_t key) { return key >> 8; }
  static constexpr int kOutOfOrderWindow = 31;

  using PriorityQueueHashHeader = util::GenericListNode<PriorityQueueHashEntry>;
 private:
  // This is not a normal priority queue because lots of priorities are
  // duplicates! Therefore, we use a hashtable to deduplicate them.
  struct PriorityQueue {
    PrioritySchedulingPolicy *sched_pol;
    PriorityQueueHashHeader *ht; // Hashtable. First item is a sentinel
    struct {
      PieceRoutine **q;
      std::atomic_uint start;
      std::atomic_uint end;
    } pending; // Pending inserts into the heap and the hashtable

    struct {
      // Ring-buffer.
      WaitState states[kOutOfOrderWindow];
      uint32_t off;
      uint32_t len;
    } waiting;

    mem::Brk brk; // memory allocator for hashtables entries and queue values
  };

  struct ZeroQueue {
    PieceRoutine **q;
    std::atomic_ulong end;
    std::atomic_ulong start;
  };

  // q for priority txns
  struct TxnQueue {
    PriorityTxn* q;
    std::atomic_ulong end;
    std::atomic_ulong start;
  };

  struct State {
    uint64_t current_sched_key;
    uint64_t ts;
    CompleteCounter complete_counter;

    static constexpr int kSleeping = 0;
    static constexpr int kRunning = 1;
    static constexpr int kDeciding = -1;
    std::atomic_int running;

    State() : current_sched_key(0), ts(0), running(kSleeping) {}
  };

  struct Queue {
    PriorityQueue pq;
    ZeroQueue zq;
    TxnQueue tq;
    util::SpinLock lock;
    State state;
  };
 public:
  static size_t g_max_item;
 private:

  static const size_t kHashTableSize;
  static constexpr size_t kMaxNrThreads = NodeConfiguration::kMaxNrThreads;

  std::array<Queue *, kMaxNrThreads> queues;
  std::atomic_ulong tot_bubbles;

 private:
  void AddToPriorityQueue(PriorityQueue &q, PieceRoutine *&r,
                          BasePieceCollection::ExecutionRoutine *state = nullptr);
  void ProcessPending(PriorityQueue &q);

 public:
  void Add(int core_id, PieceRoutine **routines, size_t nr_routines) final override;
  void Add(int core_id, PriorityTxn *txn) final override;
  void AddBubble() final override;
  bool Peek(int core_id, DispatchPeekListener &should_pop) final override;
  bool Peek(int core_id, PriorityTxn *&txn, bool dry_run = false) final override;
  bool Preempt(int core_id, BasePieceCollection::ExecutionRoutine *state) final override;
  void Reset() final override;
  void Complete(int core_id) final override;
  int TraceDependency(uint64_t key) final override;
  bool IsRunning(int core_id) final override {
    auto &s = queues[core_id]->state;
    int running = -1;
    while ((running = s.running.load(std::memory_order_seq_cst)) == State::kDeciding)
      _mm_pause();
    return running == State::kRunning;
  }
  bool IsReady(int core_id) final override;
};

}

#endif /* SCHED_H */
