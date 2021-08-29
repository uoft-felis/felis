#ifndef PIECE_H
#define PIECE_H

#include <tuple>
#include <atomic>

#include "gopp/gopp.h"
#include "varstr.h"

namespace felis {

class BasePieceCollection;
class PieceRoutine;

// Performance: It seems critical to keep this struct one cache line!
struct PieceRoutine {
  uint8_t *capture_data;
  uint32_t capture_len;
  uint8_t level;
  uint8_t node_id;
  bool is_priority; // is it a piece from priority transaction
  uint64_t sched_key; // Optional. 0 for unset. For scheduling only.
  uint64_t affinity; // Which core to run on. -1 means not specified. >= nr_threads means random.

  void (*callback)(PieceRoutine *);

  size_t NodeSize() const;
  uint8_t *EncodeNode(uint8_t *p);

  size_t DecodeNode(uint8_t *p, size_t len);

  BasePieceCollection *next;
  uint8_t __padding__[16];

  static PieceRoutine *CreateFromCapture(size_t capture_len);
  static PieceRoutine *CreateFromPacket(uint8_t *p, size_t packet_len);

  static constexpr size_t kUpdateBatchCounter = std::numeric_limits<uint64_t>::max() - (1ULL << 56);
};

static_assert(sizeof(PieceRoutine) == CACHE_LINE_SIZE);

class EpochClient;

class BasePieceCollection {
 public:
  static constexpr size_t kInlineLimit = 6;
 protected:
  friend struct PieceRoutine;
  int nr_handlers;
  int limit;
  PieceRoutine **extra_handlers;

  PieceRoutine *inline_handlers[kInlineLimit];
 public:
  static size_t g_nr_threads;
  static constexpr int kMaxHandlersLimit = 32 + kInlineLimit;

  class ExecutionRoutine : public go::Routine {
   public:
    ExecutionRoutine() {
      set_reuse(true);
    }
    static void *operator new(std::size_t size) {
      return BasePieceCollection::Alloc(util::Align(size, CACHE_LINE_SIZE));
    }
    static void operator delete(void *ptr) {}
    void Run() final override;
    void AddToReadyQueue(go::Scheduler::Queue *q, bool next_ready = false) final override;

    bool Preempt();
  };

  static_assert(sizeof(ExecutionRoutine) <= CACHE_LINE_SIZE);

  BasePieceCollection(int limit = kMaxHandlersLimit);
  void Complete();
  void Add(PieceRoutine *child);
  void AssignSchedulingKey(uint64_t key);
  void AssignAffinity(uint64_t affinity);

  static void *operator new(std::size_t size);
  static void *Alloc(size_t size);
  static void QueueRoutine(PieceRoutine **routines, size_t nr_routines, int core_id);
  static void FlushScheduler();

  size_t nr_routines() const { return nr_handlers; }
  PieceRoutine *&routine(int idx) {
    if (idx < kInlineLimit)
      return inline_handlers[idx];
    else
      return extra_handlers[idx - kInlineLimit];
  }
  PieceRoutine *&last() {
    return routine(nr_routines() - 1);
  }
};

static_assert(sizeof(BasePieceCollection) % CACHE_LINE_SIZE == 0, "BasePromise is not cache line aligned");

class PromiseRoutineTransportService {
 public:
  static constexpr size_t kPromiseMaxLevels = 16;

  virtual void TransportPromiseRoutine(PieceRoutine *routine) = 0;
  virtual bool PeriodicIO(int core) { return false; }
  virtual void PrefetchInbound() {};
  virtual void FinishCompletion(int level) {}
  virtual uint8_t GetNumberOfNodes() { return 0; }
};

class PriorityTxn;
class PromiseRoutineDispatchService {
 public:

  class DispatchPeekListener {
   public:
    virtual bool operator()(PieceRoutine *, BasePieceCollection::ExecutionRoutine *) = 0;
  };

  template <typename F>
  class GenericDispatchPeekListener : public DispatchPeekListener {
    F func;
   public:
    GenericDispatchPeekListener(F f) : func(f) {}
    bool operator()(PieceRoutine *r, BasePieceCollection::ExecutionRoutine *state) final override {
      return func(r, state);
    }
  };

  virtual void Add(int core_id, PieceRoutine **r, size_t nr_routines, bool from_pri = false) = 0;
  virtual void Add(int core_id, PriorityTxn *t) = 0;
  virtual void AddBubble() = 0;
  virtual bool Preempt(int core_id, BasePieceCollection::ExecutionRoutine *state) = 0;
  virtual bool Peek(int core_id, DispatchPeekListener &should_pop) = 0;
  virtual bool Peek(int core_id, PriorityTxn *&txn, bool dry_run = false) = 0;
  virtual void Reset() = 0;
  virtual void Complete(int core_id) = 0;
  virtual bool IsRunning(int core_id) = 0;
  virtual bool IsReady(int core_id) = 0;

  // For debugging
  virtual int TraceDependency(uint64_t) { return -1; }
};

class PromiseAllocationService {
 public:
  virtual void *Alloc(size_t size) = 0;
  virtual void Reset() = 0;
};

}

#endif
