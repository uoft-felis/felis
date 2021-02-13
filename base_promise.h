#ifndef BASE_PROMISE_H
#define BASE_PROMISE_H

#include <tuple>
#include <atomic>

#include "gopp/gopp.h"
#include "varstr.h"

namespace felis {

// TODO: I need to update this doc.

// Distributed Promise system. We can use promise to define the intra
// transaction dependencies. The dependencies have to be acyclic.

// A promise handler can take *one* input and *one* output. It can also migrate
// dyanmically according to the input and its capture data before it starts.
//
// Since each handler can only take one input, if we need a subroutine that
// takes multiple inputs from many promises, we need to handle it using static
// routine. A special property of a collector routine is that it cannot
// dynamically migrates, otherwise we will encounter race conditions between
// nodes.

class BasePromise;
class PromiseRoutine;

using PromiseRoutineWithInput = std::tuple<PromiseRoutine *, VarStr>;
// Performance: It seems critical to keep this struct one cache line!
struct PromiseRoutine {
  uint8_t *capture_data;
  uint32_t capture_len;
  uint8_t level;
  uint8_t node_id;
  uint64_t sched_key; // Optional. 0 for unset. For scheduling only.
  uint64_t affinity; // Which core to run on. -1 means not specified. >= nr_threads means random.

  void (*callback)(PromiseRoutine *, VarStr input);
  uint8_t (*node_func)(PromiseRoutine *, VarStr input);

  size_t NodeSize() const;
  uint8_t *EncodeNode(uint8_t *p);

  size_t DecodeNode(uint8_t *p, size_t len);

  size_t TreeSize(const VarStr &input) const;
  void EncodeTree(uint8_t *p, const VarStr &input);

  static constexpr long kBubblePointer = 0xdeadbeef;

  BasePromise *next;
  uint8_t __padding__[8];

  static PromiseRoutine *CreateFromCapture(size_t capture_len);
  static PromiseRoutineWithInput CreateFromPacket(uint8_t *p, size_t packet_len);

  static constexpr size_t kUpdateBatchCounter = std::numeric_limits<uint64_t>::max() - (1ULL << 56);
  static constexpr size_t kBubble = std::numeric_limits<uint64_t>::max() - (1ULL << 56) - 0x00FFFFFF;
};

static_assert(sizeof(PromiseRoutine) == CACHE_LINE_SIZE);

class EpochClient;

class BasePromise {
 public:
  static constexpr size_t kInlineLimit = 6;
 protected:
  friend struct PromiseRoutine;
  int nr_handlers;
  int limit;
  PromiseRoutine **extra_handlers;

  PromiseRoutine *inline_handlers[kInlineLimit];
 public:
  static size_t g_nr_threads;
  static constexpr int kMaxHandlersLimit = 32 + kInlineLimit;

  class ExecutionRoutine : public go::Routine {
    friend class PromiseRoutineLookupService;
   public:
    ExecutionRoutine() {
      set_reuse(true);
    }
    static void *operator new(std::size_t size) {
      return BasePromise::Alloc(util::Align(size, CACHE_LINE_SIZE));
    }
    static void operator delete(void *ptr) {}
    void Run() final override;
    void AddToReadyQueue(go::Scheduler::Queue *q, bool next_ready = false) final override;

    bool Preempt();
   private:
    void RunPromiseRoutine(PromiseRoutine *r, const VarStr &in);
  };

  static_assert(sizeof(ExecutionRoutine) <= CACHE_LINE_SIZE);

  BasePromise(int limit = kMaxHandlersLimit);
  void Complete(const VarStr &in);
  void Add(PromiseRoutine *child);
  void AssignSchedulingKey(uint64_t key);
  void AssignAffinity(uint64_t affinity);

  static void *operator new(std::size_t size);
  static void *Alloc(size_t size);
  static void QueueRoutine(PromiseRoutineWithInput *routines, size_t nr_routines, int core_id);
  static void FlushScheduler();

  size_t nr_routines() const { return nr_handlers; }
  PromiseRoutine *&routine(int idx) {
    if (idx < kInlineLimit)
      return inline_handlers[idx];
    else
      return extra_handlers[idx - kInlineLimit];
  }
  PromiseRoutine *&last() {
    return routine(nr_routines() - 1);
  }
};

static_assert(sizeof(BasePromise) % CACHE_LINE_SIZE == 0, "BasePromise is not cache line aligned");

class PromiseRoutineTransportService {
 public:
  static constexpr size_t kPromiseMaxLevels = 16;

  virtual void TransportPromiseRoutine(PromiseRoutine *routine, const VarStr &input) = 0;
  virtual bool PeriodicIO(int core) { return false; }
  virtual void PrefetchInbound() {};
  virtual void FinishCompletion(int level) {}
  virtual uint8_t GetNumberOfNodes() { return 0; }
};

class PromiseRoutineDispatchService {
 public:

  class DispatchPeekListener {
   public:
    virtual bool operator()(PromiseRoutineWithInput, BasePromise::ExecutionRoutine *) = 0;
  };

  template <typename F>
  class GenericDispatchPeekListener : public DispatchPeekListener {
    F func;
   public:
    GenericDispatchPeekListener(F f) : func(f) {}
    bool operator()(PromiseRoutineWithInput r, BasePromise::ExecutionRoutine *state) final override {
      return func(r, state);
    }
  };

  virtual void Add(int core_id, PromiseRoutineWithInput *r, size_t nr_routines) = 0;
  virtual void AddBubble() = 0;
  virtual bool Preempt(int core_id, BasePromise::ExecutionRoutine *state) = 0;
  virtual bool Peek(int core_id, DispatchPeekListener &should_pop) = 0;
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
