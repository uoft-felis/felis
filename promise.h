#ifndef PROMISE_H
#define PROMISE_H

#include <type_traits>
#include <vector>
#include <tuple>
#include <atomic>
#include <optional>
#include "sqltypes.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

namespace felis {

using sql::VarStr;

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

// Performance: It seems critical to keep this struct one cache line!
struct PromiseRoutine {
  uint8_t *capture_data;
  uint32_t capture_len;
  uint8_t level;
  uint8_t node_id;
  uint64_t sched_key; // Optional. 0 for unset. For scheduling only.

  void (*callback)(PromiseRoutine *, VarStr input);
  void *callback_native_func;

  size_t NodeSize() const;
  uint8_t *EncodeNode(uint8_t *p);

  void DecodeNode(go::TcpInputChannel *in);

  size_t TreeSize(const VarStr &input) const;
  void EncodeTree(uint8_t *p, const VarStr &input);

  static constexpr long kBubblePointer = 0xdeadbeef;

  BasePromise *next;
  uint8_t __padding__[16];

  static PromiseRoutine *CreateFromCapture(size_t capture_len);
  static std::tuple<PromiseRoutine *, VarStr> CreateFromPacket(go::TcpInputChannel *in,
                                                               size_t packet_len);

  static constexpr size_t kUpdateBatchCounter = std::numeric_limits<size_t>::max();
  static constexpr size_t kBubble = std::numeric_limits<size_t>::max() - 0x00FFFFFF;
};

static_assert(sizeof(PromiseRoutine) == CACHE_LINE_SIZE);

using PromiseRoutineWithInput = std::tuple<PromiseRoutine *, VarStr>;

class EpochClient;

class BasePromise {
 protected:
  friend struct PromiseRoutine;
  int nr_handlers;
  int limit;
  PromiseRoutine **extra_handlers;
  static constexpr size_t kInlineLimit = 6;

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

    bool Preempt(bool force);
   private:
    void RunPromiseRoutine(PromiseRoutine *r, const VarStr &in);
  };

  static_assert(sizeof(ExecutionRoutine) <= CACHE_LINE_SIZE);

  BasePromise(int limit = kMaxHandlersLimit);
  void Complete(const VarStr &in);
  void Add(PromiseRoutine *child);
  void AssignSchedulingKey(uint64_t key);

  static void *operator new(std::size_t size);
  static void *Alloc(size_t size);
  static void InitializeSourceCount(int nr_sources, size_t nr_threads);
  static void QueueRoutine(PromiseRoutineWithInput *routines, size_t nr_routines, int source_idx, int thread, bool batch = true);
  static void FlushScheduler();

  size_t nr_routines() const { return nr_handlers; }
  PromiseRoutine *&routine(int idx) {
    if (idx < kInlineLimit)
      return inline_handlers[idx];
    else
      return extra_handlers[idx - kInlineLimit];
  }
};

static_assert(sizeof(BasePromise) % CACHE_LINE_SIZE == 0, "BasePromise is not cache line aligned");

class PromiseRoutineTransportService {;
 public:
  static constexpr size_t kPromiseMaxLevels = 16;

  virtual void TransportPromiseRoutine(PromiseRoutine *routine, const VarStr &input) = 0;
  virtual void ForceFlushPromiseRoutine() {}
  virtual void PreparePromisesToQueue(int core, int level, unsigned long nr) {}
  virtual void FinishPromiseFromQueue(PromiseRoutine *routine) {}
  virtual long UrgencyCount(int core_id) { return -1; }
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
  virtual bool Preempt(int core_id, bool force) = 0;
  virtual bool Peek(int core_id, DispatchPeekListener &should_pop) = 0;
  virtual void Reset() = 0;
  virtual void Complete(int core_id) = 0;
  virtual bool IsRunning(int core_id) = 0;

  // For debugging
  virtual void PrintInfo() {};
};

class PromiseAllocationService {
 public:
  virtual void *Alloc(size_t size) = 0;
  virtual void Reset() = 0;
};

template <typename ValueType> using Optional = std::optional<ValueType>;

inline constexpr auto nullopt = std::nullopt;

struct VoidValue {
 private:
  VoidValue() {} // private because you shall never use a Optional<VoidValue> than nullopt_t!
 public:
  void Decode(const VarStr *) {}
  VarStr *EncodeFromAlloca(void *buffer) const { return nullptr; }
  size_t EncodeSize() const { return 0; }
};

struct DummyValue {
  void Decode(const VarStr *) {}
  VarStr *EncodeFromAlloca(void *buffer) const { return nullptr; }
  size_t EncodeSize() const { return 0; }
};

template <typename T>
class Promise : public BasePromise {
 public:
  typedef T Type;

  template <typename Func, typename Closure>
  struct Next {
    typedef typename std::result_of<Func (const Closure &, T)>::type OptType;
    typedef typename OptType::value_type Type;
  };

  template <typename Func, typename Closure>
  PromiseRoutine *AttachRoutine(const Closure &capture, Func func) {
    auto static_func =
        [](PromiseRoutine *routine, VarStr input) {
          auto native_func = (typename Next<Func, Closure>::OptType(*)(
              const Closure &, T))routine->callback_native_func;
          Closure capture;
          T t;
          capture.DecodeFrom(routine->capture_data);
          t.Decode(&input);

          auto next = routine->next;
          auto output = native_func(capture, t);
          if (next && next->nr_routines() > 0) {
            if (output) {
              void *buffer = Alloc(output->EncodeSize() + sizeof(VarStr) + 1);
              VarStr *output_str = output->EncodeFromAlloca(buffer);
              next->Complete(*output_str);
            } else {
              VarStr bubble(0, 0, (uint8_t *)PromiseRoutine::kBubblePointer);
              next->Complete(bubble);
            }
          }
          util::Impl<PromiseRoutineTransportService>().FinishPromiseFromQueue(routine);
        };
    auto routine = PromiseRoutine::CreateFromCapture(capture.EncodeSize());
    routine->callback = (void (*)(PromiseRoutine *, VarStr)) static_func;
    routine->callback_native_func = (void *) (typename Next<Func, Closure>::OptType (*)(const Closure &, T)) func;
    capture.EncodeTo(routine->capture_data);

    Add(routine);
    return routine;
  }

  // Static promise routine func should return a std::optional<T>. If the func
  // returns nothing, then it should return std::optional<VoidValue>.
  template <typename Func, typename Closure>
  Promise<typename Next<Func, Closure>::Type> *Then(const Closure &capture, int placement, Func func) {
    auto routine = AttachRoutine(capture, func);
    routine->node_id = placement;
    routine->level = 0;
    auto next_promise = new Promise<typename Next<Func, Closure>::Type>();
    routine->next = next_promise;
    return next_promise;
  }

};

template <>
class Promise<VoidValue> : public BasePromise {
 public:

  Promise() {
    std::abort();
  }
  static void *operator new(size_t size) noexcept {
    return nullptr;
  }
};

template <typename T>
class PromiseStream {
 protected:
  Promise<T> *p;
 public:
  PromiseStream(Promise<T> *p) : p(p) {}

  Promise<T> *operator->() const {
    return p;
  }

  Promise<T> *promise() const {
    return p;
  }

  operator Promise<T>*() const {
    return p;
  }

  template <typename Tuple>
  struct ChainType {
    using Closure = typename std::tuple_element<0, Tuple>::type;
    using Func = typename std::tuple_element<2, Tuple>::type;
    using Type = PromiseStream<typename Promise<T>::template Next<Func, Closure>::Type>;
  };

  template <typename Tuple>
  typename ChainType<Tuple>::Type operator|(Tuple t) {
    return typename ChainType<Tuple>::Type(p->Then(std::get<0>(t), std::get<1>(t), std::get<2>(t)));
  }
};

class PromiseProc : public PromiseStream<DummyValue> {
 public:
  PromiseProc() : PromiseStream<DummyValue>(nullptr) {}
  PromiseProc(const PromiseProc &rhs) = delete;
  PromiseProc(PromiseProc &&rhs) = delete;
  ~PromiseProc();

  void Reset() {
    p = new Promise<DummyValue>();
  }
};

struct BaseCombinerState {
  std::atomic<long> finished_states;
  BaseCombinerState() : finished_states(0) {}

  long Finish() { return finished_states.fetch_add(1) + 1; }
};

template <typename ...Types>
class CombinerState : public BaseCombinerState, public sql::TupleField<Types...> {
 public:
  typedef sql::Tuple<Types...> TupleType;
  typedef Promise<TupleType> PromiseType;
  static constexpr size_t kNrFields = sizeof...(Types);
};

template <typename CombinerStatePtr, typename CombinerStateType, typename T, typename ...Types>
static void CombineImplInstall(int node_id, CombinerStatePtr state_ptr,
                               typename CombinerStateType::PromiseType *next_promise, Promise<T> *p) {
  auto routine = p->AttachRoutine(
      sql::Tuple<CombinerStatePtr>(state_ptr),
      [](auto ctx, T result) -> Optional<typename CombinerStateType::TupleType> {
        // We only set the first item in the tuple because of this cast!
        CombinerStatePtr state_ptr;
        ctx.Unpack(state_ptr);

        auto *raw_state_ptr = (CombinerStateType *) state_ptr;
        sql::TupleField<T, Types...> *tuple = raw_state_ptr;
        tuple->value = result;

        if (state_ptr->Finish() < CombinerStateType::kNrFields) {
          return nullopt;
        }

        typename CombinerStateType::TupleType final_result(*raw_state_ptr);
        return final_result;
      });
  routine->node_id = node_id;
  routine->next = next_promise->Ref();
}

template <typename CombinerStatePtr, typename CombinerStateType, typename T, typename ...Types>
class CombineImpl : public CombineImpl<CombinerStatePtr, CombinerStateType, Types...> {
 public:
  void Install(int node_id, CombinerStatePtr state_ptr,
               typename CombinerStateType::PromiseType *next_promise,
               Promise<T> *p, Promise<Types>*... rest) {
    CombineImplInstall<CombinerStatePtr, CombinerStateType, T, Types...>(node_id, state_ptr, next_promise, p);
    CombineImpl<CombinerStatePtr, CombinerStateType, Types...>::Install(node_id, state_ptr, next_promise, rest...);
  }
};

template <typename CombinerStatePtr, typename CombinerStateType, typename T>
class CombineImpl<CombinerStatePtr, CombinerStateType, T> {
 public:
  void Install(int node_id, CombinerStatePtr state_ptr,
               typename CombinerStateType::PromiseType *next_promise,
               Promise<T> *p) {
    CombineImplInstall<CombinerStatePtr, CombinerStateType, T>(node_id, state_ptr, next_promise, p);
  }
};

template <typename CombinerStatePtr, typename ...Types>
class Combine {
  typedef typename CombinerState<Types...>::PromiseType PromiseType;
  CombineImpl<CombinerStatePtr, CombinerState<Types...>, Types...> impl;
  PromiseType *next_promise = new PromiseType();
 public:
  Combine(int node_id, CombinerStatePtr state_ptr, Promise<Types>*... args) {
    impl.Install(node_id, state_ptr, next_promise, args...);
    next_promise->UnRef();
  }
  PromiseType *operator->() const {
    return next_promise;
  }
  PromiseType *promise() const {
    return next_promise;
  }
  PromiseStream<typename PromiseType::Type> stream() const {
    return PromiseStream<typename PromiseType::Type>(promise());
  }
  operator PromiseType*() const {
    return next_promise;
  }
};

}

#endif /* PROMISE_H */
