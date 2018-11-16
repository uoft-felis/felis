#ifndef PROMISE_H
#define PROMISE_H

#include <type_traits>
#include <vector>
#include <tuple>
#include <atomic>
#include <experimental/optional>
#include "sqltypes.h"

namespace felis {

using sql::VarStr;

template <typename... Args>
auto T(Args&&... args) -> decltype(std::make_tuple(std::forward<Args>(args)...)) {
  return std::make_tuple(std::forward<Args>(args)...);
}


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

struct PromiseRoutinePool {
  std::atomic<uint64_t> refcnt;
  uint8_t *input_ptr;
  size_t mem_size;
  uint8_t mem[];

  bool IsManaging(const uint8_t *ptr) const {
    return (ptr >= mem && ptr < mem + mem_size);
  }

  static PromiseRoutinePool *Create(size_t size);
};


struct PromiseRoutine {
  VarStr input;
  uint32_t capture_len;
  uint8_t *capture_data;
  int node_id; // -1 if dynamic placement

  void (*callback)(PromiseRoutine *);
  int (*placement)(PromiseRoutine *);

  void *callback_native_func;
  void *placement_native_func;

  size_t NodeSize() const;
  size_t TreeSize() const;

  uint8_t *EncodeNode(uint8_t *p);
  uint8_t *DecodeNode(uint8_t *p, PromiseRoutinePool *pool);
  void EncodeTree(uint8_t *p);
  void DecodeTree(PromiseRoutinePool *pool);

  BasePromise *next;
  PromiseRoutinePool *pool;

  void Ref() {
    pool->refcnt.fetch_add(1);
  }

  void UnRef();
  void UnRefRecursively();

  static PromiseRoutine *CreateWithDedicatePool(size_t capture_len);
  static PromiseRoutine *CreateFromBufferedPool(PromiseRoutinePool *pool);
};

class PromiseRoutineTransportService {
 public:
  virtual void TransportPromiseRoutine(PromiseRoutine *routine) = 0;
};

class BasePromise {
 protected:
  friend struct PromiseRoutine;
  std::atomic<long> refcnt;
  std::vector<PromiseRoutine *> handlers;
 public:
  static size_t kNrThreads;

  BasePromise() : refcnt(1) {}
  void Complete(const VarStr &in);

  BasePromise *Ref() {
    refcnt.fetch_add(1);
    return this;
  }

  long UnRef() {
    long r = refcnt.fetch_sub(1) - 1;
    if (r == 0) {
      // delete this;
    }
    return r;
  }

  static void InitializeSourceCount(int nr_sources, size_t nr_threads);
  static void QueueRoutine(PromiseRoutine *r, int source_idx, int thread);
};

template <typename ValueType> using Optional = std::experimental::optional<ValueType>;

inline constexpr auto nullopt = std::experimental::nullopt;

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
    auto static_func = [](PromiseRoutine *routine) {
      auto native_func = (typename Next<Func, Closure>::OptType (*)(const Closure &, T)) routine->callback_native_func;
      Closure capture;
      T t;
      capture.DecodeFrom(routine->capture_data);
      t.Decode(&routine->input);

      auto output = native_func(capture, t);
      if (routine->next && output) {
        void *buffer = alloca(output->EncodeSize() + sizeof(VarStr) + 1);
        VarStr *output_str = output->EncodeFromAlloca(buffer);
        routine->next->Complete(*output_str);
      }
      routine->UnRef();
    };
    auto routine = PromiseRoutine::CreateWithDedicatePool(capture.EncodeSize());
    routine->input.data = nullptr;
    routine->callback = (void (*)(PromiseRoutine *)) static_func;
    routine->callback_native_func = (void *) (typename Next<Func, Closure>::OptType (*)(const Closure &, T)) func;

    routine->capture_len = capture.EncodeSize();
    capture.EncodeTo(routine->capture_data);

    handlers.push_back(routine);
    return routine;
  }

  // Static promise routine func should return a std::optional<T>. If the func
  // returns nothing, then it should return std::optional<VoidValue>.
  template <typename Func, typename Closure>
  Promise<typename Next<Func, Closure>::Type> *Then(const Closure &capture, int placement, Func func) {
    auto routine = AttachRoutine(capture, func);
    routine->node_id = placement;
    auto next_promise = new Promise<typename Next<Func, Closure>::Type>();
    routine->next = next_promise;
    return next_promise;
  }

  template <typename PlacementFunc, typename Func, typename Closure>
  Promise<typename Next<Func, Closure>::Type> *Then(const Closure &capture, PlacementFunc on, Func func) {
    auto routine = AttachRoutine(capture, func);
    routine->node_id = -1;
    routine->placement_native_func = on;
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
  typename ChainType<Tuple>::Type operator>>(Tuple t) {
    return typename ChainType<Tuple>::Type(p->Then(std::get<0>(t), std::get<1>(t), std::get<2>(t)));
  }
};

class PromiseProc : public PromiseStream<DummyValue> {
 public:
  PromiseProc() : PromiseStream<DummyValue>(new Promise<DummyValue>()) {}
  PromiseProc(const PromiseProc &rhs) = delete;
  PromiseProc(PromiseProc &&rhs) = delete;
  ~PromiseProc() {
    p->Complete(VarStr());
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
