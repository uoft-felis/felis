#ifndef PROMISE_H
#define PROMISE_H

#include <type_traits>
#include <vector>
#include <atomic>
#include <experimental/optional>
#include "sqltypes.h"

namespace util {

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

struct PromiseRoutinePool {
  std::atomic<uint64_t> refcnt;
  uint8_t *input_ptr;
  size_t mem_size;
  uint8_t mem[];

  bool IsManaging(const uint8_t *ptr) const {
    return (ptr >= mem && ptr < mem + mem_size);
  }
};

struct PromiseRoutine {
  VarStr input;
  uint8_t *capture_data;
  uint32_t capture_len;
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

class BasePromise {
 protected:
  friend struct PromiseRoutine;
  std::atomic<long> refcnt;
  std::vector<PromiseRoutine *> handlers;
 public:
  static int gNodeId;
  BasePromise() : refcnt(1) {}
  void Complete(const VarStr &in);

  BasePromise *Ref() {
    refcnt.fetch_add(1);
    return this;
  }
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

  template <typename Func, typename Closure>
  struct Next {
    typedef typename std::result_of<Func (const Closure &, T)>::type OptType;
    typedef typename OptType::value_type Type;
  };

  template <typename Func, typename Closure>
  PromiseRoutine *AttachRoutine(const Closure &capture, Func func) {
    auto static_func = [](PromiseRoutine *routine) {
      auto native_func = (typename Next<Func, Closure>::OptType (*)(const Closure &, T)) routine->callback_native_func;
      Closure *capture = (Closure *) routine->capture_data;
      T t;
      t.Decode(&routine->input);

      auto output = native_func(*capture, t);
      if (routine->next && output) {
        void *buffer = alloca(output->EncodeSize() + sizeof(VarStr) + 1);
        VarStr *output_str = output->EncodeFromAlloca(buffer);
        routine->next->Complete(*output_str);
      }
      routine->UnRef();
    };
    auto routine = PromiseRoutine::CreateWithDedicatePool(sizeof(Closure));
    routine->input.data = nullptr;
    routine->callback = (void (*)(PromiseRoutine *)) static_func;
    routine->callback_native_func = (void *) (typename Next<Func, Closure>::OptType (*)(const Closure &, T)) func;
    memcpy(routine->capture_data, &capture, sizeof(Closure));
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
    auto static_func = [](PromiseRoutine *routine) -> int {
      auto native_func = (int (*)(const Closure &, T)) routine->placement_native_func;
      Closure *capture = (Closure *) routine->capture_data;
      T t;
      t.Decode(&routine->input);
      return native_func(*capture, t);
    };
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

class PromiseProc {
  Promise<DummyValue> *p = new Promise<DummyValue>();
 public:
  PromiseProc() {}
  PromiseProc(const PromiseProc &rhs) = delete;
  PromiseProc(PromiseProc &&rhs) = delete;

  ~PromiseProc() {
    p->Complete(VarStr());
  }

  Promise<DummyValue> *operator->() const {
    return p;
  }
};

struct CombinerState {
  std::atomic<long> finished_states;
  std::vector<VarStr> states;
};

template <typename T, typename ...Types>
class Combine : public Combine<Types...> {
  Promise<T> *p;
  Promise<Tuple<T, Types...>> *next_promise;
 public:
  Combine(Promise<T> *p, Promise<Types>*... rest) : p(p), Combine<Types...>(rest...) {}
};

}

#endif /* PROMISE_H */
