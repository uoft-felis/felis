#ifndef PROMISE_H
#define PROMISE_H

#include <type_traits>
#include <vector>
#include <optional>
#include "sqltypes.h"

namespace util {

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

struct PromiseRoutine {
  VarStr input;
  uint8_t *capture_data;
  uint32_t capture_len;
  int node_id; // -1 if dynamic placement

  void (*callback)(PromiseRoutine *);
  int (*placement)(PromiseRoutine *);

  void *callback_native_func;
  void *placement_native_func;

  void Prepare(const VarStr &in) {
    input = input;
    if (node_id == -1) {
      node_id = placement(this);
    }
    // Migrate or create local go::Routine to run.
    //
    // In this process, input *must be* copied, next pointer will be serialized if
    // migration is happening

    // TODO:
  }

  BasePromise *next;
};

class BasePromise {
 protected:
  std::vector<PromiseRoutine *> handlers;
 public:
  void Complete(const VarStr &in) {
    for (auto p: handlers) {
      p->Prepare(in);
    }
  }
};

template <typename ValueType> using Optional = std::optional<ValueType>;
class VoidValue {
  VoidValue() {} // private because you shall never use a Optional<VoidValue> than nullopt_t!
};

template <typename T>
class Promise : public BasePromise {
 public:
  template <typename Func, typename Closure>
  PromiseRoutine *AttachRoutine(const Closure &capture, Func func) {
    auto static_func = [](PromiseRoutine *routine) {
      auto native_func = (void (*)(const Closure &, T)) data->callback_native_func;
      Closure *capture = routine->capture_data;
      T t;
      t.Decode(input);

      // TODO: check the optional return value
      auto output = native_func(*capture, t);
      if (routine->next && output) {
        void *buffer = alloca(output->EncodeSize() + sizeof(VarStr) + 1);
        VarStr *output_str = output->EncodeFromAlloca(buffer);
        routine->next->Complete(*output_str);
      }
    };
    auto routine = new PromiseRoutine();
    memset(routine, 0, sizeof(PromiseRoutine));
    routine->callback = (void (*)(PromiseRoutine *)) static_func;
    routine->callback_native_func = (void (*)(const ClosureType &, T)) func;
    routine->capture_len = sizeof(Closure);
    routine->capture_data = new Closure(capture);
    return routine;
  }

  // Static promise routine func should return a std::optional<T>. If the func
  // returns nothing, then it should return std::optional<VoidValue>.
  template <typename Func, typename Closure>
  Promise<typename std::result_of<Func (const ClosureType &, T)>::type::value_type> *Then(
      const Closure &capture, Func func, int placement) {
    auto routine = AttachRoutine(capture, func);
    routine->node_id = placement;
    auto next_promise = new Promise<typename std::result_of<Func (const ClosureType &, T)>::type::value_type>();
    routine->next = next_promise;
    return next_promise;
  }

  // TODO: Dynamic routine support? This can be done using the placement callbacks.
};

template <>
class Promise<VoidValue> : public BasePromise {
  Promise() {} // shouldn't happen either
 public:
  static void *operator new(size_t size) {
    return nullptr;
  }
}

}

#endif /* PROMISE_H */
