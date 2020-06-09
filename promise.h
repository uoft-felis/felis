#ifndef PROMISE_H
#define PROMISE_H

#include <optional>
#include "base_promise.h"
#include "util/objects.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

namespace felis {

template <typename ValueType> using Optional = std::optional<ValueType>;

inline constexpr auto nullopt = std::nullopt;

struct VoidValue {
 private:
  VoidValue() {} // private because you shall never use a Optional<VoidValue> than nullopt_t!
 public:
  void Decode(const VarStr *) {}
  VarStr *EncodeFromPtr(void *buffer) const { return nullptr; }
  size_t EncodeSize() const { return 0; }
};

struct DummyValue {
  void Decode(const VarStr *) {}
  VarStr *EncodeFromPtr(void *buffer) const { return nullptr; }
  size_t EncodeSize() const { return 0; }
};

template <typename T>
class Promise : public BasePromise {
 public:
  typedef T Type;

  Promise(PromiseRoutine *routine = nullptr) : BasePromise() {
    if (routine) routine->next = this;
  }

  template <typename Func, typename Closure>
  struct Next {
    typedef typename std::result_of<Func (const Closure &, T)>::type OptType;
    typedef typename OptType::value_type Type;
  };

  template <typename Func, typename Closure>
  PromiseRoutine *AttachRoutine(const Closure &capture, Func func,
                                uint64_t affinity = std::numeric_limits<uint64_t>::max()) {
    // C++17 allows converting from a non-capture lambda to a constexpr function pointer! Cool!
    constexpr typename Next<Func, Closure>::OptType (*native_func)(const Closure &, T) = func;

    auto static_func =
        [](PromiseRoutine *routine, VarStr input) {
          Closure capture;
          T t;
          capture.DecodeFrom(routine->capture_data);
          t.Decode(&input);

          auto next = routine->next;
          auto output = native_func(capture, t);
          if (next && next->nr_routines() > 0) {
            auto &transport = util::Impl<PromiseRoutineTransportService>();
            auto l = routine->level + 1;

            if (output) {
              void *buffer = Alloc(output->EncodeSize() + sizeof(VarStr) + 1);
              VarStr *output_str = output->EncodeFromPtr(buffer);
              next->Complete(*output_str);
            } else {
              VarStr bubble(0, 0, (uint8_t *)PromiseRoutine::kBubblePointer);
              next->Complete(bubble);
            }

            transport.FinishCompletion(l);
          }
        };
    auto routine = PromiseRoutine::CreateFromCapture(capture.EncodeSize());
    routine->callback = static_func;
    routine->affinity = affinity;
    capture.EncodeTo(routine->capture_data);

    Add(routine);
    return routine;
  }

  // Static promise routine func should return a std::optional<T>. If the func
  // returns nothing, then it should return std::optional<VoidValue>.
  template <typename Func, typename Closure>
  Promise<typename Next<Func, Closure>::Type> *Then(
      const Closure &capture, int placement, Func func,
      uint64_t affinity = std::numeric_limits<uint64_t>::max()) {
    auto routine = AttachRoutine(capture, func, affinity);
    routine->node_id = placement;
    return new Promise<typename Next<Func, Closure>::Type>(routine);
  }

  template <typename Func, typename Closure, typename PlacementFunc>
  Promise<typename Next<Func, Closure>::Type> *ThenDynamic(
      const Closure &capture, PlacementFunc placement_func, Func func,
      uint64_t affinity = std::numeric_limits<uint64_t>::max()) {
    constexpr uint8_t (*native_placement_func)(const Closure&, T) = placement_func;
    auto static_func =
        [](PromiseRoutine *routine, VarStr input) -> uint8_t {
          Closure capture;
          T t;
          capture.DecodeFrom(routine->capture_data);
          t.Decode(&input);

          return native_placement_func(capture, t);
        };

    auto routine = AttachRoutine(capture, func, affinity);
    routine->node_id = 255;
    routine->node_func = static_func;
    return new Promise<typename Next<Func, Closure>::Type>(routine);
  }

};

template <>
class Promise<VoidValue> : public BasePromise {
 public:

  Promise(PromiseRoutine *routine = nullptr) {
    std::abort();
  }
  static void *operator new(size_t size) noexcept {
    return nullptr;
  }
};

}

#endif /* PROMISE_H */
