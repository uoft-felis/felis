#ifndef PIECE_CC_H
#define PIECE_CC_H

#include "piece.h"

namespace felis {

class PieceCollection : public BasePieceCollection {
 public:
  using BasePieceCollection::BasePieceCollection;
  template <typename Func, typename Closure>
  PieceRoutine *AttachRoutine(const Closure &capture, int placement, Func func,
                              uint64_t affinity = std::numeric_limits<uint64_t>::max()) {
    // C++17 allows converting from a non-capture lambda to a constexpr function pointer! Cool!
    constexpr void (*native_func)(const Closure &) = func;

    auto static_func =
        [](PieceRoutine *routine) {
          Closure capture;
          capture.DecodeFrom(routine->capture_data);

          native_func(capture);
        };
    auto routine = PieceRoutine::CreateFromCapture(capture.EncodeSize());
    routine->node_id = placement;
    routine->callback = static_func;
    routine->affinity = affinity;
    capture.EncodeTo(routine->capture_data);

    Add(routine);
    return routine;
  }
};

}

#endif
