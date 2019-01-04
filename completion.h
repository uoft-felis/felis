#ifndef _COMPLETION_H
#define _COMPLETION_H

#include <atomic>
#include <functional>

namespace felis {

template <typename T>
class CompletionObject {
  std::atomic_ulong comp_count;
  T callback;
 public:
  CompletionObject(ulong count, T callback)
      : comp_count(count), callback(callback) {}

  void Complete(ulong dec = 1) {
    if (comp_count.fetch_sub(dec) == dec) {
      callback();
    }
  }

  void operator()() {
    Complete();
  }

  void Increment(ulong inc) {
    comp_count.fetch_add(inc);
  }

  ulong left_over() const {
    return comp_count.load();
  }
};


}

#endif
