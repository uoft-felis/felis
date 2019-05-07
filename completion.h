#ifndef _COMPLETION_H
#define _COMPLETION_H

#include <atomic>

namespace felis {

template <typename T>
class CompletionObject {
  std::atomic_ulong comp_count;
  T callback;
 public:
  CompletionObject(unsigned long count, T callback)
      : comp_count(count), callback(callback) {}

  void Complete(unsigned long dec = 1) {
    if (comp_count.fetch_sub(dec) == dec) {
      callback();
    }
  }

  void operator()() {
    Complete();
  }

  void Increment(unsigned long inc) {
    comp_count.fetch_add(inc);
  }

  unsigned long left_over() const {
    return comp_count.load();
  }
};


}

#endif
