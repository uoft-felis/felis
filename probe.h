// -*- mode: c++ -*-
#ifndef PROBE_H
#define PROBE_H

#include <mutex>
#include <set>

template <typename T, typename ...Args> void RunProbe(Args... args) {}

#define DTRACE_PROBE0(ns, klass) RunProbe<ns::klass>();
#define DTRACE_PROBE1(ns, klass, a1) RunProbe<ns::klass>(a1);
#define DTRACE_PROBE2(ns, klass, a1, a2) RunProbe<ns::klass>(a1, a2);
#define DTRACE_PROBE3(ns, klass, a1, a2, a3) RunProbe<ns::klass>(a1, a2, a3);

#define DEFINE_PROBE(ns, klass) namespace ns { struct klass{}; }

#define PROBE(ns, klass, alist)                 \
  template <> void RunProbe<ns::klass>(alist)   \

DEFINE_PROBE(general, process_exit);

namespace agg {

// aggregations

template <typename Impl>
class Agg : public Impl {
 public:
  struct Value : public Impl {
    Agg *parent;
    Value(Agg &agg) : parent(&agg) {
      parent->Add(this);
    }
    ~Value() {
      parent->Remove(this);
    }
  };

  void Add(Value *node) {
    std::lock_guard<std::mutex> _(m);
    values.insert(node);
  }

  void Remove(Value *node) {
    std::lock_guard<std::mutex> _(m);
    values.erase(node);
    *this << *node;
  }

 protected:
  std::set<Value *> values;
  std::mutex m;
};

struct Sum {
  long sum = 0;
  Sum &operator<<(long value) {
    sum += value;
    return *this;
  }
  Sum &operator<<(const Sum &rhs) {
    sum += rhs.sum;
    return *this;
  }
};

}


#endif /* PROBE_H */
