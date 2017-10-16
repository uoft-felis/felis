// -*- mode: c++ -*-
#ifndef PROBE_H
#define PROBE_H

#include <cstring>
#include <cmath>
#include <mutex>
#include <set>
#include <iostream>
#include <iomanip>
#include <limits>
#include <functional>

template <typename T, typename ...Args> void RunProbe(Args... args) {}

#define DTRACE_PROBE0(ns, klass) RunProbe<ns::klass>();
#define DTRACE_PROBE1(ns, klass, a1) RunProbe<ns::klass>(a1);
#define DTRACE_PROBE2(ns, klass, a1, a2) RunProbe<ns::klass>(a1, a2);
#define DTRACE_PROBE3(ns, klass, a1, a2, a3) RunProbe<ns::klass>(a1, a2, a3);

class ScopeProbe {
  std::function<void ()> f;
 public:
  ScopeProbe(std::function<void ()> f1, std::function<void ()> f2) : f(f2) {
    f1();
  }
  ~ScopeProbe() {
    f();
  }
};

#define DEFINE_PROBE(ns, klass) namespace ns { struct klass{}; }

#define PROBE(ns, klass, ...)                           \
  template <> void RunProbe<ns::klass>(__VA_ARGS__)     \

#define AT_EXIT()                                               \
  static struct ProbeMain { ~ProbeMain(); } __probe_main__;     \
  ProbeMain::~ProbeMain()                                       \

namespace agg {

// aggregations

#define AGG(ins) decltype(global.ins)::Value ins = global.ins

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

  Value local() {
    return Value(*this);
  }

  void Add(Value *node) {
    std::lock_guard<std::mutex> _(m);
    values.insert(node);
  }

  void Remove(Value *node) {
    std::lock_guard<std::mutex> _(m);
    values.erase(node);
    *this << *node;
  }

  Impl operator()() {
    Impl o;
    o << *this;
    std::lock_guard<std::mutex> _(m);
    for (Value *e: values) {
      o << *e;
    }
    return o;
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

static inline std::ostream &operator<<(std::ostream &out, const Sum &s)
{
  out << s.sum;
  return out;
}

struct Average {
  long sum = 0;
  long cnt = 0;
  Average &operator<<(long value) {
    sum += value;
    cnt++;
    return *this;
  }
  Average &operator<<(const Average &rhs) {
    sum += rhs.sum;
    cnt += rhs.cnt;
    return *this;
  }
};

static inline std::ostream &operator<<(std::ostream &out, const Average &avg)
{
  out << 1.0l * avg.sum / avg.cnt;
  return out;
}

template <int N = 128, int Offset = 0, int Bucket = 100>
struct Histogram {
  long hist[N];
  Histogram() {
    memset(hist, 0, sizeof(long) * N);
  }
  Histogram &operator<<(long value) {
    long idx = (value - Offset) / Bucket;
    if (idx >= 0 && idx < N) hist[idx]++;
    return *this;
  }
  Histogram &operator<<(const Histogram &rhs) {
    for (int i = 0; i < N; i++) hist[i] += rhs.hist[i];
    return *this;
  }
};

template <int N, int Offset, int Bucket>
std::ostream &operator<<(std::ostream &out, const Histogram<N, Offset, Bucket>& h)
{
  long last = std::numeric_limits<long>::max();
  bool repeat = false;
  long unit = 0;
  for (int i = 0; i < N; i++)
    if (unit < h.hist[i] / 100) unit = h.hist[i] / 100;

  for (int i = 0; i < N; i++) {
    if (last != h.hist[i]) {
      long start = i * Bucket + Offset;
      out << std::setw(10) << start
          << " - "
          << std::setw(10) << start + Bucket
          << ": "
          << std::setw(10) << h.hist[i] << " ";
      if (unit > 0)
        for (int j = 0; j < h.hist[i] / unit; j++)
          out << '@';

      out << std::endl;

      last = h.hist[i];
      repeat = false;
    } else {
      if (!repeat) {
        out << "..." << std::endl;
        repeat = true;
      }
    }
  }
  return out;
}

template <int N = 10, int Offset = 0, int Base = 2>
struct LogHistogram {
  long hist[N];
  LogHistogram() {
    memset(hist, 0, sizeof(long) * N);
  }
  LogHistogram &operator<<(long value) {
    long idx = std::log2(value) / std::log2(Base) - Offset;
    if (idx >= 0 && idx < N) hist[idx]++;
    return *this;
  }
  LogHistogram &operator<<(const LogHistogram &rhs) {
    for (int i = 0; i < N; i++) hist[i] += rhs.hist[i];
    return *this;
  }
};

template <int N = 10, int Offset, int Base = 2>
std::ostream &operator<<(std::ostream &out, const LogHistogram<N, Offset, Base> &h)
{
  long unit = 0;
  for (int i = 0; i < N; i++)
    if (unit < h.hist[i] / 100) unit = h.hist[i] / 100;
  long start = long(std::pow(Base, Offset));
  for (int i = 0; i < N; i++) {
    long end = start * Base;
    out << std::setw(10) << start
        << " - "
        << std::setw(10) << end
        << ": "
        << std::setw(10) << h.hist[i] << " ";
    if (unit > 0)
      for (int j = 0; j < h.hist[i] / unit; j++)
        out << '@';
    out << std::endl;
    start = end;
  }
  return out;
}

}


#endif /* PROBE_H */
