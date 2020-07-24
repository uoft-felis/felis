// -*- mode: c++ -*-
#ifndef PROBE_UTILS_H
#define PROBE_UTILS_H

#include <cstring>
#include <cmath>
#include <mutex>
#include <set>
#include <iostream>
#include <iomanip>
#include <limits>


template <typename T> void OnProbe(T t);

#define PROBE_PROXY(klass) void klass::operator()() const { OnProbe(*this); }

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

  void Add(Value *node) {
    std::lock_guard<std::mutex> _(m);
    values.insert(node);
  }

  void Remove(Value *node) {
    std::lock_guard<std::mutex> _(m);
    (*this) << *node;
    values.erase(node);
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
  double getAvg() { return (cnt == 0) ? 0 : (1.0l * sum / cnt); }
};

static inline std::ostream &operator<<(std::ostream &out, const Average &avg)
{
  if (avg.cnt == 0) {
    out << "NaN";
    return out;
  }
  out << 1.0l * avg.sum / avg.cnt;
  return out;
}

template <typename Type>
struct Max {
  uint64_t max = 0;
  Type properties;
  Max &operator<<(uint64_t value) {
    if (value > max)
      max = value;
    return *this;
  }
  void addData(uint64_t value, Type _properties) {
    if (value > max) {
      max = value;
      properties = _properties;
    }
  }
  uint64_t getMax() { return this->max; }
  Max &operator<<(const Max &rhs) {
    if (rhs.max > max) {
      max = rhs.max;
      properties = rhs.properties;
    }
    return *this;
  }
};

template <typename Type>
static inline std::ostream &operator<<(std::ostream &out, const Max<Type> &max)
{
  out << max.max;
  return out;
}

std::string format_sid(uint64_t sid)
{
  return "node_id " + std::to_string(sid & 0x000000FF) +
         ", epoch " + std::to_string(sid >> 32) +
         ", txn sequence " + std::to_string(sid >> 8 & 0xFFFFFF);
}

template <>
std::ostream &operator<<(std::ostream &out, const Max<uint64_t> &max)
{
  out << max.max << " us, at txn " << format_sid(max.properties);
  return out;
}

template <>
std::ostream &operator<<(std::ostream &out, const Max<std::tuple<uint64_t, uint64_t>> &max)
{
  out << max.max << " us, at epoch " << std::get<0>(max.properties) << ", delay " << std::get<1>(max.properties) / 2200 << " us";
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
    else if (idx >= N) hist[N-1]++; // larger than max is put in last bucket
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

  /*
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
  */

  // percentile calc
  long sum = 0, accu = 0;
  for (int i = 0;  i < N; ++i)
    sum += h.hist[i];
  if (sum == 0) return out;

  const int pct_size = 4;
  double percentages[pct_size] = {50, 90, 99, 99.9};
  int lvls[pct_size];
  for (int i = 0; i < pct_size; ++i)
    lvls[i] = -1;

  for (int i = 0; i < N; ++i) {
    accu += h.hist[i];
    auto accu_pct = (double)accu * 100 / sum;
    for (int j = 0; j < pct_size; ++j)
      if (lvls[j] == -1 && accu_pct > percentages[j])
        lvls[j] = i;
  }

  for (int i = 0; i < pct_size; ++i)
    if (lvls[i] != -1)
      out << std::setw(6) << percentages[i] << " percentile:  "
          << "[" << lvls[i] * Bucket + Offset
          << ", " << lvls[i] * Bucket + Offset + Bucket << "), "
          << "bucket " << lvls[i] + 1 << "/" << N << ",   "
          << std::endl;
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


#endif /* PROBE_UTILS_H */
