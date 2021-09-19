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
#include <sstream>


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
  long getCnt() { return cnt; }
};

static inline std::ostream &operator<<(std::ostream &out, const Average &avg)
{
  if (avg.cnt == 0) {
    out << "NaN";
    return out;
  }
  out << 1.0l * avg.sum / avg.cnt << " (cnt " << avg.cnt << ")";
  return out;
}

template <typename Type>
struct Max {
  int64_t max = std::numeric_limits<int64_t>::min();
  Type properties;
  Max &operator<<(int64_t value) {
    if (value > max)
      max = value;
    return *this;
  }
  void addData(int64_t value, Type _properties) {
    if (value > max) {
      max = value;
      properties = _properties;
    }
  }
  int64_t getMax() { return this->max; }
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
  return "epoch " + std::to_string(sid >> 32) +
         ", txn sequence " + std::to_string(sid >> 8 & 0xFFFFFF);
}

template <>
std::ostream &operator<<(std::ostream &out, const Max<std::tuple<uint64_t, int>> &max)
{
  out << max.max << " us (at core " << std::get<1>(max.properties) << ", txn " << format_sid(std::get<0>(max.properties)) << ")";
  return out;
}

template <>
std::ostream &operator<<(std::ostream &out, const Max<std::tuple<uint64_t, uintptr_t, int>> &max)
{
  out << max.max << " us (at core " << std::get<2>(max.properties) << ", txn " << format_sid(std::get<0>(max.properties)) << ", func addr 0x" << std::hex << std::get<1>(max.properties) << std::dec << ")";
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
  int CalculatePercentile(double scale) {
    size_t total_nr = Count();
    long medium_idx = total_nr * scale;
    for (int i = 0; i < N; i++) {
      medium_idx -= hist[i];
      if (medium_idx < 0)
        return i * Bucket + Offset;
    }
    return Offset; // 0?
  }
  int CalculateMedian() { return CalculatePercentile(0.5); }
  size_t Count() {
    size_t total_nr = 0;
    for (int i = 0; i < N; i++)
      total_nr += hist[i];
    return total_nr;
  }
};

#define N_SPECIAL 109
struct SpecialHistogram {
  long hist[N_SPECIAL];
  SpecialHistogram() {
    memset(hist, 0, sizeof(long) * N_SPECIAL);
  }
  const double buckets[N_SPECIAL] = {
    0,
    1,1.5,2,2.5,3,3.5,4,4.5,5,5.5,6,6.5,7,7.5,8,8.5,9,9.5,
    10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,
    100,150,200,250,300,350,400,450,500,550,600,650,700,750,800,850,900,950,
    1000,1500,2000,2500,3000,3500,4000,4500,5000,5500,6000,6500,7000,7500,8000,8500,9000,9500,
    10000,15000,20000,25000,30000,35000,40000,45000,50000,55000,60000,65000,70000,75000,80000,85000,90000,95000,
    100000,150000,200000,250000,300000,350000,400000,450000,500000,550000,600000,650000,700000,750000,800000,850000,900000,950000,
  };
  int val2idx(double value) {
    if (value < 1)
      return 0;
    int exp = floor(log10(value));
    int start = pow(10, exp);
    double step = pow(10, exp) / 2;
    // printf("%lf %d %lf ", value, start, step);
    return exp * 18 + (value - start) / step + 1;
  }
  SpecialHistogram &operator<<(double value) {
    long idx = val2idx(value);
    if (idx >= 0 && idx < N_SPECIAL) hist[idx]++;
    else if (idx >= N_SPECIAL) hist[N_SPECIAL-1]++; // larger than max is put in last bucket
    return *this;
  }
  SpecialHistogram &operator<<(const SpecialHistogram &rhs) {
    for (int i = 0; i < N_SPECIAL; i++) hist[i] += rhs.hist[i];
    return *this;
  }
  double CalculatePercentile(double scale, int &idx) const {
    size_t total_nr = Count();
    long medium_idx = total_nr * scale;
    for (int i = 0; i < N_SPECIAL; i++) {
      medium_idx -= hist[i];
      if (medium_idx < 0) {
        idx = i;
        return buckets[i];
      }
    }
    return 0;
  }
  size_t Count() const {
    size_t total_nr = 0;
    for (int i = 0; i < N_SPECIAL; i++)
      total_nr += hist[i];
    return total_nr;
  }

  std::string OutputCdf() {
    std::ostringstream out;
    const long total_cnt = Count();
    long accu = 0;
    out << std::fixed;
    for (int i = 0; i < N_SPECIAL; ++i) {
      accu += hist[i];
      if (hist[i] != 0) {
        double cdf = (double)accu / (double)total_cnt;
        out << i+1 << "," << buckets[i] << "," << hist[i] << "," << cdf << std::endl;
      }
    }
    return out.str();
  }
};

std::ostream &operator<<(std::ostream &out, const SpecialHistogram& h)
{
  // percentile calc
  const int pct_size = 4;
  double percentages[pct_size] = {50, 90, 99, 99.9};

  for (int i = 0; i < pct_size; ++i) {
    int idx;
    out << std::setw(6) << percentages[i] << " percentile:  ["
        << h.CalculatePercentile(percentages[i] / 100, idx)
        << ", " << h.buckets[idx+1] << ")"
        << std::endl;
  }
  return out;
}


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
  // */

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
  static constexpr int kNrBins = N;
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
