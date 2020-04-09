// -*- mode: c++ -*-
#ifndef FELIS_PROBES_H
#define FELIS_PROBES_H

#include <cstdint>

namespace felis {
namespace probes {

struct NumVersionsOnGC {
  unsigned long nr;
  void operator()() const;
};

struct VersionRead {
  bool blocking;
  void *handle;
  void operator()() const;
};

struct VersionWrite {
  void *handle;
  uint64_t epoch_nr;
  void operator()() const;
};

struct WaitCounters {
  unsigned long wait_cnt;
  uint64_t sid;
  uint64_t version_id;
  uintptr_t ptr;
  void operator()() const;
};

struct VHandleAppend {
  void *handle;
  uint64_t sid;
  int alloc_regionid;
  void operator()() const;
};

struct LocalitySchedule {
  int core;
  int weight;
  uint64_t result;
  void operator()() const;
};

struct TpccNewOrder {
  int piece_id;
  int nr_update;
  void operator()() const;
};

struct TpccPayment {
  int piece_id;
  int nr_update;
  int warehouse_coreid;
  void operator()() const;
};

struct TpccDelivery {
  int piece_id;
  int nr_update;
  void operator()() const;
};

}
}

#define PROBE_LIST                              \
  PROBE_PROXY(felis::probes::NumVersionsOnGC);  \
  PROBE_PROXY(felis::probes::VersionRead);      \
  PROBE_PROXY(felis::probes::VersionWrite);     \
  PROBE_PROXY(felis::probes::WaitCounters);     \
  PROBE_PROXY(felis::probes::VHandleAppend);    \
  PROBE_PROXY(felis::probes::LocalitySchedule); \
  PROBE_PROXY(felis::probes::TpccNewOrder);     \
  PROBE_PROXY(felis::probes::TpccPayment);      \
  PROBE_PROXY(felis::probes::TpccDelivery);     \

#endif /* FELIS_PROBES_H */
