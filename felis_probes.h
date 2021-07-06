// -*- mode: c++ -*-
#ifndef FELIS_PROBES_H
#define FELIS_PROBES_H

#include <cstdint>
#include "json11/json11.hpp"

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
  long pos;
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

struct VHandleAppendSlowPath {
  void *handle;
  void operator()() const;
};

struct VHandleAbsorb {
  void *handle;
  int size;
  void operator()() const;
};

struct VHandleExpand {
  void *handle;
  unsigned int oldcap;
  unsigned int newcap;

  void operator()() const;
};

struct OnDemandSplit {
  uint64_t sum;
  uint64_t nr_batched;
  uint64_t nr_splitted;
  void operator()() const;
};

struct LocalitySchedule {
  int core;
  int weight;
  uint64_t result;
  uint64_t seed;
  uint64_t max_seed;
  long load;
  void operator()() const;
};

struct EndOfPhase {
  uint64_t epoch_nr;
  int phase_id;
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

struct PriInitQueueTime {
  uint64_t time;
  uint64_t sid;
  void operator()() const;
};

struct PriInitTime {
  uint64_t succ_time;
  uint64_t fail_time;
  int fail_cnt;
  uint64_t sid;
  void operator()() const;
};

struct PriExecQueueTime {
  uint64_t time;
  uint64_t sid;
  void operator()() const;
};

struct PriExecTime {
  uint64_t time;
  uint64_t total_latency;
  uint64_t sid;
  void operator()() const;
};

struct PieceTime {
  uint64_t time;
  uint64_t sid;
  uintptr_t addr;
  void operator()() const;
};

struct Distance {
  int64_t dist;
  uint64_t sid;
  void operator()() const;
};
}
}

#define PROBE_LIST                                                             \
  PROBE_PROXY(felis::probes::NumVersionsOnGC);                                 \
  PROBE_PROXY(felis::probes::VersionRead);                                     \
  PROBE_PROXY(felis::probes::VersionWrite);                                    \
  PROBE_PROXY(felis::probes::WaitCounters);                                    \
  PROBE_PROXY(felis::probes::VHandleAppend);                                   \
  PROBE_PROXY(felis::probes::VHandleAppendSlowPath);                           \
  PROBE_PROXY(felis::probes::VHandleAbsorb);                                   \
  PROBE_PROXY(felis::probes::VHandleExpand);                                   \
  PROBE_PROXY(felis::probes::LocalitySchedule);                                \
  PROBE_PROXY(felis::probes::OnDemandSplit);                                   \
  PROBE_PROXY(felis::probes::EndOfPhase);                                      \
  PROBE_PROXY(felis::probes::TpccNewOrder);                                    \
  PROBE_PROXY(felis::probes::TpccPayment);                                     \
  PROBE_PROXY(felis::probes::TpccDelivery);                                    \
  PROBE_PROXY(felis::probes::PriInitQueueTime);                                \
  PROBE_PROXY(felis::probes::PriInitTime);                                     \
  PROBE_PROXY(felis::probes::PriExecQueueTime);                                \
  PROBE_PROXY(felis::probes::PriExecTime);                                     \
  PROBE_PROXY(felis::probes::PieceTime);                                       \
  PROBE_PROXY(felis::probes::Distance);

#endif /* FELIS_PROBES_H */
