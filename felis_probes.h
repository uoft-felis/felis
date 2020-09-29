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

struct WaitCounters {
  unsigned long wait_cnt;
  uint64_t sid;
  uint64_t version_id;
  void operator()() const;
};

struct PriInitQueueTime {
  uint64_t time;
  uint64_t epoch_nr;
  uint64_t delay;
  void operator()() const;
};

struct PriInitTime {
  uint64_t succ_time;
  uint64_t fail_time;
  int fail_cnt;
  uint64_t sid;
  void operator()() const;
};

struct PriExecIssueTime {
  uint64_t time;
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
  void operator()() const;
};

struct Distance {
  uint64_t dist;
  uint64_t sid;
  void operator()() const;
};
}
}

#define PROBE_LIST                              \
  PROBE_PROXY(felis::probes::NumVersionsOnGC);  \
  PROBE_PROXY(felis::probes::VersionRead);      \
  PROBE_PROXY(felis::probes::WaitCounters);     \
  PROBE_PROXY(felis::probes::PriInitQueueTime); \
  PROBE_PROXY(felis::probes::PriInitTime); \
  PROBE_PROXY(felis::probes::PriExecIssueTime); \
  PROBE_PROXY(felis::probes::PriExecQueueTime); \
  PROBE_PROXY(felis::probes::PriExecTime); \
  PROBE_PROXY(felis::probes::PieceTime); \
  PROBE_PROXY(felis::probes::Distance); \

#endif /* FELIS_PROBES_H */
