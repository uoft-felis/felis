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

struct WaitCounters {
  unsigned long wait_cnt;
  uint64_t sid;
  uint64_t version_id;
  void operator()() const;
};

}
}

#define PROBE_LIST                              \
  PROBE_PROXY(felis::probes::NumVersionsOnGC);  \
  PROBE_PROXY(felis::probes::VersionRead);      \
  PROBE_PROXY(felis::probes::WaitCounters);     \

#endif /* FELIS_PROBES_H */
