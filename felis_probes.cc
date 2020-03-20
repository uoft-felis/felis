#include <cstdlib>
#include <iostream>

#include "felis_probes.h"
#include "probe_utils.h"

static struct ProbeMain {
  agg::Agg<agg::LogHistogram<16>> wait_cnt;

  ~ProbeMain();
} global;

thread_local struct ProbePerCore {
  AGG(wait_cnt);
} statcnt;

// Default for all probes
template <typename T> void OnProbe(T t) {}

// Override for some enabled probes
template <> void OnProbe(felis::probes::WaitCounters p)
{
  statcnt.wait_cnt << p.wait_cnt;
}

ProbeMain::~ProbeMain()
{
  std::cout << global.wait_cnt() << std::endl;
}

PROBE_LIST;
