#include <cstdlib>
#include <iostream>
#include "gopp/gopp.h"

#include "felis_probes.h"
#include "probe_utils.h"

static struct ProbeMain {
  agg::Agg<agg::LogHistogram<16>> wait_cnt;
  agg::Agg<agg::Histogram<32, 0, 1>> write_cnt;
  agg::Agg<agg::Histogram<32, 0, 1>> lm_ask_cnt;
  agg::Agg<agg::Histogram<32, 0, 1>> lm_ans_cnt;

  agg::Agg<agg::Histogram<32, 0, 1>> neworder_cnt;
  agg::Agg<agg::Histogram<32, 0, 1>> payment_cnt;
  agg::Agg<agg::Histogram<32, 0, 1>> delivery_cnt;
  ~ProbeMain();
} global;

thread_local struct ProbePerCore {
  AGG(wait_cnt);
  AGG(write_cnt);
  AGG(lm_ask_cnt);
  AGG(lm_ans_cnt);

  AGG(neworder_cnt);
  AGG(payment_cnt);
  AGG(delivery_cnt);
} statcnt;

// Default for all probes
template <typename T> void OnProbe(T t) {}

// Override for some enabled probes

thread_local uint64_t last_wait_cnt;

template <> void OnProbe(felis::probes::VersionRead p)
{
  last_wait_cnt = 0;
}

template <> void OnProbe(felis::probes::WaitCounters p)
{
  statcnt.wait_cnt << p.wait_cnt;
  last_wait_cnt = p.wait_cnt;
}

static void CountUpdate(agg::Histogram<32, 0, 1> &agg, int nr_update, int core = -1)
{
  if (core == -1)
    core = go::Scheduler::CurrentThreadPoolId() - 1;
  while (nr_update--)
    agg << core;
}

#if 0

template <> void OnProbe(felis::probes::TpccDelivery p)
{
  CountUpdate(statcnt.delivery_cnt, p.nr_update);
}

template <> void OnProbe(felis::probes::TpccPayment p)
{
  CountUpdate(statcnt.payment_cnt, p.nr_update);
}

template <> void OnProbe(felis::probes::TpccNewOrder p)
{
  CountUpdate(statcnt.neworder_cnt, p.nr_update);
}

#endif

template <> void OnProbe(felis::probes::VersionWrite p)
{
  if (p.epoch_nr > 0)
    CountUpdate(statcnt.write_cnt, 1);
}

template <> void OnProbe(felis::probes::LocalitySchedule p)
{
  CountUpdate(statcnt.lm_ask_cnt, 1, p.core);
  CountUpdate(statcnt.lm_ans_cnt, 1, p.result);
}

ProbeMain::~ProbeMain()
{
  std::cout
      << "waitcnt" << std::endl
      << global.wait_cnt() << std::endl
      << global.write_cnt() << std::endl
      << global.lm_ask_cnt() << std::endl
      << global.lm_ans_cnt() << std::endl;
}

PROBE_LIST;
