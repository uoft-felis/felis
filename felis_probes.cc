#include <cstdlib>
#include <iostream>

#include "felis_probes.h"
#include "probe_utils.h"

static struct ProbeMain {
  agg::Agg<agg::LogHistogram<16>> wait_cnt;

  agg::Agg<agg::Average> init_queue_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, uint64_t>>> init_queue_max;
  agg::Agg<agg::Histogram<128, 0, 2>> init_queue_hist;

  agg::Agg<agg::Average> init_fail_avg;
  agg::Agg<agg::Max<uint64_t>> init_fail_max;
  agg::Agg<agg::Histogram<128, 0, 2>> init_fail_hist;

  agg::Agg<agg::Average> init_succ_avg;
  agg::Agg<agg::Max<uint64_t>> init_succ_max;
  agg::Agg<agg::Histogram<128, 0, 2>> init_succ_hist;

  agg::Agg<agg::Average> exec_issue_avg;
  agg::Agg<agg::Max<uint64_t>> exec_issue_max;
  agg::Agg<agg::Histogram<128, 0, 1>> exec_issue_hist;

  agg::Agg<agg::Average> exec_queue_avg;
  agg::Agg<agg::Max<uint64_t>> exec_queue_max;
  agg::Agg<agg::Histogram<128, 0, 4>> exec_queue_hist;

  agg::Agg<agg::Average> exec_avg;
  agg::Agg<agg::Max<uint64_t>> exec_max;
  agg::Agg<agg::Histogram<128, 0, 2>> exec_hist;

  agg::Agg<agg::Average> total_latency_avg;
  agg::Agg<agg::Max<uint64_t>> total_latency_max;
  agg::Agg<agg::Histogram<256, 0, 4>> total_latency_hist;

  ~ProbeMain();
} global;

thread_local struct ProbePerCore {
  AGG(wait_cnt);

  AGG(init_queue_avg);
  AGG(init_queue_max);
  AGG(init_queue_hist);
  AGG(init_fail_avg);
  AGG(init_fail_max);
  AGG(init_fail_hist);
  AGG(init_succ_avg);
  AGG(init_succ_max);
  AGG(init_succ_hist);
  AGG(exec_issue_avg);
  AGG(exec_issue_max);
  AGG(exec_issue_hist);
  AGG(exec_queue_avg);
  AGG(exec_queue_max);
  AGG(exec_queue_hist);
  AGG(exec_avg);
  AGG(exec_max);
  AGG(exec_hist);
  AGG(total_latency_avg);
  AGG(total_latency_max);
  AGG(total_latency_hist);
} statcnt;

// Default for all probes
template <typename T> void OnProbe(T t) {}

// Override for some enabled probes
template <> void OnProbe(felis::probes::WaitCounters p)
{
  statcnt.wait_cnt << p.wait_cnt;
}

template <> void OnProbe(felis::probes::PriInitQueueTime p)
{
  statcnt.init_queue_avg << p.time;
  statcnt.init_queue_hist << p.time;
  statcnt.init_queue_max.addData(p.time, std::make_tuple(p.epoch_nr, p.delay));
}

template <> void OnProbe(felis::probes::PriInitTime p)
{
  statcnt.init_fail_avg << p.fail_time;
  statcnt.init_fail_max.addData(p.fail_time, p.sid);
  statcnt.init_fail_hist << p.fail_time;
  statcnt.init_succ_avg << p.succ_time;
  statcnt.init_succ_max.addData(p.succ_time, p.sid);
  statcnt.init_succ_hist << p.succ_time;
}


template <> void OnProbe(felis::probes::PriExecIssueTime p)
{
  statcnt.exec_issue_avg << p.time;
  statcnt.exec_issue_hist << p.time;
  statcnt.exec_issue_max.addData(p.time, p.sid);
}

template <> void OnProbe(felis::probes::PriExecQueueTime p)
{
  statcnt.exec_queue_avg << p.time;
  statcnt.exec_queue_hist << p.time;
  statcnt.exec_queue_max.addData(p.time, p.sid);
}

template <> void OnProbe(felis::probes::PriExecTime p)
{
  statcnt.exec_avg << p.time;
  statcnt.exec_hist << p.time;
  statcnt.exec_max.addData(p.time, p.sid);
  statcnt.total_latency_avg << p.total_latency;
  statcnt.total_latency_hist << p.total_latency;
  statcnt.total_latency_max.addData(p.total_latency, p.sid);
}

ProbeMain::~ProbeMain()
{
  std::cout << "[Pri-stat] init queue " << global.init_queue_avg() << " us, "
            << "max " << global.init_queue_max() << std::endl;
  std::cout << global.init_queue_hist() << std::endl << std::endl;

  std::cout << "[Pri-stat] init fail " << global.init_fail_avg() << " us, "
            << "max " << global.init_fail_max() << std::endl;
  std::cout << global.init_fail_hist() << std::endl << std::endl;

  std::cout << "[Pri-stat] init succ " << global.init_succ_avg() << " us, "
            << "max " << global.init_succ_max() << std::endl;
  std::cout << global.init_succ_hist() << std::endl << std::endl;

  std::cout << "[Pri-stat] exec_issue " << global.exec_issue_avg() << " us, "
            << "max " << global.exec_issue_max() << std::endl;
  std::cout << global.exec_issue_hist() << std::endl << std::endl;

  std::cout << "[Pri-stat] exec_queue " << global.exec_queue_avg() << " us, "
            << "max " << global.exec_queue_max() << std::endl;
  std::cout << global.exec_queue_hist() << std::endl << std::endl;

  std::cout << "[Pri-stat] exec " << global.exec_avg() << " us, "
            << "max " << global.exec_max() << std::endl;
  std::cout << global.exec_hist() << std::endl << std::endl;

  std::cout << "[Pri-stat] total " << global.total_latency_avg() << " us, "
            << "max " << global.total_latency_max() << std::endl;
  std::cout << global.total_latency_hist() << std::endl << std::endl;

  // std::cout << global.wait_cnt() << std::endl;
}

PROBE_LIST;
