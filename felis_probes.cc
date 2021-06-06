#include <cstdlib>
#include <iostream>
#include <fstream>

#include "felis_probes.h"
#include "probe_utils.h"
#include "opts.h"
#include "node_config.h"

static struct ProbeMain {
  agg::Agg<agg::LogHistogram<16>> wait_cnt;

  agg::Agg<agg::Average> init_queue_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, uint64_t>>> init_queue_max;
  agg::Agg<agg::Histogram<512, 0, 3>> init_queue_hist;

  agg::Agg<agg::Average> init_fail_avg;
  agg::Agg<agg::Max<int64_t>> init_fail_max;
  agg::Agg<agg::Histogram<512, 0, 2>> init_fail_hist;

  agg::Agg<agg::Sum> init_fail_cnt;
  agg::Agg<agg::Average> init_fail_cnt_avg;
  agg::Agg<agg::Max<int64_t>> init_fail_cnt_max;
  agg::Agg<agg::Histogram<128, 0, 8>> init_fail_cnt_hist;

  agg::Agg<agg::Average> init_succ_avg;
  agg::Agg<agg::Max<int64_t>> init_succ_max;
  agg::Agg<agg::Histogram<128, 0, 2>> init_succ_hist;

  agg::Agg<agg::Average> exec_queue_avg;
  agg::Agg<agg::Max<int64_t>> exec_queue_max;
  agg::Agg<agg::Histogram<512, 0, 3>> exec_queue_hist;

  agg::Agg<agg::Average> exec_avg;
  agg::Agg<agg::Max<int64_t>> exec_max;
  agg::Agg<agg::Histogram<128, 0, 2>> exec_hist;

  agg::Agg<agg::Average> total_latency_avg;
  agg::Agg<agg::Max<int64_t>> total_latency_max;
  agg::Agg<agg::Histogram<2048, 0, 2>> total_latency_hist;

  agg::Agg<agg::Average> piece_avg;
  agg::Agg<agg::Max<int64_t>> piece_max;
  agg::Agg<agg::Histogram<512, 0, 5>> piece_hist;

  agg::Agg<agg::Average> dist_avg;
  agg::Agg<agg::Max<int64_t>> dist_max;
  agg::Agg<agg::Histogram<512, -10000, 40>> dist_hist;
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
  AGG(init_fail_cnt);
  AGG(init_fail_cnt_avg);
  AGG(init_fail_cnt_max);
  AGG(init_fail_cnt_hist);
  AGG(init_succ_avg);
  AGG(init_succ_max);
  AGG(init_succ_hist);
  AGG(exec_queue_avg);
  AGG(exec_queue_max);
  AGG(exec_queue_hist);
  AGG(exec_avg);
  AGG(exec_max);
  AGG(exec_hist);
  AGG(total_latency_avg);
  AGG(total_latency_max);
  AGG(total_latency_hist);
  AGG(piece_avg);
  AGG(piece_max);
  AGG(piece_hist);
  AGG(dist_avg);
  AGG(dist_max);
  AGG(dist_hist);
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
  if (p.fail_time != 0) {
    statcnt.init_fail_cnt << p.fail_cnt;
    statcnt.init_fail_cnt_avg << p.fail_cnt;
    statcnt.init_fail_cnt_max.addData(p.fail_cnt, p.sid);
    statcnt.init_fail_cnt_hist << p.fail_cnt;
  }

  statcnt.init_fail_avg << p.fail_time;
  statcnt.init_fail_max.addData(p.fail_time, p.sid);
  statcnt.init_fail_hist << p.fail_time;

  statcnt.init_succ_avg << p.succ_time;
  statcnt.init_succ_max.addData(p.succ_time, p.sid);
  statcnt.init_succ_hist << p.succ_time;
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

template <> void OnProbe(felis::probes::PieceTime p)
{
  statcnt.piece_avg << p.time;
  statcnt.piece_hist << p.time;
  statcnt.piece_max.addData(p.time, p.sid);
}

template <> void OnProbe(felis::probes::Distance p)
{
  statcnt.dist_avg << p.dist;
  statcnt.dist_hist << p.dist;
  statcnt.dist_max.addData(p.dist, p.sid);
}

enum PriTxnMeasureType : int{
  InitQueue,
  InitFail,
  InitSucc,
  ExecQueue,
  Exec,
  Total,
  NumPriTxnMeasureType,
};

const std::string kPriTxnMeasureTypeLabel[] = {
  "1init_queue",
  "2init_fail",
  "3init_succ",
  "4exec_queue",
  "5exec",
  "6total_latency",
};

ProbeMain::~ProbeMain()
{
  // std::cout << global.wait_cnt() << std::endl;

  if (!felis::NodeConfiguration::g_priority_txn)
    return;
  // std::cout << "[Pri-stat] (batched and priority) piece " << global.piece_avg() << "  us "
  //           << "(max: " << global.piece_max() << ")" << std::endl;
  // std::cout << global.piece_hist();

  std::cout << "[Pri-stat] init_queue " << global.init_queue_avg() << " us "
            << "(max: " << global.init_queue_max() << ")" << std::endl;
  // std::cout << global.init_queue_hist();

  std::cout << "[Pri-stat] init_fail " << global.init_fail_avg() << " us "
            << "(failed txn cnt: " << global.init_fail_cnt() << ") "
            << "(max: " << global.init_fail_max() << ")" << std::endl;
  // std::cout << global.init_fail_hist();

  std::cout << "[Pri-stat] failed txn cnt: " << global.init_fail_cnt()
            << " (avg: " << global.init_fail_cnt_avg() << " times,"
            << " max: " << global.init_fail_cnt_max() << ")" << std::endl;
  // std::cout << global.init_fail_cnt_hist();

  std::cout << "[Pri-stat] init_succ " << global.init_succ_avg() << " us "
            << "(max: " << global.init_succ_max() << ")" << std::endl;
  // std::cout << global.init_succ_hist();

  std::cout << "[Pri-stat] exec_queue " << global.exec_queue_avg() << " us "
            << "(max: " << global.exec_queue_max() << ")" << std::endl;
  // std::cout << global.exec_queue_hist();

  std::cout << "[Pri-stat] exec " << global.exec_avg() << " us "
            << "(max: " << global.exec_max() << ")" << std::endl;
  // std::cout << global.exec_hist();

  std::cout << "[Pri-stat] total_latency " << global.total_latency_avg() << " us "
            << "(max: " << global.total_latency_max() << ")" << std::endl;
  std::cout << global.total_latency_hist();

  // std::cout << "[Pri-stat] dist " << global.dist_avg() << " sids "
  //           << "(max: " << global.dist_max() << ")" << std::endl;
  // std::cout << global.dist_hist();

  if (felis::NodeConfiguration::g_priority_txn && felis::Options::kOutputDir) {
    json11::Json::object result;
    const int size = PriTxnMeasureType::NumPriTxnMeasureType;
    agg::Agg<agg::Average> *arr[size] = {
      &global.init_queue_avg, &global.init_fail_avg, &global.init_succ_avg,
      &global.exec_queue_avg, &global.exec_avg,
      &global.total_latency_avg,
    };

    for (int i = 0; i < size; ++i) {
      result.insert({kPriTxnMeasureTypeLabel[i], arr[i]->getAvg()});
    }
    result.insert({"7init_fail_cnt", std::to_string(global.init_fail_cnt.sum)});

    auto node_name = util::Instance<felis::NodeConfiguration>().config().name;
    time_t tm;
    char now[80];
    time(&tm);
    strftime(now, 80, "-%F-%X", localtime(&tm));
    std::ofstream result_output(
        felis::Options::kOutputDir.Get() + "/pri_latency.json");
    result_output << json11::Json(result).dump() << std::endl;

    std::ofstream latency_dist_output(
        felis::Options::kOutputDir.Get() + "/latency_dist.log");
    latency_dist_output << global.total_latency_hist();
  }

}


PROBE_LIST;
