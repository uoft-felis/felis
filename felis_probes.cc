#include <cstdlib>
#include <iostream>
#include <fstream>
#include "gopp/gopp.h"

#include "felis_probes.h"
#include "probe_utils.h"
#include "opts.h"
#include "node_config.h"

#include "vhandle.h" // Let's hope this won't slow down the build.
#include "gc.h"
#include "epoch.h"

static struct ProbeMain {
  agg::Agg<agg::LogHistogram<16>> wait_cnt;
  agg::Agg<agg::LogHistogram<18, 0, 2>> versions;
  agg::Agg<agg::Histogram<32, 0, 1>> write_cnt;

  agg::Agg<agg::Histogram<32, 0, 1>> neworder_cnt;
  agg::Agg<agg::Histogram<32, 0, 1>> payment_cnt;
  agg::Agg<agg::Histogram<32, 0, 1>> delivery_cnt;

  agg::Agg<agg::Histogram<16, 0, 1>> absorb_memmove_size_detail;
  agg::Agg<agg::Histogram<1024, 0, 16>> absorb_memmove_size;
  agg::Agg<agg::Average> absorb_memmove_avg;
  agg::Agg<agg::Histogram<128, 0, 1 << 10>> msc_wait_cnt;
  agg::Agg<agg::Average> msc_wait_cnt_avg;

  std::vector<long> mem_usage;
  std::vector<long> expansion;

  agg::Agg<agg::Average> init_queue_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, int>>> init_queue_max;
  agg::Agg<agg::Histogram<512, 0, 2>> init_queue_hist;

  agg::Agg<agg::Average> init_fail_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, int>>> init_fail_max;
  agg::Agg<agg::Histogram<512, 0, 1>> init_fail_hist;

  agg::Agg<agg::Sum> init_fail_cnt;
  agg::Agg<agg::Average> init_fail_cnt_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, int>>> init_fail_cnt_max;
  agg::Agg<agg::Histogram<128, 0, 1>> init_fail_cnt_hist;

  agg::Agg<agg::Average> init_succ_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, int>>> init_succ_max;
  agg::Agg<agg::Histogram<512, 0, 1>> init_succ_hist;

  agg::Agg<agg::Average> exec_queue_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, int>>> exec_queue_max;
  agg::Agg<agg::Histogram<1024, 0, 2>> exec_queue_hist;

  agg::Agg<agg::Average> exec_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, int>>> exec_max;
  agg::Agg<agg::Histogram<512, 0, 2>> exec_hist;

  agg::Agg<agg::Average> total_latency_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, int>>> total_latency_max;
  agg::Agg<agg::SpecialHistogram> total_latency_hist;

  agg::Agg<agg::Average> piece_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, uintptr_t, int>>> piece_max;
  agg::Agg<agg::Histogram<512, 0, 1>> piece_hist;

  agg::Agg<agg::Average> dist_global_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, int>>> dist_global_max;
  agg::Agg<agg::Histogram<1024, -10000, 20>> dist_global_hist;

  agg::Agg<agg::Average> dist_local_avg;
  agg::Agg<agg::Max<std::tuple<uint64_t, int>>> dist_local_max;
  agg::Agg<agg::Histogram<1024, -10000, 20>> dist_local_hist;
  ~ProbeMain();
} global;

thread_local struct ProbePerCore {
  AGG(wait_cnt);
  AGG(versions);
  AGG(write_cnt);

  AGG(neworder_cnt);
  AGG(payment_cnt);
  AGG(delivery_cnt);

  AGG(absorb_memmove_size_detail);
  AGG(absorb_memmove_size);
  AGG(absorb_memmove_avg);
  AGG(msc_wait_cnt);
  AGG(msc_wait_cnt_avg);

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
  AGG(dist_global_avg);
  AGG(dist_global_max);
  AGG(dist_global_hist);
  AGG(dist_local_avg);
  AGG(dist_local_max);
  AGG(dist_local_hist);
} statcnt;

// Default for all probes
template <typename T> void OnProbe(T t) {}

static void CountUpdate(agg::Histogram<32, 0, 1> &agg, int nr_update, int core = -1)
{
  if (core == -1)
    core = go::Scheduler::CurrentThreadPoolId() - 1;
  while (nr_update--)
    agg << core;
}

////////////////////////////////////////////////////////////////////////////////
// Override for some enabled probes
////////////////////////////////////////////////////////////////////////////////

#if 0

template <> void OnProbe(felis::probes::VHandleAbsorb p)
{
  statcnt.absorb_memmove_size << p.size;
  statcnt.absorb_memmove_size_detail << p.size;
  statcnt.absorb_memmove_avg << p.size;
}

thread_local uint64_t last_tsc;
template <> void OnProbe(felis::probes::VHandleAppend p)
{
  last_tsc = __rdtsc();
}

template <> void OnProbe(felis::probes::VHandleAppendSlowPath p)
{
  auto msc_wait = __rdtsc() - last_tsc;
  statcnt.msc_wait_cnt << msc_wait;
  statcnt.msc_wait_cnt_avg << msc_wait;
}

#endif

#if 0
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

template <> void OnProbe(felis::probes::VersionWrite p)
{
  if (p.epoch_nr > 0) {
    CountUpdate(statcnt.write_cnt, 1);

    // Check if we are the last write
    auto row = (felis::SortedArrayVHandle *) p.handle;
    if (row->nr_versions() == p.pos + 1) {
      statcnt.versions << row->nr_versions() - row->current_start();
    }
  }
}

static int nr_split = 0;

template <> void OnProbe(felis::probes::OnDemandSplit p)
{
  nr_split += p.nr_splitted;
}

static long total_expansion = 0;

template <> void OnProbe(felis::probes::EndOfPhase p)
{
  if (p.phase_id != 1) return;

  auto p1 = mem::GetMemStats(mem::RegionPool);
  auto p2 = mem::GetMemStats(mem::VhandlePool);

  global.mem_usage.push_back(p1.used + p2.used);
  global.expansion.push_back(total_expansion);
}

template <> void OnProbe(felis::probes::VHandleExpand p)
{
  total_expansion += p.newcap - p.oldcap;
}

#endif

template <> void OnProbe(felis::probes::PriInitQueueTime p)
{
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  statcnt.init_queue_avg << p.time;
  statcnt.init_queue_hist << p.time;
  statcnt.init_queue_max.addData(p.time, std::make_tuple(p.sid, core_id));
}

template <> void OnProbe(felis::probes::PriInitTime p)
{
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  if (p.fail_time != 0) {
    statcnt.init_fail_cnt << p.fail_cnt;
    statcnt.init_fail_cnt_avg << p.fail_cnt;
    statcnt.init_fail_cnt_max.addData(p.fail_cnt, std::make_tuple(p.sid, core_id));
    statcnt.init_fail_cnt_hist << p.fail_cnt;
  }

  statcnt.init_fail_avg << p.fail_time;
  statcnt.init_fail_max.addData(p.fail_time, std::make_tuple(p.sid, core_id));
  statcnt.init_fail_hist << p.fail_time;

  statcnt.init_succ_avg << p.succ_time;
  statcnt.init_succ_max.addData(p.succ_time, std::make_tuple(p.sid, core_id));
  statcnt.init_succ_hist << p.succ_time;
}

template <> void OnProbe(felis::probes::PriExecQueueTime p)
{
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  statcnt.exec_queue_avg << p.time;
  statcnt.exec_queue_hist << p.time;
  statcnt.exec_queue_max.addData(p.time, std::make_tuple(p.sid, core_id));
}

template <> void OnProbe(felis::probes::PriExecTime p)
{
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  statcnt.exec_avg << p.time;
  statcnt.exec_hist << p.time;
  statcnt.exec_max.addData(p.time, std::make_tuple(p.sid, core_id));
  statcnt.total_latency_avg << p.total_latency;
  statcnt.total_latency_hist << p.total_latency;
  statcnt.total_latency_max.addData(p.total_latency, std::make_tuple(p.sid, core_id));
}

template <> void OnProbe(felis::probes::PieceTime p)
{
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  statcnt.piece_avg << p.time;
  statcnt.piece_hist << p.time;
  statcnt.piece_max.addData(p.time, std::make_tuple(p.sid, p.addr, core_id));
}

template <> void OnProbe(felis::probes::Distance p)
{
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  statcnt.dist_global_avg << p.dist_global;
  statcnt.dist_global_hist << p.dist_global;
  statcnt.dist_global_max.addData(p.dist_global, std::make_tuple(p.sid, core_id));
  statcnt.dist_local_avg << p.dist_local;
  statcnt.dist_local_hist << p.dist_local;
  statcnt.dist_local_max.addData(p.dist_local, std::make_tuple(p.sid, core_id));
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

ProbeMain::~ProbeMain()
{
#if 0
  std::cout
      << "waitcnt" << std::endl
      << global.wait_cnt() << std::endl
      << global.write_cnt() << std::endl;
  std::cout << nr_split << std::endl
            << global.versions << std::endl;

  {
    std::ofstream fout("versions.csv");
    fout << "bin_start,bin_end,count" << std::endl;
    for (int i = 0; i < global.versions.kNrBins; i++) {
      fout << long(std::pow(2, i)) << ','
           << long(std::pow(2, i + 1)) << ','
           << global.versions.hist[i] / 49 << std::endl;
    }
  }

  {
    std::ofstream fout("mem_usage.log");
    int label = felis::GC::g_lazy ? -1 : felis::GC::g_gc_every_epoch;
    for (int i = 0; i < mem_usage.size(); i++) {
      fout << label << ',' << i << ',' << mem_usage[i] << std::endl;
    }
  }
#endif

#if 0
  std::cout << "VHandle MSC Spin Time Distribution (in TSC)" << std::endl
            << global.msc_wait_cnt << std::endl;
  std::cout << "VHandle MSC Spin Time Avg: "
            << global.msc_wait_cnt_avg
            << std::endl;

  std::cout << "Memmove/Sorting Distance Distribution:" << std::endl;
  std::cout << global.absorb_memmove_size_detail
            << global.absorb_memmove_size << std::endl;
  std::cout << "Memmove/Sorting Distance Medium: "
            << global.absorb_memmove_size_detail.CalculatePercentile(
                .5 * global.absorb_memmove_size.Count() / global.absorb_memmove_size_detail.Count())
            << std::endl;
  std::cout << "Memmove/Sorting Distance Avg: " << global.absorb_memmove_avg << std::endl;
#endif
  std::cout << "[Pri-stat] batch piece " << global.piece_avg() << " us "
            << "(max: " << global.piece_max() << ")" << std::endl;
  std::cout << global.piece_hist();

  if (global.init_queue_avg().getCnt() != 0) {
    std::cout << "[Pri-stat] init_queue " << global.init_queue_avg() << " us "
              << "(max: " << global.init_queue_max() << ")" << std::endl;
    std::cout << global.init_queue_hist();
  }

  if (global.init_fail_avg().getCnt() != 0) {
    std::cout << "[Pri-stat] init_fail " << global.init_fail_avg() << " us "
              << "(max: " << global.init_fail_max() << ")" << std::endl;
    std::cout << global.init_fail_hist();
  }

  if (global.init_fail_cnt_avg().getCnt() != 0) {
    std::cout << "[Pri-stat] failed txn cnt: " << global.init_fail_cnt()
              << " (avg: " << global.init_fail_cnt_avg() << " times,"
              << " max: " << global.init_fail_cnt_max() << ")" << std::endl;
    std::cout << global.init_fail_cnt_hist();
  }

  if (global.init_succ_avg().getCnt() != 0) {
    std::cout << "[Pri-stat] init_succ " << global.init_succ_avg() << " us "
              << "(max: " << global.init_succ_max() << ")" << std::endl;
    std::cout << global.init_succ_hist();
  }

  if (global.exec_queue_avg().getCnt() != 0) {
    std::cout << "[Pri-stat] exec_queue " << global.exec_queue_avg() << " us "
              << "(max: " << global.exec_queue_max() << ")" << std::endl;
    std::cout << global.exec_queue_hist();
  }

  if (global.exec_avg().getCnt() != 0) {
    std::cout << "[Pri-stat] exec " << global.exec_avg() << " us "
              << "(max: " << global.exec_max() << ")" << std::endl;
    std::cout << global.exec_hist();
  }

  if (global.total_latency_avg().getCnt() != 0) {
    std::cout << "[Pri-stat] total_latency " << global.total_latency_avg() << " us "
              << "(max: " << global.total_latency_max() << ")" << std::endl;
    std::cout << global.total_latency_hist();
  }

  if (global.dist_global_avg().getCnt() != 0) {
    std::cout << "[Pri-stat] global dist " << global.dist_global_avg() << " sids "
              << "(max: " << global.dist_global_max() << ")" << std::endl;
    std::cout << global.dist_global_hist();
  }

  if (global.dist_local_avg().getCnt() != 0) {
    std::cout << "[Pri-stat] local dist " << global.dist_local_avg() << " sids "
              << "(max: " << global.dist_local_max() << ")" << std::endl;
    std::cout << global.dist_local_hist();
  }

  if (global.total_latency_avg().getCnt() != 0 && felis::Options::kOutputDir) {
    json11::Json::object result;
    const int size = PriTxnMeasureType::NumPriTxnMeasureType;
    agg::Agg<agg::Average> *arr[size + 1] = {
      &global.init_queue_avg, &global.init_fail_avg,
      &global.init_succ_avg, &global.exec_queue_avg,
      &global.exec_avg, &global.total_latency_avg,
      &global.dist_local_avg,
    };

    // X_1 is average, where X in [1,7]
    for (int i = 0; i < 7; ++i) {
      std::string label = std::to_string(i+1) + "_1";
      result.insert({label, arr[i]->getAvg()});
    }
    // X_2, X_3, X_4, X_5 are 50%, 90%, 99%, 99.9% numbers, where X in [1,6]
    for (int i = 0; i < 7; ++i) {
      for (int j = 2; j <= 5; ++j) {
        std::string label = std::to_string(i+1) + "_" + std::to_string(j);
        double pctile;
        switch (j) {
          case 2: pctile = 0.5;   break;
          case 3: pctile = 0.9;   break;
          case 4: pctile = 0.99;  break;
          case 5: pctile = 0.999; break;
        }
        switch (i) {
          case 0:
            result.insert({label, global.init_queue_hist.CalculatePercentile(pctile)});
            break;
          case 1:
            result.insert({label, global.init_fail_hist.CalculatePercentile(pctile)});
            break;
          case 2:
            result.insert({label, global.init_succ_hist.CalculatePercentile(pctile)});
            break;
          case 3:
            result.insert({label, global.exec_queue_hist.CalculatePercentile(pctile)});
            break;
          case 4:
            result.insert({label, global.exec_hist.CalculatePercentile(pctile)});
            break;
          case 5:
            result.insert({label, global.total_latency_hist.CalculatePercentile(pctile)});
            break;
          case 6:
            result.insert({label, global.dist_local_hist.CalculatePercentile(pctile)});
            break;
        }
      }
    }

    // 8_1 abort rate, unit %
    long cnt = global.total_latency_avg.getCnt();
    double abort_rate = 100.0 * global.init_fail_cnt.sum / cnt;
    result.insert({"8_1", abort_rate});
    // 8_2 txn count
    result.insert({"8_2", static_cast<int>(cnt)});
    // 9_1 batch throughput, 9_2 priority throughput, 9_3 total throughput
    auto &client = felis::EpochClient::g_workload_client;
    auto dur = client->GetPerf().duration_ms();
    long total_nr_txns = (client->g_max_epoch - 1) * client->NumberOfTxns();
    long batch_tpt = total_nr_txns * 1000 / dur; // duration is in ms
    long pri_tpt = batch_tpt * cnt / total_nr_txns;
    int total_tpt = batch_tpt + pri_tpt;
    result.insert({"9_1", static_cast<int>(batch_tpt)});
    result.insert({"9_2", static_cast<int>(pri_tpt)});
    result.insert({"9_3", total_tpt});

    auto node_name = util::Instance<felis::NodeConfiguration>().config().name;
    time_t tm;
    char now[80];
    time(&tm);
    strftime(now, 80, "-%F-%X", localtime(&tm));
    std::ofstream result_output(
        felis::Options::kOutputDir.Get() + "/pri" + now + ".json");
    result_output << json11::Json(result).dump() << std::endl;

    // std::ofstream latency_dist_output(
    //     felis::Options::kOutputDir.Get() + "/latency_dist.log");
    // latency_dist_output << global.total_latency_hist();
  }

}

PROBE_LIST;
