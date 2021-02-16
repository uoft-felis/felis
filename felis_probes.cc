#include <cstdlib>
#include <iostream>
#include <fstream>
#include "gopp/gopp.h"

#include "felis_probes.h"
#include "probe_utils.h"

#include "vhandle.h" // Let's hope this won't slow down the build.
#include "gc.h"

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
static std::mutex version_size_array_m;
//MOMO what should the initial values of the vesionSize array be? How to figure that out?
static int version_size_array[5000] = { 0 }; // all elements 0
// static std::vector<int> version_size_array(100, 0);
template <> void OnProbe(felis::probes::VersionSizeArray p)
{
  std::lock_guard _(version_size_array_m);
  // std::cout << "MOMO p.cur_size:" << p.cur_size <<" --- p.delta:" << p.delta << std::endl;
  
  if (p.cur_size > 5000)
  {
    return;
  }
  
  if(version_size_array[p.cur_size] != 0)
  {
    version_size_array[p.cur_size] -= 1;
  }
  
  version_size_array[p.cur_size + p.delta] += 1;

  // version_size_array[5] += 1;
}

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

#if 0

static std::mutex pool_m;
//number of bytes allocated for varstr
static long long total_varstr_alloc_bytes = 0;
static long long max_varstr_alloc_bytes = 0;
template <> void OnProbe(felis::probes::RegionPoolVarstr p)
{
  std::lock_guard _(pool_m); //released automatically when lockguard variable is destroyed
  total_varstr_alloc_bytes += p.num_bytes;
  if (total_varstr_alloc_bytes > max_varstr_alloc_bytes)
  {
    max_varstr_alloc_bytes = total_varstr_alloc_bytes;
  }
}
#endif

static std::mutex trans_pers_m;
// number of bytes allocated for varstr
static long long total_transient = 0;
static long long total_persistent = 0;
template <> void OnProbe(felis::probes::TransientPersistentCount p) {
  std::lock_guard _(trans_pers_m); // released automatically when lockguard
                                   // variable is destroyed
  if (p.isPersistent) {
    total_persistent++;
  }
  else {
    total_transient++;
  }
}

ProbeMain::~ProbeMain()
{
  std::cout << "number of transient varstr: " << total_transient << std::endl;
  std::cout << "number of persistent varstr: " << total_persistent << std::endl;
  // std::cout << "MOMO printing versionSizeArray of size: " << version_size_array.size()  << std::endl;

  std::cout << "MOMO printing versionSizeArray for upto size 20 out of 5000" << std::endl;

  for(int i = 0; i < 20; i++) {
    std::cout << "MOMO versionSizeArray[" << i << "]:" << version_size_array[i] << std::endl;
  }
  std::cout << "MOMO DONE printing versionSizeArray" << std::endl;

#if 0
  std::cout << "number of bytes allocated for varstr: "
            << total_varstr_alloc_bytes << " (max " << max_varstr_alloc_bytes << ")" << std::endl;
#endif

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
}

PROBE_LIST;
