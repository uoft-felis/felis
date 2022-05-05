#include <cstdlib>
#include <iostream>
#include <fstream>
#include "gopp/gopp.h"

#include "felis_probes.h"
#include "probe_utils.h"

#include "vhandle.h" // Let's hope this won't slow down the build.
#include "index_info.h"
#include "gc.h"
#include "gc_dram.h"

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
static int version_size_array[5001] = { 0 }; // all elements 0
// static std::vector<int> version_size_array(100, 0);
template <> void OnProbe(felis::probes::VersionSizeArray p)
{
  std::lock_guard _(version_size_array_m);
  // std::cout << "MOMO p.cur_size:" << p.cur_size <<" --- p.delta:" << p.delta << std::endl;
  int index = p.cur_size;
  int new_index = p.cur_size + p.delta; 
  if (index >= 5000)
  {
    index = 5000;
  }
  if (new_index >= 5000)
  {
    new_index = 5000;
  }

  if(version_size_array[index] != 0)
  {
    version_size_array[index] -= 1;
  }
  
  version_size_array[new_index] += 1;

}

static std::mutex mem_alloc_parallel_brk_pool_m;
static int mem_alloc_parallel_brk_pool_per_epoch[51] = {0}; // all elements 0
static unsigned long total_mem_allocated = 0;
static unsigned long max_mem_allocated_per_epoch = 0;
static int mem_probe_index = 0;
template <> void OnProbe(felis::probes::MemAllocParallelBrkPool p) {
  std::lock_guard _(mem_alloc_parallel_brk_pool_m);
  int offset = p.cur_offset;
  
  total_mem_allocated += offset;
  if (offset > max_mem_allocated_per_epoch){
    max_mem_allocated_per_epoch = offset;
  }
  // int epoch_index = mem_probe_index;

  // mem_alloc_parallel_brk_pool_per_epoch[epoch_index] += offset;
  // mem_probe_index += 1;
}


//shirley: # versions allocated through varstr new use_pmem=true, total bytes allocated for them
static std::mutex varstr_new_pmem_m;
static int total_varstr_new_pmem_bytes = 0;
static int max_varstr_new_pmem_bytes = 0;
static unsigned long total_varstr_new_pmem_number = 0;
template <> void OnProbe(felis::probes::VarStrNewPmem p) {
  std::lock_guard _(varstr_new_pmem_m);
  total_varstr_new_pmem_bytes += (p.num_bytes > 0) ? p.num_bytes : 0;
  max_varstr_new_pmem_bytes += p.num_bytes;
  total_varstr_new_pmem_number += 1;
}

//shirley: version values sizes in the database
static std::mutex version_value_size_array_m;
static int version_value_size_array[162] = {0}; // all elements 0
template <> void OnProbe(felis::probes::VersionValueSizeArray p) {
  std::lock_guard _(version_value_size_array_m);
  int index = p.cur_size + 4; //shirley: plus 4 because each varstr has header of 4 bytes
  if (index >= 161) {
    index = 161;
  }
  version_value_size_array[index] += 1;
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

//Corey: Comparing total # inline allocations to # external allocations
static std::mutex version_alloc_comparison_m;
int countInlineAlloc = 0;
int countExtAlloc = 0;
template <> void OnProbe(felis::probes::VersionAllocCountInlineToExternal p) {
  std::lock_guard _(version_alloc_comparison_m);
  countInlineAlloc += p.countInlineAlloc;
  countExtAlloc += p.countExtAlloc;
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

static std::mutex index_size_m;
// number of bytes allocated for varstr
static long long index_size_total = 0;
template <> void OnProbe(felis::probes::IndexSizeTotal p) {
  std::lock_guard _(index_size_m); // released automatically when lockguard
                                   // variable is destroyed
  index_size_total += p.num_bytes;
}

static std::mutex num_vhdl_m;
// number of bytes allocated for varstr
static long long number_vhandles_total = 0;
template <> void OnProbe(felis::probes::NumVHandlesTotal p) {
  std::lock_guard _(num_vhdl_m); // released automatically when lockguard
                                 // variable is destroyed
  number_vhandles_total += p.num_vhandles;
}

static std::mutex num_unwritten_m;
// number of bytes allocated for varstr
static long long number_unwritten_total = 0;
template <> void OnProbe(felis::probes::NumUnwrittenDramCache p) {
  std::lock_guard _(num_unwritten_m); // released automatically when lockguard
                                      // variable is destroyed
  number_unwritten_total += p.num;
}

static std::mutex num_rwdp_m;
// number of bytes allocated for varstr
static long long num_rd_trans_total[5] = {0,0,0,0,0};
static long long num_rd_drcache_total[5] = {0,0,0,0,0};
static long long num_rd_pmem_total[5] = {0,0,0,0,0};
static long long num_wr_trans_total[5] = {0,0,0,0,0};
static long long num_wr_drcache_total[5] = {0,0,0,0,0};
static long long num_wr_pmem_total[5] = {0,0,0,0,0};
template <> void OnProbe(felis::probes::NumReadWriteDramPmem p) {
  std::lock_guard _(num_rwdp_m); // released automatically when lockguard
                                 // variable is destroyed
  if (p.access_type == 0){
    if (p.mem_type == 0){
      num_rd_trans_total[p.phase_type]++;
    }
    else if (p.mem_type == 1){
      num_rd_drcache_total[p.phase_type]++;
    }
    else if (p.mem_type == 2){
      num_rd_pmem_total[p.phase_type]++;
    }
  }
  else if (p.access_type == 1){
    if (p.mem_type == 0){
      num_wr_trans_total[p.phase_type]++;
    }
    else if (p.mem_type == 1){
      num_wr_drcache_total[p.phase_type]++;
    }
    else if (p.mem_type == 2){
      num_wr_pmem_total[p.phase_type]++;
    }
  }
}

static std::mutex row_size_m;
// number of bytes allocated for varstr

static uint64_t row_size[8388607] = {0};
static uint64_t row_size_total[5000000] = {0};
template <> void OnProbe(felis::probes::RowSize p) {
  std::lock_guard _(row_size_m); // released automatically when lockguard
                                      // variable is destroyed
  for (int i = 0; i < 10; i++){
    row_size[p.keys[i]]++;
  }
  
}

ProbeMain::~ProbeMain()
{
  std::cout << std::endl;
  std::cout << "total size for (masstree) indexes (MB): " << index_size_total/1024/1024 << std::endl;

  std::cout << "number of vhandles: " << number_vhandles_total << std::endl;
  std::cout << std::endl;

  std::cout << "number of unwritten DRAM caches: " << number_unwritten_total << std::endl;
  std::cout << std::endl;

  std::cout << "number of transient varstr: " << total_transient << std::endl;
  std::cout << "number of persistent varstr: " << total_persistent << std::endl;
  std::cout << std::endl;

  // std::cout << "NUMBER OF READS:" << std::endl;
  // std::cout << "insert: " << num_rd_trans_total[0] << " " << num_rd_drcache_total[0] << " " << num_rd_pmem_total[0] << std::endl;
  // std::cout << "append: " << num_rd_trans_total[1] << " " << num_rd_drcache_total[1] << " " << num_rd_pmem_total[1] << std::endl;
  // std::cout << "execute: " << num_rd_trans_total[2] << " " << num_rd_drcache_total[2] << " " << num_rd_pmem_total[2] << std::endl;
  // std::cout << "major GC: " << num_rd_trans_total[3] << " " << num_rd_drcache_total[3] << " " << num_rd_pmem_total[3] << std::endl;
  // std::cout << "dram GC: " << num_rd_trans_total[4] << " " << num_rd_drcache_total[4] << " " << num_rd_pmem_total[4] << std::endl;
  
  // std::cout << "NUMBER OF WRITES:" << std::endl;
  // std::cout << "insert: " << num_wr_trans_total[0] << " " << num_wr_drcache_total[0] << " " << num_wr_pmem_total[0] << std::endl;
  // std::cout << "append: " << num_wr_trans_total[1] << " " << num_wr_drcache_total[1] << " " << num_wr_pmem_total[1] << std::endl;
  // std::cout << "execute: " << num_wr_trans_total[2] << " " << num_wr_drcache_total[2] << " " << num_wr_pmem_total[2] << std::endl;
  // std::cout << "major GC: " << num_wr_trans_total[3] << " " << num_wr_drcache_total[3] << " " << num_wr_pmem_total[3] << std::endl;
  // std::cout << "dram GC: " << num_wr_trans_total[4] << " " << num_wr_drcache_total[4] << " " << num_wr_pmem_total[4] << std::endl;
  // std::cout << std::endl;

  // // calculate row size
  // std::cout << "ROW SIZES:" << std::endl;
  // for (unsigned int i = 0; i < 8388607; i++) {
  //   row_size_total[row_size[i] + 1]++;
  // }
  // for (unsigned int i = 0; i < 5000000; i++) {
  //   if (row_size_total[i]) {
  //     printf("%u\t%lu\n", i, row_size_total[i]);
  //   }
  // }
  // std::cout << std::endl;

  std::cout << "Total Bytes VarStr::New use_pmem=true: " << total_varstr_new_pmem_bytes 
            << " (max: " << max_varstr_new_pmem_bytes << ")" << std::endl;
  // std::cout << "Total Number VarStr::New use_pmem=true: " << total_varstr_new_pmem_number << std::endl;
  // std::cout << std::endl;

  // std::cout << "MOMO printing versionSizeArray" << std::endl;
  // for(int i = 0; i < 5002; i++) {
  //   if (version_size_array[i]) {
  //     std::cout << "MOMO versionSizeArray[" << i << "]:" << version_size_array[i] << std::endl;
  //   }
  // }
  // std::cout << "MOMO DONE printing versionSizeArray" << std::endl;
  // std::cout << std::endl;

  // std::cout << "START print version values sizes" << std::endl;
  // for(int i = 0; i < 162; i++) {
  //   if (version_value_size_array[i]) {
  //     std::cout << "version value of size[" << i << "]:" << version_value_size_array[i] << std::endl;
  //   }
  // }
  // std::cout << "DONE printing version value size" << std::endl;
  // std::cout << std::endl;

  std::cout << "Verison Alloc compare | Inline: " << countInlineAlloc << " , External: " << countExtAlloc << std::endl;
  std::cout << std::endl;

  // std::cout << "Total Amount of Memory Allocated through Transient Parallel Brk Pool : " << total_mem_allocated << std::endl;
  std::cout << "Max Transient Pool Size In An Epoch: " << max_mem_allocated_per_epoch << std::endl;

  // std::cout << "MOMO printing Memory Allocated per epoch" << std::endl;
  // for(int i = 0; i < 51; i++) {
  //   if (mem_alloc_parallel_brk_pool_per_epoch[i]) {
  //     std::cout << "MOMO mem_allocated_for_epoch[" << i << "]:" 
  //               << mem_alloc_parallel_brk_pool_per_epoch[i]
  //               << std::endl;
  //   }
  // }
  // std::cout << "MOMO DONE printing mem_allocated_for_epoch" << std::endl;
  // std::cout << std::endl;

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
