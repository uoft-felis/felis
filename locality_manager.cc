#include <algorithm>
#include "gopp/gopp.h"
#include "locality_manager.h"
#include "node_config.h"
#include "vhandle.h"
#include "vhandle_batchappender.h"
#include "opts.h"
#include "felis_probes.h"

namespace felis {

LocalityManager::LocalityManager()
    : enable(Options::kLocalityManagement)
{
  auto wd_size = util::Align(
      sizeof(WeightDist) + sizeof(long) * NodeConfiguration::g_nr_threads, 64);
  for (int node = 0; node < NodeConfiguration::g_nr_threads / mem::kNrCorePerNode; node++) {
    auto p = (uint8_t *) mem::MemMapAlloc(
        mem::MemAllocType::Epoch, wd_size * mem::kNrCorePerNode, node);
    for (int i = 0; i < mem::kNrCorePerNode; i++) {
      per_core_weights[i + node * mem::kNrCorePerNode] =
          new (p + i * wd_size) WeightDist();
    }
  }
}

LocalityManager::~LocalityManager()
{
  for (int node = 0; node < NodeConfiguration::g_nr_threads / mem::kNrCorePerNode; node++) {
    // Free
  }
}

void LocalityManager::Reset()
{
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++)
    memset(per_core_weights[i], 0, sizeof(WeightDist));
}

void LocalityManager::Balance()
{
  if (!enable) return;
  long tot = 0;
  const auto tot_cores = NodeConfiguration::g_nr_threads;

  for (int core = 0; core < tot_cores; core++) {
    auto &w = (*per_core_weights[core]);

    for (int j = 0; j < tot_cores; j++) {
      w.weight += per_core_weights[j]->weights_per_core[core];
      per_core_weights[j]->weights_per_core[core] = 0;
    }

    w.load = w.weight;
    tot += w.weight;
  }
  long average = tot / tot_cores;
  size_t nr_prealloc = 0;
  for (int core = 0; core < tot_cores; core++) {
    auto &w = (*per_core_weights[core]);
    w.dist = dist_prealloc + nr_prealloc;
    w.cores = core_prealloc + nr_prealloc;
    auto weight = w.weight;
    w.dist[0] = weight;
    w.cores[0] = core;
    w.nr_dist = 1;
    if (weight > average) {
      w.dist[0] = average;
      w.load = average;
      OffloadCore(core, w, average);
    }
    nr_prealloc += w.nr_dist;
    abort_if(nr_prealloc > kPrealloc, "{}<{} is too small inside Locality Manager",
             kPrealloc, nr_prealloc)
  }
}

void LocalityManager::OffloadCore(int core, WeightDist &w, long limit)
{
  const auto tot_cores = NodeConfiguration::g_nr_threads;
  int walk_cores[tot_cores];
  int nr_walk = 0;
  auto numa_node = core / mem::kNrCorePerNode;
  for (int i = numa_node * mem::kNrCorePerNode;
       i < (numa_node + 1) * mem::kNrCorePerNode && i < tot_cores;
       i++) {
    if (i == core) continue;
    if (per_core_weights[i]->load >= limit) continue;
    walk_cores[nr_walk++] = i;
  }
  for (int i = 0; i < tot_cores; i++) {
    if (i / mem::kNrCorePerNode == numa_node) continue;
    if (per_core_weights[i]->load >= limit) continue;
    walk_cores[nr_walk++] = i;
  }

  auto weight = w.weight;
  long load_cdf = limit;
  while (load_cdf < weight) {
    int d = w.nr_dist - 1;
    if (d >= nr_walk) break;

    auto cur = walk_cores[d];

    auto delta = std::min(weight - load_cdf, limit - per_core_weights[cur]->load);
    load_cdf += delta;
    w.cores[d + 1] = cur;
    w.dist[d + 1] = load_cdf;
    per_core_weights[cur]->load += delta;
    w.nr_dist++;
  }
}

void LocalityManager::PrintLoads()
{
  if (!enable) return;
  fmt::memory_buffer buffer;
  for (int core = 0; core < NodeConfiguration::g_nr_threads; core++) {
    auto &w = (*per_core_weights[core]);
    fmt::format_to(buffer, "core {} weight {} load {} cdf: ", core, w.weight, w.load);
    for (int d = 0; d < w.nr_dist; d++) {
      fmt::format_to(buffer, "{} on {} ", w.dist[d], w.cores[d]);
    }
    if (w.weight > w.dist[w.nr_dist - 1]) fmt::format_to(buffer, "*");
    fmt::format_to(buffer, "\n");
  }
  logger->info("Loads information: \n{}", std::string_view(buffer.data(), buffer.size()));
}

void LocalityManager::PlanLoad(int core, long delta)
{
  if (!enable) return;

  int current_core = go::Scheduler::CurrentThreadPoolId() - 1;
  auto &w = (*per_core_weights[current_core]);
  w.weights_per_core[core] += delta;
}

static thread_local util::XORRandom64 local_rand;

uint64_t LocalityManager::GetScheduleCore(int core, int weight)
{
  if (!enable) return std::numeric_limits<uint64_t>::max();

  auto &w = (*per_core_weights[core]);
  uint64_t sched_core = 0;
  uint64_t seed = 0, max_seed = w.dist[w.nr_dist - 1];
  if (w.nr_dist == 1) {
    sched_core = w.cores[0];
  } else {
    seed = local_rand.Next() % w.dist[w.nr_dist - 1];
    auto it = std::upper_bound(w.dist, w.dist + w.nr_dist, seed);
    sched_core = w.cores[it - w.dist];
  }
  probes::LocalitySchedule{core, weight, sched_core, seed, max_seed, w.load}();
  return sched_core;
}

VHandle *LocalityManager::SelectRow(uint64_t bitmap, VHandle *const *it)
{
  if (bitmap == 0)
    return nullptr;
  auto nrbits = __builtin_popcountll(bitmap);
  auto sel = local_rand.Next() % nrbits;
  do {
    int skip = __builtin_ctzll(bitmap);
    it += skip;
    if (sel-- == 0) {
      return *it;
    }
    ++it;
    bitmap >>= skip + 1;
  } while (bitmap != 0);
  std::abort();
}

}
