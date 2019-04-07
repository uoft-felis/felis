#ifndef NODE_CONFIG_H
#define NODE_CONFIG_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include "util.h"
#include "log.h"
#include "gopp/channels.h"
#include "promise.h"

namespace felis {

class NodeServerRoutine;
class NodeServerThreadRoutine;
struct PromiseRoutine;

class PromiseRoundRobin;
class SendChannel;

class TransportBatchMetadata {
 public:
  // Thread local information
  class LocalMetadata {
    friend class TransportBatchMetadata;
    // How many pieces should we expect on this core?
    std::atomic_ulong expect;
    unsigned long finish;
    // For each level, we had been accumulating increment to global counters.
    unsigned long *delta; // for each destinations

    LocalMetadata() {}

    void Init(int nr_nodes) {
      delta = new unsigned long[nr_nodes];
      Reset(nr_nodes);
    }

    void Reset(int nr_nodes) {
      expect = 0;
      finish = 0;
      memset(delta, 0, sizeof(unsigned long) * nr_nodes);
    }
   public:
    void IncrementExpected(unsigned long d = 1) { expect.fetch_add(d); }
    bool Finish() { return (++finish) == expect.load(); }
    void AddRoute(int dst) { delta[dst - 1]++; }
    // unsigned long RouteCount(int dst) { return delta[dst - 1]; }
  };
 private:
  static constexpr auto kMaxLevels = PromiseRoutineTransportService::kPromiseMaxLevels;

  // Given a level, how many pieces we should see for each destination node?
  std::array<std::atomic_ulong *, kMaxLevels> counters;
  std::array<LocalMetadata *, kMaxLevels> thread_local_data;
  friend class NodeConfiguration;
 public:
  TransportBatchMetadata() {
    counters.fill(nullptr);
    thread_local_data.fill(nullptr);
  }

  void Init(int nr_nodes, int nr_cores) {
    for (auto &p: thread_local_data) {
      p = new LocalMetadata[nr_cores];
      for (auto i = 0; i < nr_cores; i++) p[i].Init(nr_nodes);
    }
    for (auto &p: counters) {
      p = new std::atomic_ulong[nr_nodes];
    }
  }

  void Reset(int nr_nodes, int nr_cores) {
    for (auto p: thread_local_data) {
      for (auto i = 0; i < nr_cores; i++) p[i].Reset(nr_nodes);
    }
    for (auto p: counters) {
      for (auto i = 0; i < nr_nodes; i++) p[i] = 0;
    }
  }

  LocalMetadata &GetLocalData(int level, int core) { return thread_local_data[level][core]; }

  unsigned long Merge(int level, LocalMetadata &local, int node) {
    auto v = local.delta[node - 1];
    local.delta[node - 1] = 0;
    return counters[level][node - 1].fetch_add(v) + v;
  }
};

class NodeConfiguration : public PromiseRoutineTransportService {
  NodeConfiguration();

  template <typename T> friend struct util::InstanceInit;

  int id;
  // Round Robin for local transport
  PromiseRoundRobin *lb;
 public:

  static size_t g_nr_threads;
  static int g_core_shifting; // Starting to use from which core. Useful for debugging on a single node.
  static constexpr size_t kMaxNrThreads = 32;
  static bool g_data_migration;

  struct NodePeerConfig {
    std::string host;
    uint16_t port;
  };

  struct NodeConfig {
    int id;
    std::string name;
    NodePeerConfig worker_peer;
    NodePeerConfig index_shipper_peer;
    NodePeerConfig row_shipper_peer;
  };

  int node_id() const { return id; }
  void SetupNodeName(std::string name);

  const NodeConfig &config(int idx = -1) const {
    if (idx == -1) idx = id;
    abort_if(!all_config[idx],
             "configuration for node {} does not exist!", idx);
    return all_config[idx].value();
  }

  void RunAllServers();

  void TransportPromiseRoutine(PromiseRoutine *routine, const VarStr &in) final override;
  void PreparePromisesToQueue(int core, int level, unsigned long nr) final override;
  void FinishPromiseFromQueue(PromiseRoutine *routine) final override;
  void ForceFlushPromiseRoutine() final override;
  long UrgencyCount(int core_id) final override { return urgency_cnt[core_id]; }
  void IncrementUrgencyCount(int core_id, long delta = 1) {
    urgency_cnt[core_id] += delta;
  }
  void DecrementUrgencyCount(int core_id, long delta = 1) {
    urgency_cnt[core_id] -= delta;
  }

  void ResetBufferPlan();
  void CollectBufferPlan(BasePromise *root, unsigned long *cnts);
  void FlushBufferPlan(bool sync);
  void FlushBufferPlanCompletion(uint64_t epoch_nr);

  void SendBarrier(int node_id);
  void BroadcastBarrier();

  // node id starts from 1
  int nr_nodes() const { return max_node_id; }

  unsigned long *local_buffer_plan_counters() const {
    return local_batch_counters + 2;
  };

  static constexpr size_t kMaxNrNode = 1024;
 private:
  static void RunIndexShipmentReceiverThread(std::string host, unsigned short port);

  SendChannel *GetOutputChannel(int node_id);

  std::vector<go::TcpSocket *> clients;

 private:
  friend class NodeServerRoutine;
  friend class NodeServerThreadRoutine;
  std::array<util::Optional<NodeConfig>, kMaxNrNode> all_config;
  std::array<go::TcpSocket *, kMaxNrNode> all_nodes;
  std::array<NodeServerThreadRoutine *, kMaxNrNode> all_in_routines;
  std::array<SendChannel *, kMaxNrNode> all_out_channels;
  size_t max_node_id;

  static constexpr unsigned long kPromiseBarrierWatermark = 1 << 20;
  // The BufferRootPromise is going to run an analysis on the root promise to
  // keep track of how many handlers needs to be sent.
  //
  // The counters should be in the format of
  // channel_batch_counters[level][src][dst], where src and dst are the node
  // number - 1.
  std::atomic_ulong *total_batch_counters;
  unsigned long *local_batch_counters;

  TransportBatchMetadata transport_meta;

  unsigned long urgency_cnt[kMaxNrThreads];
 private:
  void CollectBufferPlanImpl(PromiseRoutine *routine, unsigned long *cnts, int level, int src, int nr_extra);
  size_t BatchBufferIndex(int level, int src_node, int dst_node);
};

}

namespace util {

template <>
struct InstanceInit<felis::NodeConfiguration> {
  static constexpr bool kHasInstance = true;
  static felis::NodeConfiguration *instance;

  InstanceInit() {
    instance = new felis::NodeConfiguration();
  }
};

}

#endif /* NODE_CONFIG_H */
