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

class TransportImpl;
class SendChannel;

// TODO: Clean these constant up. They are all over the place.
static constexpr size_t kMaxNrNode = 254;

class TransportBatchMetadata {
 public:
  // Thread local information
  class LocalMetadata {
    friend class TransportBatchMetadata;
    // How many pieces should we expect on this core?
    std::atomic_ulong expect;
    unsigned long finish;
    // For each level, we had been accumulating increment to global counters.
    std::array<unsigned long, kMaxNrNode> delta; // for each destinations

    LocalMetadata() {}

    void Init(int nr_nodes) { Reset(nr_nodes); }
    void Reset(int nr_nodes) {
      expect = 0;
      finish = 0;
      std::fill(delta.begin(), delta.begin() + nr_nodes, 0);
    }
   public:
    void IncrementExpected(unsigned long d = 1) { expect.fetch_add(d); }
    bool Finish() { return (++finish) == expect.load(); }
    void AddRoute(int dst) { delta[dst - 1]++; }
  };
 private:
  static constexpr auto kMaxLevels = PromiseRoutineTransportService::kPromiseMaxLevels;

  // Given a level, how many pieces we should see for each destination node?
  std::array<std::array<std::atomic_ulong, kMaxNrNode>, kMaxLevels> counters;
  std::array<LocalMetadata *, 32> thread_local_data;
  friend class NodeConfiguration;
 public:
  TransportBatchMetadata() {}

  void Init(int nr_nodes, int nr_cores);
  void Reset(int nr_nodes, int nr_cores);
  LocalMetadata &GetLocalData(int level, int core) { return thread_local_data[core][level]; }
  unsigned long Merge(int level, LocalMetadata &local, int node);
};

class NodeConfiguration : public PromiseRoutineTransportService {
  NodeConfiguration();

  template <typename T> friend struct util::InstanceInit;

  int id;
  // Round Robin for local transport
  TransportImpl *lb;
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

  void ResetBufferPlan();
  void CollectBufferPlan(BasePromise *root, unsigned long *cnts);
  void FlushBufferPlan(unsigned long *per_core_cnts);
  void FlushBufferPlanCompletion(uint64_t epoch_nr);

  // node id starts from 1
  int nr_nodes() const { return max_node_id; }

  std::atomic_ulong *local_buffer_plan_counters() const {
    return local_batch->counters;
  };

  static constexpr size_t kMaxNrNode = 254;

 private:
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
  struct LocalBatch {
    unsigned long magic;
    unsigned long node_id;
    std::atomic_ulong counters[];
  } *local_batch;
  std::atomic_ulong local_batch_completed;

  TransportBatchMetadata transport_meta;
 private:
  void CollectBufferPlanImpl(PromiseRoutine *routine, unsigned long *cnts, int level, int src);
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
