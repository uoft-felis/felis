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

template <typename T> class FlushImpl;
class PromiseRoundRobinImpl;
class SendChannelImpl;

using PromiseRoundRobin = FlushImpl<PromiseRoundRobinImpl>;
using SendChannel = FlushImpl<SendChannelImpl>;

class NodeConfiguration : public PromiseRoutineTransportService {
  NodeConfiguration();

  template <typename T> friend T &util::Instance() noexcept;

  int id;
  // Round Robin for local transport
  PromiseRoundRobin *lb;
 public:

  static size_t g_nr_threads;
  static int g_core_shifting; // Starting to use from which core. Useful for debugging on a single node.
  static constexpr size_t kMaxNrThreads = 32;

  struct NodePeerConfig {
    std::string host;
    uint16_t port;
  };

  struct NodeConfig {
    int id;
    std::string name;
    NodePeerConfig worker_peer;
    NodePeerConfig index_shipper_peer;
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
  void FlushPromiseRoutine() final override;
  long IOPending(int core_id) final override;
  void IncrementExtraIOPending(int core_id) { extra_iopendings[core_id]++; }
  void DecrementExtraIOPending(int core_id) { extra_iopendings[core_id]--; }

  void ResetBufferPlan();
  void CollectBufferPlan(BasePromise *root);
  void FlushBufferPlan(bool sync);
  void FlushBufferPlanCompletion(uint64_t epoch_nr);

  void SendBarrier(int node_id);
  void BroadcastBarrier();

  // node id starts from 1
  int nr_nodes() const { return max_node_id; }

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

  static constexpr size_t kPromiseMaxLevels = 16;
  static constexpr size_t kPromiseMaxDebugLevels = 3;
  static constexpr ulong kPromiseBarrierWatermark = 1 << 20;
  // The BufferRootPromise is going to run an analysis on the root promise to
  // keep track of how many handlers needs to be sent.
  //
  // The counters should be in the format of
  // channel_batch_counters[level][src][dst], where src and dst are the node
  // number - 1.
  std::atomic_ulong *total_batch_counters;
  // How many we have issued at this level? If we are catching up, then flush.
  std::atomic_ulong *batch_counters[kPromiseMaxLevels];
  ulong *local_batch_counters;

  // Transactions can execute without having to wait for all IOs to
  // complete. However, this is very likely to introduce deadlocks.
  //
  // We introduce IOPendings to tell the transactions to yield to IOs instead of
  // dedicately spin there.
  //
  // IOPendings automatically take account into currently in-flight flushes,
  // current IO routine running on this core. However, there are some other
  // types of IOs we need to keep track of: for example, unfinished issuers.
  //
  ulong extra_iopendings[kMaxNrThreads];
 private:
  void CollectBufferPlanImpl(PromiseRoutine *routine, int level, int src, int nr_extra);
  size_t BatchBufferIndex(int level, int src_node, int dst_node);
};

}

#endif /* NODE_CONFIG_H */
