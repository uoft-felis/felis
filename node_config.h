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
class PromiseRoutine;

class PromiseRoundRobin {
  std::atomic_ulong cur_thread = 0;
 public:
  void QueueRoutine(PromiseRoutine *routine, int idx);
};

class NodeConfiguration : public PromiseRoutineTransportService {
  NodeConfiguration();

  template <typename T> friend T &util::Instance();

  int id;
  // Round Robin for local transport
  PromiseRoundRobin lb;
 public:

  static size_t kNrThreads;
  static int kCoreShifting; // Starting to use from which core. Useful for debugging on a single node.
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
  void TransportPromiseRoutine(PromiseRoutine *routine) final override;
  void SendBarrier(int node_id);
  void BroadcastBarrier();

  // node id starts from 1
  int nr_nodes() const { return max_node_id; }

  static constexpr size_t kMaxNrNode = 1024;
 private:
  static void RunIndexShipmentReceiverThread(std::string host, unsigned short port);

  go::TcpOutputChannel *GetOutputChannel(int node_id);

  size_t nr_clients;

 private:
  friend class NodeServerRoutine;
  std::array<util::Optional<NodeConfig>, kMaxNrNode> all_config;
  std::array<go::TcpSocket *, kMaxNrNode> all_nodes;
  size_t max_node_id;
};

}

#endif /* NODE_CONFIG_H */
