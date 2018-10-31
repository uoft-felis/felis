#ifndef NODE_CONFIG_H
#define NODE_CONFIG_H

#include <string>
#include <vector>
#include <array>
#include <atomic>
#include "util.h"
#include "gopp/channels.h"
#include "promise.h"

namespace felis {

class NodeServerRoutine;
class PromiseRoutine;

class PromiseRoundRobin {
  std::atomic_ulong cur_thread = 1;
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
  static constexpr size_t kMaxNrThreads = 32;

  struct NodePeerConfig {
    std::string host;
    uint16_t port;
  };

  struct NodeConfig {
    int id;
    std::string name;
    NodePeerConfig worker_peer;
  };

  int node_id() const { return id; }
  void SetupNodeName(std::string name);

  const NodeConfig &config() const {
    if (!all_config[id])
      std::abort();
    return all_config[id].value();
  }

  void RunAllServers();
  void TransportPromiseRoutine(PromiseRoutine *routine) final override;
  void SendBarrier(int node_id);
  void BroadcastBarrier();

  // node id starts from 1
  int nr_nodes() const { return max_node_id; }

  static constexpr size_t kMaxNrNode = 1024;
 private:
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
