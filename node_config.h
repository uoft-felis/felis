#include <string>
#include <vector>
#include <array>
#include "util.h"
#include "gopp/channels.h"

namespace dolly {

class NodeServerRoutine;
class PromiseRoutine;

class NodeConfiguration {
  NodeConfiguration();

  static NodeConfiguration *instance;
  template <typename T> friend T &util::Instance();

  int id;
 public:

  struct NodePeerConfig {
    std::string host;
    uint16_t port;
  };

  struct NodeConfig {
    int id;
    NodePeerConfig worker_peer;
  };

  int node_id() const { return id; }
  const NodeConfig &config() const {
    if (!all_config[id])
      std::abort();
    return all_config[id].value();
  }

  void RunNodeServer();
  void TransportPromiseRoutine(PromiseRoutine *routine);

  static constexpr size_t kMaxNrNode = 1024;
 private:
  friend class NodeServerRoutine;
  std::array<util::Optional<NodeConfig>, kMaxNrNode> all_config;
  std::array<go::TcpSocket *, kMaxNrNode> all_nodes;
  size_t max_node_id;
};

}
