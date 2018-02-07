#include <string>
#include <vector>
#include <array>
#include "util.h"

namespace dolly {

class NodeConfiguration {
  NodeConfiguration();

  static NodeConfiguration *instance;
  template <typename T> friend T &util::Instance();

  int id;
 public:

  struct NodeCoreConfig {
    std::string host;
    uint16_t port;
  };

  struct NodeConfig {
    int id;
    std::vector<NodeCoreConfig> cores;
  };

  int node_id() const { return id; }
 private:
  static constexpr size_t kMaxNrNode = 1024;
  std::array<NodeConfig, kMaxNrNode> all_config;
};

}
