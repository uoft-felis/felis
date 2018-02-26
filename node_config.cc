#include <fstream>
#include <iterator>
#include <algorithm>
#include "json11/json11.hpp"
#include "node_config.h"
#include "log.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

namespace dolly {

NodeConfiguration *NodeConfiguration::instance = nullptr;

static const std::string kNodeConfiguration = "nodes.json";

static void ParseNodeConfig(util::Optional<NodeConfiguration::NodeConfig> &config, json11::Json json)
{
  // TOOD: parse the epoch manager settings too!
  auto &json_map = json.object_items().find("worker")->second.object_items();

  config->worker_peer.host = json_map.find("host")->second.string_value();
  config->worker_peer.port = (uint16_t) json_map.find("port")->second.int_value();
}

NodeConfiguration::NodeConfiguration()
{
  const char *strid = getenv("DOLLY_NODE_ID");

  if (!strid || (id = std::stoi(std::string(strid))) <= 0) {
    fprintf(stderr, "Must specify DOLLY_NODE_ID\n");
    std::abort();
  }

  std::ifstream fin(kNodeConfiguration);
  std::string conf_text{std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>()};
  std::string err;
  json11::Json conf_doc = json11::Json::parse(conf_text, err);

  if (!err.empty()) {
    fputs("Failed to parse node configuration", stderr);
    fputs(err.c_str(), stderr);
    std::abort();
  }

  auto json_map = conf_doc.object_items();
  for (auto it = json_map.begin(); it != json_map.end(); ++it) {
    int idx = std::stoi(it->first);
    all_config[idx] = NodeConfig();
    auto &config = all_config[idx];
    config->id = idx;
    ParseNodeConfig(config, it->second);
    max_node_id = std::max(max_node_id, idx);
  }
}

using go::TcpSocket;
using util::Instance;

class NodeServerRoutine : public go::Routine {
 public:
  virtual void Run() final {
    auto server_sock = new TcpSocket(1024, 1024);
    auto &configuration = Instance<NodeConfiguration>();
    auto &node_conf = configuration.config();
    if (!server_sock->Bind(node_conf.worker_peer.host, node_conf.worker_peer.port)) {
      std::abort();
    }
    if (!server_sock->Listen(NodeConfiguration::kMaxNrNode)) {
      std::abort();
    }
    // Now if anybody else tries to connect to us, it should be in the listener
    // queue. We are safe to call connect at this point. It shouldn't lead to
    // deadlock.


  }
};

void NodeConfiguration::RunNodeServer()
{
  logger->info("Starting node server with id {}", node_id());
}

}
