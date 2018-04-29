#include <fstream>
#include <iterator>
#include <algorithm>

#include "json11/json11.hpp"
#include "node_config.h"
#include "console.h"
#include "log.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

#include "promise.h"

namespace dolly {

size_t NodeConfiguration::kNrThreads = 8;

static const std::string kNodeConfiguration = "nodes.json";

static NodeConfiguration::NodePeerConfig ParseNodePeerConfig(json11::Json json, std::string name)
{
  NodeConfiguration::NodePeerConfig conf;
  auto &json_map = json.object_items().find(name)->second.object_items();
  conf.host = json_map.find("host")->second.string_value();
  conf.port = (uint16_t) json_map.find("port")->second.int_value();
  return conf;
}

static void ParseNodeConfig(util::Optional<NodeConfiguration::NodeConfig> &config, json11::Json json)
{
  config->worker_peer = ParseNodePeerConfig(json, "worker");
  config->web_conf = ParseNodePeerConfig(json, "web");
}

NodeConfiguration::NodeConfiguration()
{
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
    max_node_id = std::max((int) max_node_id, idx);
  }

  nr_clients = 0;
}

using go::TcpSocket;
using go::TcpInputChannel;
using go::BufferChannel;
using util::Instance;

class NodeServerThreadRoutine : public go::Routine {
  TcpInputChannel *in;
  int idx;
 public:
  NodeServerThreadRoutine(TcpSocket *client_sock, int idx)
      : in(client_sock->input_channel()), idx(idx)
  {}
  virtual void Run() final;
};

class NodeServerRoutine : public go::Routine {
 public:
  virtual void Run() final;
};

void NodeServerThreadRoutine::Run()
{
  int max_nr_thread = NodeConfiguration::kNrThreads;
  int cur_thread = 1;
  while (true) {
    while (true) {
      uint64_t promise_size = 0;
      in->Read(&promise_size, 8);

      if (promise_size == 0) {
        BasePromise::QueueRoutine(nullptr, idx, -1);
        break;
      }
      PromiseRoutinePool *pool = PromiseRoutinePool::Create(promise_size);
      in->Read(pool->mem, promise_size);

      auto r = PromiseRoutine::CreateFromBufferedPool(pool);
      BasePromise::QueueRoutine(r, idx, ++cur_thread);
      if (cur_thread == max_nr_thread) cur_thread = 1;
    }
  }
}

void NodeServerRoutine::Run()
{
  auto &console = util::Instance<Console>();

  auto server_sock = new TcpSocket(1024, 1024);
  auto &configuration = Instance<NodeConfiguration>();
  auto &node_conf = configuration.config();

  auto nr_nodes = configuration.nr_nodes();
  BasePromise::InitializeSourceCount(nr_nodes, configuration.kNrThreads);

  if (!server_sock->Bind("0.0.0.0", node_conf.worker_peer.port)) {
    std::abort();
  }
  if (!server_sock->Listen(NodeConfiguration::kMaxNrNode)) {
    std::abort();
  }
  console.UpdateServerStatus("listening");

  console.WaitForServerStatus("connecting");
  // Now if anybody else tries to connect to us, it should be in the listener
  // queue. We are safe to call connect at this point. It shouldn't lead to
  // deadlock.
  for (auto &config: configuration.all_config) {
    if (!config) continue;
    if (config->id == configuration.node_id()) continue;
    TcpSocket *remote_sock = new TcpSocket(1024, 1024);
    remote_sock->Connect(config->worker_peer.host, config->worker_peer.port);
    configuration.all_nodes[config->id] = remote_sock;
  }

  // Now we can begining to accept. Each client sock is a source for our Promise.
  // 0 is reserved for local source.
  //
  // The sources are different from nodes, and their orders are certainly
  // different from nodes too.
  for (size_t i = 1; i < nr_nodes; i++) {
    TcpSocket *client_sock = server_sock->Accept();
    if (client_sock == nullptr) continue;
    logger->info("New Connection");
    configuration.nr_clients++;
    go::Scheduler::Current()->WakeUp(new NodeServerThreadRoutine(client_sock, i));
  }
  console.UpdateServerStatus("ready");
}

void RunConsoleServer(std::string netmask, std::string service_port);

void NodeConfiguration::RunAllServers()
{
  RunConsoleServer("0.0.0.0", std::to_string(config().web_conf.port));
  logger->info("Starting node server with id {}", node_id());
  go::GetSchedulerFromPool(1)->WakeUp(new NodeServerRoutine());
}

go::TcpOutputChannel *NodeConfiguration::GetOutputChannel(int node_id)
{
  auto sock = all_nodes[node_id];
  if (!sock) {
    logger->critical("node {} does not exist!", node_id);
    std::abort();
  }
  return sock->output_channel();
}

void NodeConfiguration::TransportPromiseRoutine(PromiseRoutine *routine)
{
  uint64_t buffer_size = routine->TreeSize();
  uint8_t *buffer = (uint8_t *) malloc(buffer_size);
  routine->EncodeTree(buffer);
  auto out = GetOutputChannel(routine->node_id);
  out->Write(&buffer_size, 8);
  out->Write(buffer, buffer_size);
}

void NodeConfiguration::SendBarrier(int node_id)
{
  auto out = GetOutputChannel(node_id);
  uint64_t eop = 0;
  out->Write(&eop, 8);
}

void NodeConfiguration::BroadcastBarrier()
{
  for (auto &config: all_config) {
    if (!config) continue;
    SendBarrier(config->id);
  }
}

}
