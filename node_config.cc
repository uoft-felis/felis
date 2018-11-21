#include <fstream>
#include <iterator>
#include <algorithm>
#include <sys/types.h>
#include <sys/socket.h>

#include "json11/json11.hpp"
#include "node_config.h"
#include "console.h"
#include "log.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"
#include "index_common.h"

#include "promise.h"

namespace felis {

size_t NodeConfiguration::g_nr_threads = 8;
int NodeConfiguration::g_core_shifting = 0;

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
  config->index_shipper_peer = ParseNodePeerConfig(json, "index_shipper");
  config->name = json.object_items().find("name")->second.string_value();
}

NodeConfiguration::NodeConfiguration()
{
  auto &console = util::Instance<Console>();

  console.WaitForServerStatus(Console::ServerStatus::Configuring);
  json11::Json conf_doc = console.FindConfigSection("nodes");

  auto hosts_conf = conf_doc.array_items();

  for (int i = 0; i < hosts_conf.size(); i++) {
    int idx = i + 1;
    all_config[idx] = NodeConfig();
    auto &config = all_config[idx];
    config->id = idx;
    ParseNodeConfig(config, hosts_conf[i]);
    max_node_id = std::max((int) max_node_id, idx);
  }

  nr_clients = 0;
}

using go::TcpSocket;
using go::TcpInputChannel;
using go::BufferChannel;
using util::Instance;

void PromiseRoundRobin::QueueRoutine(PromiseRoutine *routine, int idx)
{
  auto routine_thread_id = cur_thread.fetch_add(1) % NodeConfiguration::g_nr_threads + 1;
  BasePromise::QueueRoutine(routine, idx, routine_thread_id);
}

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
  PromiseRoundRobin lb;
  while (true) {
    while (true) {
      uint64_t promise_size = 0;
      in->Read(&promise_size, 8);

      if (promise_size == 0) {
        BasePromise::QueueRoutine(nullptr, idx, -1);
        break;
      }
      // TODO: Tune for latency: we can force yield to the next routine, since
      // we are on thread 0 anyway.
      auto pool = PromiseRoutinePool::Create(promise_size);
      in->Read(pool->mem, promise_size);

      auto r = PromiseRoutine::CreateFromBufferedPool(pool);
      // printf("received a remote routine, put it in the queue\n");
      lb.QueueRoutine(r, idx);
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
  BasePromise::InitializeSourceCount(nr_nodes, configuration.g_nr_threads);

  // Reuse addr just for debugging
  int enable = 1;
  setsockopt(server_sock->fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

  abort_if(!server_sock->Bind("0.0.0.0", node_conf.worker_peer.port),
           "Cannot bind peer address");
  abort_if(!server_sock->Listen(NodeConfiguration::kMaxNrNode),
           "Cannot listen");
  console.UpdateServerStatus(Console::ServerStatus::Listening);

  console.WaitForServerStatus(Console::ServerStatus::Connecting);
  // Now if anybody else tries to connect to us, it should be in the listener
  // queue. We are safe to call connect at this point. It shouldn't lead to
  // deadlock.
  for (auto &config: configuration.all_config) {
    if (!config) continue;
    if (config->id == configuration.node_id()) continue;
    logger->info("Connecting worker peer on node {}\n", config->id);
    TcpSocket *remote_sock = new TcpSocket(1024, 1024);
    auto &peer = config->worker_peer;
    bool rs = remote_sock->Connect(peer.host, peer.port);
    abort_if(!rs, "Cannot connect to {}:{}", peer.host, peer.port);
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
    logger->info("New worker peer connection");
    configuration.nr_clients++;
    go::Scheduler::Current()->WakeUp(new NodeServerThreadRoutine(client_sock, i));
  }
  console.UpdateServerStatus(Console::ServerStatus::Running);
}

void NodeConfiguration::SetupNodeName(std::string name)
{
  for (int i = 1; i <= max_node_id; i++) {
    if (all_config[i] && all_config[i]->name == name) {
      id = i;
      return;
    }
  }
}

class NodeIndexShipmentReceiverRoutine : public go::Routine {
  std::string host;
  unsigned short port;
 public:
  NodeIndexShipmentReceiverRoutine(std::string host, unsigned short port) : host(host), port(port) {}

  void Run() final override;
};

void NodeIndexShipmentReceiverRoutine::Run()
{
  go::TcpSocket *server = new go::TcpSocket(8192, 1024);
  server->Bind(host, port);
  server->Listen();

  while (true) {
    auto *client_sock = server->Accept();
    auto receiver = new IndexShipmentReceiver(client_sock);
    go::Scheduler::Current()->WakeUp(receiver);
  }
}

void NodeConfiguration::RunAllServers()
{
  logger->info("Starting system thread for index shipment");
  auto &peer = config().index_shipper_peer;
  go::GetSchedulerFromPool(g_nr_threads + 1)->WakeUp(
      new NodeIndexShipmentReceiverRoutine(peer.host, peer.port));

  logger->info("Starting node server with id {}", node_id());
  go::GetSchedulerFromPool(0)->WakeUp(new NodeServerRoutine());
}

go::TcpOutputChannel *NodeConfiguration::GetOutputChannel(int node_id)
{
  auto sock = all_nodes[node_id];
  abort_if(!sock, "node with id {} does not exist!", node_id);
  return sock->output_channel();
}

void NodeConfiguration::TransportPromiseRoutine(PromiseRoutine *routine)
{
  if (routine->node_id != 0 && routine->node_id != node_id()) {
    uint64_t buffer_size = routine->TreeSize();
    uint8_t *buffer = (uint8_t *) malloc(8 + buffer_size);

    memcpy(buffer, &buffer_size, 8);
    routine->EncodeTree(buffer + 8);
    auto out = GetOutputChannel(routine->node_id);

    /*
     * This stream is shared among all cores.
     * Thankfully, our Write() is atomic.
     */
    out->Write(buffer, buffer_size + 8);
    out->Flush();

    free(buffer);
    routine->input.data = nullptr;
    routine->UnRefRecursively();
  } else {
    auto &in = routine->input;
    uint8_t *p = (uint8_t *) malloc(in.len);
    memcpy(p, in.data, in.len);
    routine->input = VarStr(in.len, in.region_id, p);

    lb.QueueRoutine(routine, 0);
  }
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
