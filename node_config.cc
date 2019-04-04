#include <fstream>
#include <iterator>
#include <algorithm>
#include <sys/types.h>
#include <sys/socket.h>

#include "json11/json11.hpp"
#include "node_config.h"
#include "console.h"
#include "log.h"
#include "epoch.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"
#include "index_common.h"

#include "promise.h"
#include "slice.h"

namespace felis {

template <typename T>
class Flushable {
 protected:

  static constexpr auto kThreadBitmapSize =
      NodeConfiguration::kMaxNrThreads / 64 + 1;

  static inline void ThreadBitmapInit(uint64_t *bitmap) {
    memset(bitmap, 0, sizeof(uint64_t) * kThreadBitmapSize);
  }

  static inline void ThreadBitmapMark(uint64_t *bitmap, int idx) {
    uint64_t mask = 1 << (idx % 64);
    bitmap[idx / 64] |= mask;
  }

  static inline bool ThreadBitmapIsMarked(uint64_t *bitmap, int idx) {
    uint64_t mask = 1 << (idx % 64);
    return (bitmap[idx / 64] & mask) != 0;
  }

 private:
  T *self() { return (T *) this; }

 public:

  void Flush() {
    uint64_t flushed[kThreadBitmapSize];
    bool need_do_flush = false;
    size_t nr_flushed = 0;
    // Also flush the main go-routine
    auto nr_threads = NodeConfiguration::g_nr_threads + 1;
    ThreadBitmapInit(flushed);

    while (nr_flushed < nr_threads) {
      for (int i = 0; i < nr_threads; i++) {
        if (!ThreadBitmapIsMarked(flushed, i)
            && self()->TryLock(i)) {
          auto [start, end] = self()->GetFlushRange(i);
          self()->UpdateFlushStart(i, end);

          if (self()->PushRelease(i, start, end)) {
            need_do_flush = true;
          }
          ThreadBitmapMark(flushed, i);
          nr_flushed++;
        }
      }
    }
    if (need_do_flush)
      self()->DoFlush();
  }
};

class PromiseRoundRobin : public Flushable<PromiseRoundRobin> {
  struct Queue {
    PromiseRoutineWithInput *routines;
    std::atomic_bool lock;
    unsigned int flusher_start;
    std::atomic_uint append_start;
  };

  util::CacheAligned<Queue> queues[NodeConfiguration::kMaxNrThreads + 1];
  int idx;
  std::atomic_ulong round;
  static constexpr size_t kBufferSize = 16384;
 public:
  PromiseRoundRobin(int idx);
  void QueueRoutine(PromiseRoutine *routine, const VarStr &in);
  void QueueBubble();

  std::tuple<uint, uint> GetFlushRange(int tid);
  void UpdateFlushStart(int tid, unsigned int flusher_start);
  bool PushRelease(int thr, unsigned int start, unsigned int end);
  void DoFlush() {
    BasePromise::FlushScheduler();
  }
  bool TryLock(int i) {
    bool locked = false;
    return queues[i]->lock.compare_exchange_strong(locked, true);
  }
  void Unlock(int i) {
    queues[i]->lock.store(false);
  }
};

class SendChannel : public Flushable<SendChannel> {
  go::TcpOutputChannel *out;
  go::BufferChannel *flusher_channel;

  struct Channel {
    uint8_t *mem;
    unsigned int flusher_start;
    std::atomic_uint append_start;
    std::atomic_bool lock;
    std::atomic_bool dirty;
  };

  // We need to create a per-thread, long-running flusher go::Routine.
  class FlusherRoutine : public go::Routine {
    go::BufferChannel *flusher_channel;
    go::TcpOutputChannel *out;
   public:
    FlusherRoutine(go::BufferChannel *chn, go::TcpOutputChannel *out)
        : flusher_channel(chn), out(out) {
      // set_urgent(true);
    }
    void Run() final override;
  };

  util::CacheAligned<Channel> channels[NodeConfiguration::kMaxNrThreads + 1];

 public:
  static constexpr size_t kPerThreadBuffer = 16 << 10;
  SendChannel(go::TcpSocket *sock);
  void *Alloc(size_t sz);
  void Finish(size_t sz);
  long PendingFlush(int core_id);

  std::tuple<uint, uint> GetFlushRange(int thr);
  void UpdateFlushStart(int thr, uint flush_start);
  bool PushRelease(int thr, unsigned int start, unsigned int end);
  void DoFlush();
  bool TryLock(int i) {
    bool locked = false;
    return channels[i]->lock.compare_exchange_strong(locked, true);
  }
  void Unlock(int i) {
    channels[i]->lock.store(false);
  }
};

PromiseRoundRobin::PromiseRoundRobin(int idx)
    : idx(idx), round(0)
{
  for (int i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    auto &q = queues[i];
    q->lock = false;
    q->routines = new PromiseRoutineWithInput[kBufferSize];
    q->flusher_start = 0;
    q->append_start = 0;
  }
}

std::tuple<uint, uint> PromiseRoundRobin::GetFlushRange(int tid)
{
  auto &q = queues[tid];
  return {
    q->flusher_start, q->append_start.load(std::memory_order_acquire)
  };
}

void PromiseRoundRobin::UpdateFlushStart(int tid, unsigned int flusher_start)
{
  queues[tid]->flusher_start = flusher_start;
}

void PromiseRoundRobin::QueueBubble()
{
  // Currently we don't batch the bubbles, and we consider bubbles are rare.
  util::Impl<PromiseRoutineDispatchService>().AddBubble();
}

void PromiseRoundRobin::QueueRoutine(PromiseRoutine *routine, const VarStr &in)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  auto &q = queues[tid];
retry:
  auto end = q->append_start.load(std::memory_order_relaxed);
  if (end == kBufferSize) {
    while (!TryLock(tid)) __builtin_ia32_pause();

    auto start = q->flusher_start;
    q->append_start.store(0, std::memory_order_release);
    q->flusher_start = 0;
    PushRelease(tid, start, end);
    goto retry;
  }
  q->routines[end] = std::make_tuple(routine, in);
  q->append_start.store(end + 1, std::memory_order_release);
}

bool PromiseRoundRobin::PushRelease(int thr, unsigned int start, unsigned int end)
{
  auto nr_routines = end - start;
  if (nr_routines == 0) {
    Unlock(thr);
    return false;
  }

  PromiseRoutineWithInput routines[kBufferSize];
  int nr_threads = NodeConfiguration::g_nr_threads;
  // ulong delta = nr_threads - nr_routines % nr_threads;
  ulong delta = nr_routines % nr_threads;
  ulong rnd = round.fetch_add(delta);
  memcpy(routines, queues[thr]->routines + start, nr_routines * sizeof(PromiseRoutineWithInput));
  Unlock(thr);

#if 0
  // This is putting promise that's co-located together on the same thread.
  // It's not real roundrobin
  for (int i = 0; i < nr_threads; i++) {
    size_t start = i * nr_routines / nr_threads;
    size_t end = (i + 1) * nr_routines / nr_threads;

    if (end == start) continue;
    BasePromise::QueueRoutine(routines + start, end - start, idx,
                              (i + rnd) % nr_threads + 1,
                              false);
  }
#endif

  // True roundrobin
  PromiseRoutineWithInput rounds[kBufferSize / nr_threads + 1];
  auto &transport = util::Impl<PromiseRoutineTransportService>();
  for (int i = 0; i < nr_threads; i++) {
    int sz = 0;
    int core = (i + rnd) % nr_threads;
    std::array<unsigned long, NodeConfiguration::kPromiseMaxLevels> cnts;
    cnts.fill(0);
    for (int j = i; j < nr_routines; j += nr_threads) {
      auto [r, _] = routines[j];
      rounds[sz++] = routines[j];
      cnts[r->level]++;
    }
    for (auto i = 0; i < NodeConfiguration::kPromiseMaxLevels; i++) {
      if (cnts[i] == 0) continue;
      transport.PreparePromisesToQueue(core, i, cnts[i]);
    }
    if (sz == 0) continue;
    BasePromise::QueueRoutine(rounds, sz, idx, core + 1, false);
  }

  return true;
}

SendChannel::SendChannel(go::TcpSocket *sock)
    : out(sock->output_channel())
{
  auto buffer =
      (uint8_t *) malloc((NodeConfiguration::g_nr_threads + 1) * kPerThreadBuffer);
  for (int i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    auto &chn = channels[i];
    chn->mem = buffer + i * kPerThreadBuffer;
    chn->append_start = 0;
    chn->flusher_start = 0;
    chn->lock = false;
    chn->dirty = false;
  }
  flusher_channel = new go::BufferChannel(512);
  go::GetSchedulerFromPool(0)->WakeUp(new FlusherRoutine(flusher_channel, out));
}

std::tuple<uint, uint> SendChannel::GetFlushRange(int tid)
{
  auto &chn = channels[tid];
  return {
    chn->flusher_start, chn->append_start.load(std::memory_order_acquire),
  };
}

void SendChannel::UpdateFlushStart(int tid, uint flush_start)
{
  channels[tid]->flusher_start = flush_start;
}

void *SendChannel::Alloc(size_t sz)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  abort_if(tid < 0, "Have to call this within a go-routine");

  auto &chn = channels[tid];
retry:
  auto end = chn->append_start.load(std::memory_order_relaxed);
  if (end + sz >= kPerThreadBuffer) {
    while (!TryLock(tid)) __builtin_ia32_pause();
    auto start = chn->flusher_start;
    chn->flusher_start = 0;
    chn->append_start.store(0, std::memory_order_release);
    PushRelease(tid, start, end);
    goto retry;
  }
  auto ptr = chn->mem + end;
  return ptr;
}

void SendChannel::Finish(size_t sz)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  auto &chn = channels[tid];
  chn->append_start.store(chn->append_start.load(std::memory_order_relaxed) + sz,
                          std::memory_order_release);
}

bool SendChannel::PushRelease(int tid, unsigned int start, unsigned int end)
{
  auto mem = channels[tid]->mem;
  if (end - start > 0) {
    void *buf = alloca(end - start);
    memcpy(buf, mem + start, end - start);
    channels[tid]->dirty.store(true, std::memory_order_release);
    Unlock(tid);
    out->Write(buf, end - start);
    return true;
  } else {
    Unlock(tid);
    return channels[tid]->dirty.load();
  }
}

void SendChannel::DoFlush()
{
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  auto &chn = channels[core_id];

  uint8_t signal = 0;
  // logger->info("SendChannel signaling flusher");
  flusher_channel->Write(&signal, 1);

  for (int i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    channels[i]->dirty = false;
  }
}

void SendChannel::FlusherRoutine::Run()
{
  while (true) {
    uint8_t signal = 0;
    flusher_channel->Read(&signal, 1);
    out->Flush();
  }
}

long SendChannel::PendingFlush(int core_id)
{
  // return channels[core_id]->flusher_cnt;
  return 0;
}

size_t NodeConfiguration::g_nr_threads = 8;
int NodeConfiguration::g_core_shifting = 0;
bool NodeConfiguration::g_data_migration = false;

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
  if (NodeConfiguration::g_data_migration) {
    config->row_shipper_peer = ParseNodePeerConfig(json, "row_shipper");
  } else {
    config->index_shipper_peer = ParseNodePeerConfig(json, "index_shipper");
  }
  config->name = json.object_items().find("name")->second.string_value();
}

size_t NodeConfiguration::BatchBufferIndex(int level, int src_node, int dst_node)
{
  return level * nr_nodes() * nr_nodes() + (src_node - 1) * nr_nodes() + dst_node - 1;
}

NodeConfiguration::NodeConfiguration()
    : lb(new PromiseRoundRobin(0))
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

  total_batch_counters = new std::atomic_ulong[kPromiseMaxLevels * nr_nodes() * nr_nodes()];
  local_batch_counters = new ulong[2 + kPromiseMaxLevels * nr_nodes() * nr_nodes()];

  for (int i = 0; i < kPromiseMaxLevels; i++) {
    for (int j = 0; j < nr_nodes(); j++) {
      for (int k = 0; k < nr_nodes(); k++) {
        total_batch_counters[BatchBufferIndex(i, j + 1, k + 1)] = 0;
      }
    }
  }

  transport_meta.Init(nr_nodes(), g_nr_threads);

  ResetBufferPlan();

  memset(urgency_cnt, 0, kMaxNrThreads * sizeof(long));
}

using go::TcpSocket;
using go::TcpInputChannel;
using go::BufferChannel;
using util::Instance;

class NodeServerThreadRoutine : public go::Routine {
  TcpSocket *sock;
  int idx;
  ulong src_node_id;
  std::atomic<long> tid;
  PromiseRoundRobin lb;
  NodeConfiguration &conf;
 public:
  NodeServerThreadRoutine(TcpSocket *client_sock, int idx)
      : sock(client_sock),
        idx(idx), src_node_id(0),
        tid(1),
        lb(idx), conf(Instance<NodeConfiguration>()) {
    client_sock->OmitReadLock();
  }
  void Flush() {
    lb.Flush();
  }
  int thread_pool_id() const { return tid.load(std::memory_order_relaxed); }
  virtual void Run() final;

 private:
  void UpdateBatchCounters();
};

class NodeServerRoutine : public go::Routine {
 public:
  virtual void Run() final;
};

void NodeServerThreadRoutine::Run()
{
  while (true) {
    ulong nr_recv[NodeConfiguration::kPromiseMaxLevels];
    memset(nr_recv, 0, sizeof(ulong) * NodeConfiguration::kPromiseMaxLevels);
    ulong nr_recv_bytes = 0;

    while (true) {
      auto in = sock->input_channel();
      size_t promise_size = 0;
      in->Read(&promise_size, 8);

      if (promise_size == 0) {
        break;
      }

      if (promise_size == PromiseRoutine::kUpdateBatchCounter) {
        UpdateBatchCounters();
        break;
      }

      abort_if(src_node_id == 0,
               "Protocol error. Should always send the updated counters first");

      uint8_t level = 0;

      if (promise_size & PromiseRoutine::kBubble) {
        level = (promise_size & 0x00FF);
        util::Impl<PromiseRoutineDispatchService>().AddBubble();
      } else {
        auto [r, input] = PromiseRoutine::CreateFromPacket(in, promise_size);
        level = r->level;

        lb.QueueRoutine(r, input);

        nr_recv_bytes += 8 + promise_size;
      }

      auto cnt = ++nr_recv[level];
      auto idx = conf.BatchBufferIndex(level, src_node_id, conf.node_id());

      if (cnt == conf.total_batch_counters[idx].load()) {
        logger->info("Flush from node {}, level = {}, cur_recv_bytes {}, cnt = {}",
                     src_node_id, level, nr_recv_bytes, cnt);
        Flush();
      }

#if 0
      // Load balancing?
      if (cnt % (1 << 17) == 0) {
        auto sched = go::Scheduler::Current();
        auto r =
            go::Make(
                [this]() {
                  auto ord = std::memory_order_relaxed;
                  auto new_tid = (tid.load(ord) + 1) % NodeConfiguration::g_nr_threads + 1;
                  tid.store(new_tid, ord);
                  auto sched = go::GetSchedulerFromPool(tid.load());
                  sched->WakeUp(this);
                });
        r->set_urgent(true);
        sched->WakeUp(r);
        sched->RunNext(go::Scheduler::ReadyState);
      }
#endif

    }
  }
}

void NodeServerThreadRoutine::UpdateBatchCounters()
{
  auto &conf = util::Instance<NodeConfiguration>();
  auto nr_nodes = conf.nr_nodes();
  auto cmp = EpochClient::g_workload_client->completion_object();
  auto buffer_size = 8 + NodeConfiguration::kPromiseMaxLevels * nr_nodes * nr_nodes * sizeof(ulong);
  auto *counters = (ulong *) alloca(buffer_size);
  auto in = sock->input_channel();

  in->Read(counters, buffer_size);
  src_node_id = counters[0];

  logger->info("from node {}", src_node_id);

  for (int i = 0; i < NodeConfiguration::kPromiseMaxLevels; i++) {
    bool all_zero = true;
    for (int src = 0; src < nr_nodes; src++) {
      for (int dst = 0; dst < nr_nodes; dst++) {
        auto idx = conf.BatchBufferIndex(i, src + 1, dst + 1);
        auto cnt = counters[1 + idx];
        if (cnt == 0) continue;

        conf.total_batch_counters[idx].fetch_add(cnt);
        all_zero = false;

        if (dst + 1 == conf.node_id())
          cmp->Increment(cnt);
      }
    }

    if (all_zero) continue;

    // Print out debugging information
    printf("update: \t%d\t", i);
    for (int src = 0; src < nr_nodes; src++) {
      for (int dst = 0; dst < nr_nodes; dst++) {
        auto idx = conf.BatchBufferIndex(i, src + 1, dst + 1);
        auto cnt = counters[1 + idx];

        printf(" %d->%d=%lu", src + 1, dst + 1,
               conf.total_batch_counters[idx].load());
      }
    }
    puts("");
  }
  cmp->Complete();
}

void NodeServerRoutine::Run()
{
  auto &console = util::Instance<Console>();

  auto server_sock = new TcpSocket(1024, 1024);
  auto &conf = Instance<NodeConfiguration>();
  auto &node_conf = conf.config();

  auto nr_nodes = conf.nr_nodes();
  BasePromise::InitializeSourceCount(nr_nodes, conf.g_nr_threads);

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
  for (auto &config: conf.all_config) {
    if (!config) continue;
    util::Instance<SliceMappingTable>().InitNode(config->id);
    if (config->id == conf.node_id()) {
      util::Instance<SliceMappingTable>().AddEntry(100 + config->id, IndexOwner, config->id);
      continue;
    }

    logger->info("Connecting worker peer on node {}", config->id);
    TcpSocket *remote_sock = new TcpSocket(1024, 512 << 20);
    auto &peer = config->worker_peer;
    bool rs = remote_sock->Connect(peer.host, peer.port);
    abort_if(!rs, "Cannot connect to {}:{}", peer.host, peer.port);
    conf.all_nodes[config->id] = remote_sock;
    conf.all_out_channels[config->id] = new SendChannel(remote_sock);
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
    conf.clients.push_back(client_sock);
    auto *routine = new NodeServerThreadRoutine(client_sock, i);

    conf.all_in_routines[i - 1] = routine;

    // go::GetSchedulerFromPool(1)->WakeUp(routine);
    sched->WakeUp(routine);
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
  logger->info("Shipment listening on {} {}", host, port);
  server->Bind(host, port);
  server->Listen();

  while (true) {
    auto *client_sock = server->Accept();
    auto receiver = new IndexShipmentReceiver(client_sock);
    go::Scheduler::Current()->WakeUp(receiver);
  }
}

class NodeRowShipmentReceiverRoutine : public go::Routine {
  std::string host;
  unsigned short port;
 public:
  NodeRowShipmentReceiverRoutine(std::string host, unsigned short port) : host(host), port(port) {}

  void Run() final override;
};

void NodeRowShipmentReceiverRoutine::Run()
{
  go::TcpSocket *server = new go::TcpSocket(8192, 1024);
  logger->info("Row Shipment listening on {} {}", host, port);
  server->Bind(host, port);
  server->Listen();

  while (true) {
    auto *client_sock = server->Accept();
    auto receiver = new RowShipmentReceiver(client_sock);
    go::Scheduler::Current()->WakeUp(receiver);
  }
}

void NodeConfiguration::RunAllServers()
{
  if (NodeConfiguration::g_data_migration) {
    logger->info("Starting system thread for row shipment receiving");
    auto &peer = config().row_shipper_peer;
    go::GetSchedulerFromPool(g_nr_threads + 1)->WakeUp(
        new NodeRowShipmentReceiverRoutine(peer.host, peer.port));
  } else {
    logger->info("Starting system thread for index shipment receiving");
    auto &peer = config().index_shipper_peer;
    go::GetSchedulerFromPool(g_nr_threads + 1)->WakeUp(
        new NodeIndexShipmentReceiverRoutine(peer.host, peer.port));
  }

  logger->info("Starting node server with id {}", node_id());
  go::GetSchedulerFromPool(0)->WakeUp(new NodeServerRoutine());
}

SendChannel *NodeConfiguration::GetOutputChannel(int node_id)
{
  auto ch = all_out_channels[node_id];
  abort_if(!ch, "node with id {} does not exist!", node_id);
  return ch;
}

void NodeConfiguration::TransportPromiseRoutine(PromiseRoutine *routine, const VarStr &in)
{
  auto src_node = node_id();
  auto dst_node = routine->node_id == 0 ? id : routine->node_id;
  int level = routine->level;
  auto &meta = transport_meta.GetLocalData(level, go::Scheduler::CurrentThreadPoolId() - 1);
  bool bubble = (in.data == (uint8_t *) PromiseRoutine::kBubblePointer);

  if (src_node != dst_node) {
    auto out = GetOutputChannel(dst_node);
    if (!bubble) {
      uint64_t buffer_size = routine->TreeSize(in);
      auto *buffer = (uint8_t *) out->Alloc(8 + buffer_size);

      memcpy(buffer, &buffer_size, 8);
      routine->EncodeTree(buffer + 8, in);
      out->Finish(8 + buffer_size);
    } else {
      auto *flag = (uint64_t *) out->Alloc(8);
      *flag = PromiseRoutine::kBubble | routine->level;
      out->Finish(8);
    }
  } else {
    if (!bubble) {
      lb->QueueRoutine(routine, in);
    } else {
      lb->QueueBubble();
    }
  }
  meta.AddRoute(dst_node);
}

void NodeConfiguration::PreparePromisesToQueue(int core, int level, unsigned long nr)
{
  auto &meta = transport_meta.GetLocalData(level, core);
  meta.IncrementExpected(nr);
}

void NodeConfiguration::FinishPromiseFromQueue(PromiseRoutine *routine)
{
  auto src_node = node_id();
  auto core = go::Scheduler::CurrentThreadPoolId() - 1;
  int level = routine ? routine->level : -1;
  if (level >= 0) {
    auto &meta = transport_meta.GetLocalData(level, core);
    if (!meta.Finish())
      return;
  }

  level++;
  auto &meta = transport_meta.GetLocalData(level, core);
  for (auto dst_node = 1; dst_node <= nr_nodes(); dst_node++) {
    auto idx = BatchBufferIndex(level, src_node, dst_node);
    auto target_cnt = total_batch_counters[idx].load();
    auto cnt = transport_meta.Merge(level, meta, dst_node);
    // printf("cnt %lu, target %lu\n", cnt, target_cnt);
    if (cnt == target_cnt) {
      // Flush channels to this route
      if (dst_node != src_node) {
        GetOutputChannel(dst_node)->Flush();
      } else {
        lb->Flush();
      }
    }
  }
}

void NodeConfiguration::ForceFlushPromiseRoutine()
{
  for (int i = 1; i < nr_nodes(); i++) {
    all_in_routines[i - 1]->Flush();
  }
  for (int i = 1; i <= nr_nodes(); i++) {
    if (i == node_id()) continue;
    GetOutputChannel(i)->Flush();
  }
  lb->Flush();
}

void NodeConfiguration::ResetBufferPlan()
{
  local_batch_counters[0] = std::numeric_limits<ulong>::max();
  memset(local_batch_counters + 2, 0,
         kPromiseMaxLevels * nr_nodes() * nr_nodes() * sizeof(ulong));
  for (ulong i = 0; i < kPromiseMaxLevels * nr_nodes() * nr_nodes(); i++)
    total_batch_counters[i].store(0);
  transport_meta.Reset(nr_nodes(), g_nr_threads);
}

void NodeConfiguration::CollectBufferPlan(BasePromise *root)
{
  auto src_node = node_id();
  for (size_t i = 0; i < root->nr_routines(); i++) {
    auto *routine = root->routine(i);
    CollectBufferPlanImpl(routine, 0, src_node, 0);
  }
}

void NodeConfiguration::CollectBufferPlanImpl(PromiseRoutine *routine, int level, int src_node, int nr_extra)
{
  abort_if(level >= kPromiseMaxLevels, "promise level {} too deep", level);
  routine->level = level;

  auto dst_node = routine->node_id;
  if (dst_node == 0)
    dst_node = src_node;
  local_batch_counters[2 + BatchBufferIndex(level, src_node, dst_node)] += 1 + nr_extra;

  if (routine->next == nullptr)
    return;

  if (routine->pipeline > 0)
    nr_extra = routine->pipeline;

  for (size_t i = 0; i < routine->next->nr_routines(); i++) {
    auto *subroutine = routine->next->routine(i);
    CollectBufferPlanImpl(subroutine, level + 1, dst_node, nr_extra);
  }
}

void NodeConfiguration::FlushBufferPlan(bool sync)
{
  EpochClient::g_workload_client->completion_object()->Increment(nr_nodes() - 1);
  local_batch_counters[1] = (ulong) node_id();
  logger->info("Flushing buffer plan");
  for (int i = 0; i < kPromiseMaxLevels; i++) {
    bool all_zero = true;
    for (int src = 0; src < nr_nodes(); src++) {
      for (int dst = 0; dst < nr_nodes(); dst++) {
        auto idx = BatchBufferIndex(i, src + 1, dst + 1);
        auto counter = local_batch_counters[2 + idx];
        if (counter == 0) continue;

        total_batch_counters[idx].fetch_add(counter);
        all_zero = false;

        if (dst + 1 == node_id()) {
          EpochClient::g_workload_client->completion_object()->Increment(counter);
        }
      }
    }
    if (all_zero) continue;

    for (int src = 0; src < nr_nodes(); src++) {
      for (int dst = 0; dst < nr_nodes(); dst++) {
        auto idx = BatchBufferIndex(i, src + 1, dst + 1);
        auto counter = local_batch_counters[2 + idx];

        printf(" %d->%d=%lu(%lu)", src + 1, dst + 1,
               total_batch_counters[idx].load(), counter);
      }
    }

    puts("");
  }

  std::vector<std::function<void ()>> funcs;

  for (int id = 1; id <= nr_nodes(); id++) {
    if (id == node_id()) continue;

    auto out = all_nodes[id]->output_channel();
    auto in = all_nodes[id]->input_channel();
    auto buffer_size = 16 + kPromiseMaxLevels * nr_nodes() * nr_nodes() * sizeof(ulong);
    auto buffer = local_batch_counters;
    funcs.emplace_back(
        [in, out, buffer, buffer_size]() {
          uint64_t remote_epoch_finished;
          in->Read(&remote_epoch_finished, 8);

          // Read and replay slice mapping table updates.
          uint32_t num_slice_table_updates;
          in->Read(&num_slice_table_updates, 4);
          for (int i = 0; i < num_slice_table_updates; i++) {
            uint32_t op;
            in->Read(&op, 4);
            util::Instance<SliceMappingTable>().ReplayUpdate(op);
          }

          out->Write(buffer, buffer_size);
          out->Flush();
        });
  }

  if (sync) {
    for (auto &f: funcs) f();
  } else {
    auto sched = go::Scheduler::Current();
    for (auto &f: funcs)
      sched->WakeUp(go::Make(f));
    sched->current_routine()->VoluntarilyPreempt(false);
  }
}

void NodeConfiguration::FlushBufferPlanCompletion(uint64_t epoch_nr)
{
  for (auto *sock: clients) {
    auto out = sock->output_channel();
    out->Write(&epoch_nr, 8);

    // Write out all the slice mapping table update commands.
    auto &broadcast_buffer = util::Instance<SliceMappingTable>().broadcast_buffer;
    auto int_buf = broadcast_buffer.size();
    out->Write(&int_buf, 4);
    for (const auto &it : broadcast_buffer) {
      out->Write(&it, 4);
    }
    broadcast_buffer.clear();

    out->Flush();
  }
}

void NodeConfiguration::SendBarrier(int node_id)
{
  // TODO:
  // auto out = GetOutputChannel(node_id);
  // uint64_t eop = 0;
  // out->Write(&eop, 8);
  TBD();
}

void NodeConfiguration::BroadcastBarrier()
{
  // TODO:
  TBD();

  for (auto &config: all_config) {
    if (!config) continue;
    SendBarrier(config->id);
  }
}

}

namespace util {

felis::NodeConfiguration *InstanceInit<felis::NodeConfiguration>::instance;

}
