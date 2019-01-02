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

#include "iface.h"

namespace felis {

template <typename T>
class FlushImpl : public T {
 public:

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
            && this->TryLock(i)) {
          if (this->PushRelease(i))
            need_do_flush = true;
          ThreadBitmapMark(flushed, i);
          nr_flushed++;
          this->Unlock(i);
        }
      }
    }
    if (need_do_flush)
      this->DoFlush();
  }

  using T::T;
};

class PromiseRoundRobinImpl {
  struct Queue {
    PromiseRoutine **routines;
    size_t nr_routines;
    std::atomic_bool lock;
  };

  util::CacheAligned<Queue> queues[NodeConfiguration::kMaxNrThreads + 1];
  int idx;
  std::atomic_ulong round;
  static constexpr size_t kBufferSize = 4096;
 public:
  PromiseRoundRobinImpl(int idx);
  void QueueRoutine(PromiseRoutine *routine);
 protected:
  bool PushRelease(int thr);
  void DoFlush() { BasePromise::FlushScheduler(); }
  bool TryLock(int i) {
    bool locked = false;
    return queues[i]->lock.compare_exchange_strong(locked, true);
  }
  void Unlock(int i) {
    queues[i]->lock.store(false);
  }
};

class SendChannelImpl {
  go::TcpOutputChannel *out;

  struct Channel {
    mem::Brk brk;
    std::atomic_bool lock;
    go::BufferChannel *flusher_channel;
    long flusher_cnt;
  };

  // We need to create a per-thread, long-running flusher go::Routine.
  class FlusherRoutine : public go::Routine {
    Channel *chn;
    go::TcpOutputChannel *out;
   public:
    FlusherRoutine(Channel *chn, go::TcpOutputChannel *out)
        : chn(chn), out(out) {
      set_urgent(true);
    }
    void Run() final override;
  };

  util::CacheAligned<Channel> channels[NodeConfiguration::kMaxNrThreads + 1];

 public:
  static constexpr size_t kPerThreadBuffer = 16 << 10;
  SendChannelImpl(go::TcpSocket *sock);
  void *Alloc(size_t sz);
  void Unlock() {
    Unlock(go::Scheduler::CurrentThreadPoolId());
  }

  long PendingFlush(int core_id);

 protected:
  bool PushRelease(int thr);
  void DoFlush();
  bool TryLock(int i) {
    bool locked = false;
    return channels[i]->lock.compare_exchange_strong(locked, true);
  }
  void Unlock(int i) {
    channels[i]->lock.store(false);
  }
};

PromiseRoundRobinImpl::PromiseRoundRobinImpl(int idx)
    : idx(idx), round(0)
{
  for (int i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    queues[i]->lock = false;
    queues[i]->routines = new PromiseRoutine*[kBufferSize];
    queues[i]->nr_routines = 0;
  }
}

void PromiseRoundRobinImpl::QueueRoutine(PromiseRoutine *routine)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  while (!TryLock(tid)) __builtin_ia32_pause();
  auto &q = queues[tid];

  q->routines[q->nr_routines++] = routine;
  if (q->nr_routines == kBufferSize) {
    PushRelease(tid);
  } else {
    Unlock(tid);
  }
}

bool PromiseRoundRobinImpl::PushRelease(int thr)
{
  size_t nr_routines = queues[thr]->nr_routines;
  PromiseRoutine *routines[kBufferSize];
  int nr_threads = NodeConfiguration::g_nr_threads;
  ulong delta = nr_threads - nr_routines % nr_threads;
  ulong rnd = round.fetch_add(delta);
  memcpy(routines, queues[thr]->routines, nr_routines * sizeof(PromiseRoutine *));
  queues[thr]->nr_routines = 0;
  Unlock(thr);

  for (int i = 0; i < nr_threads; i++) {
    size_t start = i * nr_routines / nr_threads;
    size_t end = (i + 1) * nr_routines / nr_threads;

    if (end == start) continue;
    BasePromise::QueueRoutine(routines + start, end - start, idx,
                              (i + rnd) % NodeConfiguration::g_nr_threads + 1,
                              false);
  }
  return nr_routines > 0;
}

SendChannelImpl::SendChannelImpl(go::TcpSocket *sock)
    : out(sock->output_channel())
{
  auto buffer =
      (uint8_t *) malloc((NodeConfiguration::g_nr_threads + 1) * kPerThreadBuffer);
  for (int i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    auto &chn = channels[i];
    chn->brk.move(mem::Brk(buffer + i * kPerThreadBuffer, kPerThreadBuffer));
    chn->lock = false;
    chn->flusher_channel = new go::BufferChannel(1024);
    chn->flusher_cnt = 0;
    go::GetSchedulerFromPool(i + 1)->WakeUp(new FlusherRoutine(&chn.elem, out));
  }
}

void *SendChannelImpl::Alloc(size_t sz)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  abort_if(tid < 0, "Have to call this within a go-routine");

  while (!TryLock(tid)) __builtin_ia32_pause();

  auto &chn = channels[tid];

  auto &b = chn->brk;
  while (!b.Check(sz)) {
    PushRelease(tid);
    while (!TryLock(tid)) __builtin_ia32_pause();
  }
  return b.Alloc(sz);
}

bool SendChannelImpl::PushRelease(int tid)
{
  auto &b = channels[tid]->brk;
  if (b.current_size() > 0) {
    void *buf = alloca(b.current_size());
    auto sz = b.current_size();
    memcpy(buf, b.ptr(), sz);
    b.Reset();
    Unlock(tid);
    out->Write(buf, sz);
    return true;
  } else {
    Unlock(tid);
    return false;
  }
}

void SendChannelImpl::DoFlush()
{
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  auto &chn = channels[core_id];
  if (chn->flusher_cnt++ == 0) {
    uint8_t signal = 0;
    chn->flusher_channel->Write(&signal, 1);
  }
  go::Scheduler::Current()->current_routine()->VoluntarilyPreempt(true);
}

void SendChannelImpl::FlusherRoutine::Run()
{
  while (true) {
    uint8_t signal = 0;
    if (chn->flusher_cnt == 0)
      chn->flusher_channel->Read(&signal, 1);
    out->Flush();
    chn->flusher_cnt--;
  }
}

long SendChannelImpl::PendingFlush(int core_id)
{
  return channels[core_id]->flusher_cnt;
}

size_t NodeConfiguration::g_nr_threads = 8;
int NodeConfiguration::g_core_shifting = 0;

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
    batch_counters[i] = new std::atomic_ulong[nr_nodes()];
    for (int j = 0; j < nr_nodes(); j++) {
      for (int k = 0; k < nr_nodes(); k++) {
        total_batch_counters[BatchBufferIndex(i, j + 1, k + 1)] = 0;
      }
      batch_counters[i][j] = 0;
    }
  }
  ResetBufferPlan();

  memset(extra_iopendings, 0, sizeof(ulong) * kMaxNrThreads);
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
    set_urgent(true);
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
      ulong promise_size = 0;
      in->Read(&promise_size, 8);

      if (promise_size == 0) {
        // TODO: Epoch sync??
        // BasePromise::QueueRoutine(nullptr, idx, -1);
        break;
      }

      if (promise_size == std::numeric_limits<ulong>::max()) {
        UpdateBatchCounters();
        break;
      }

      abort_if(src_node_id == 0,
               "Protocol error. Should always send the updated counters first");

      auto *p = (uint8_t *) util::Impl<PromiseAllocationService>().Alloc(
          util::Align(promise_size, CACHE_LINE_SIZE));
      in->Read(p, promise_size);

      auto r = PromiseRoutine::CreateFromPacket(p, promise_size);
      auto level = r->level;
      lb.QueueRoutine(r);

      nr_recv_bytes += 8 + promise_size;
      auto cnt = ++nr_recv[level];
      auto idx = conf.BatchBufferIndex(level, src_node_id, conf.node_id());

      if (cnt == conf.total_batch_counters[idx].load()) {
        logger->info("Flush from node {}, level = {}, cur_recv_bytes {}",
                     src_node_id, level, nr_recv_bytes);
        Flush();
      }
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
  for (int i = 0; i < NodeConfiguration::kPromiseMaxDebugLevels; i++) {
    printf("update: \t%d\t", i);
    for (int src = 0; src < nr_nodes; src++) {
      for (int dst = 0; dst < nr_nodes; dst++) {
        auto idx = conf.BatchBufferIndex(i, src + 1, dst + 1);
        auto cnt = counters[1 + idx];
        conf.total_batch_counters[idx].fetch_add(cnt);

        printf(" %d->%d=%lu", src + 1, dst + 1,
               conf.total_batch_counters[idx].load());

        if (dst + 1 == conf.node_id())
          cmp->Increment(cnt);
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
    if (config->id == conf.node_id()) continue;
    logger->info("Connecting worker peer on node {}\n", config->id);
    TcpSocket *remote_sock = new TcpSocket(512 << 20, 512 << 20);
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
    go::GetSchedulerFromPool(1)->WakeUp(routine);
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

void NodeConfiguration::RunAllServers()
{
  logger->info("Starting system thread for index shipment");
  auto &peer = config().index_shipper_peer;
  go::GetSchedulerFromPool(g_nr_threads + 1)->WakeUp(
      new NodeIndexShipmentReceiverRoutine(peer.host, peer.port));

  logger->info("Starting node server with id {}", node_id());
  go::GetSchedulerFromPool(0)->WakeUp(new NodeServerRoutine());
}

SendChannel *NodeConfiguration::GetOutputChannel(int node_id)
{
  auto ch = all_out_channels[node_id];
  abort_if(!ch, "node with id {} does not exist!", node_id);
  return ch;
}

void NodeConfiguration::TransportPromiseRoutine(PromiseRoutine *routine)
{
  auto src_node = node_id();
  auto dst_node = routine->node_id == 0 ? id : routine->node_id;
  int level = routine->level;
  auto idx = BatchBufferIndex(level, src_node, dst_node);
  auto target_cnt = total_batch_counters[idx].load();
  auto &current_cnt = batch_counters[level][dst_node - 1];

  if (src_node != dst_node) {
    uint64_t buffer_size = routine->TreeSize();
    auto out = GetOutputChannel(dst_node);
    auto *buffer = (uint8_t *) out->Alloc(8 + buffer_size);

    memcpy(buffer, &buffer_size, 8);
    routine->EncodeTree(buffer + 8);
    out->Unlock();

    if (current_cnt.fetch_add(1) + 1 == target_cnt) {
      logger->info("Flush to {} level {}", dst_node, level);
      out->Flush();
    }

    routine->input.data = nullptr;
  } else {
    // auto &in = routine->input;
    // uint8_t *p = (uint8_t *) malloc(in.len);
    // memcpy(p, in.data, in.len);
    // routine->input = VarStr(in.len, in.region_id, p);

    lb->QueueRoutine(routine);
    if (current_cnt.fetch_add(1) + 1 == target_cnt) {
      logger->info("Flush from myself. level = {}", level);
      lb->Flush();
    }
  }
}

void NodeConfiguration::FlushPromiseRoutine()
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

long NodeConfiguration::IOPending(int core_id)
{
  long s = extra_iopendings[core_id];

  // Has this core being used as a in_routine (the one reading promises from
  // other nodes)?
  for (int i = 1; i < nr_nodes(); i++) {
    if (core_id + 1 == all_in_routines[i - 1]->thread_pool_id())
      s++;
  }
  for (int i = 1; i <= nr_nodes(); i++) {
    if (i == node_id()) continue;
    s += GetOutputChannel(i)->PendingFlush(core_id);
  }
  return s;
}

void NodeConfiguration::ResetBufferPlan()
{
  local_batch_counters[0] = std::numeric_limits<ulong>::max();
  memset(local_batch_counters + 2, 0,
         kPromiseMaxLevels * nr_nodes() * nr_nodes() * sizeof(ulong));
  for (ulong i = 0; i < kPromiseMaxLevels * nr_nodes() * nr_nodes(); i++)
    total_batch_counters[i].store(0);
  for (ulong l = 0; l < kPromiseMaxLevels; l++) {
    for (ulong i = 0; i < nr_nodes(); i++)
      batch_counters[l][i] = 0;
  }
}

void NodeConfiguration::CollectBufferPlan(BasePromise *root)
{
  auto src_node = node_id();
  auto *routines = root->routines();
  for (size_t i = 0; i < root->nr_routines(); i++) {
    auto *routine = routines[i];
    CollectBufferPlanImpl(routine, 0, src_node);
  }
}

void NodeConfiguration::CollectBufferPlanImpl(PromiseRoutine *routine, int level, int src_node)
{
  abort_if(level >= kPromiseMaxLevels, "promise level {} too deep", level);
  routine->level = level;

  auto dst_node = routine->node_id;
  if (dst_node == 0)
    dst_node = src_node;
  local_batch_counters[2 + BatchBufferIndex(level, src_node, dst_node)]++;

  if (routine->next == nullptr)
    return;

  auto *subroutines = routine->next->routines();
  for (size_t i = 0; i < routine->next->nr_routines(); i++) {
    auto *subroutine = subroutines[i];
    CollectBufferPlanImpl(subroutine, level + 1, dst_node);
  }
}

void NodeConfiguration::FlushBufferPlan(bool sync)
{
  EpochClient::g_workload_client->completion_object()->Increment(nr_nodes() - 1);
  local_batch_counters[1] = (ulong) node_id();
  logger->info("Flushing buffer plan");
  for (int i = 0; i < kPromiseMaxLevels; i++) {
    if (i < kPromiseMaxDebugLevels) printf("level: \t%d\t", i);
    for (int src = 0; src < nr_nodes(); src++) {
      for (int dst = 0; dst < nr_nodes(); dst++) {
        auto idx = BatchBufferIndex(i, src + 1, dst + 1);
        auto counter = local_batch_counters[2 + idx];
        auto current_cnt = total_batch_counters[idx].fetch_add(counter) + counter;
        if (i < kPromiseMaxDebugLevels)
          printf(" %d->%d=%lu(%lu)", src + 1, dst + 1, current_cnt, counter);
        if (dst + 1 == node_id()) {
          EpochClient::g_workload_client->completion_object()->Increment(counter);
        }
      }
    }
    if (i < kPromiseMaxDebugLevels) puts("");
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
