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
#include "opts.h"

namespace felis {

void TransportBatchMetadata::Init(int nr_nodes, int nr_cores)
{
  LocalMetadata *mem = nullptr;
  for (auto i = 0; i < nr_cores; i++) {
    auto d = std::div(i + NodeConfiguration::g_core_shifting, mem::kNrCorePerNode);
    auto numa_node = d.quot;
    auto numa_offset = d.rem;
    if (numa_offset == 0) {
      mem = (LocalMetadata *) mem::MemMapAlloc(
          mem::Promise,
          sizeof(LocalMetadata) * kMaxLevels * mem::kNrCorePerNode,
          numa_node);
    }
    thread_local_data[i] = mem + kMaxLevels * numa_offset;
  }
}

void TransportBatchMetadata::Reset(int nr_nodes, int nr_cores)
{
  for (auto i = 0; i < nr_cores; i++) {
    for (auto j = 0; j < kMaxLevels; j++) {
      thread_local_data[i][j].Reset(nr_nodes);
    }
  }
  for (auto &p: counters) {
    for (auto n = 0; n < nr_nodes; n++) p[n] = 0;
  }
}

unsigned long TransportBatchMetadata::Merge(int level, LocalMetadata &local, int node)
{
  auto v = local.delta[node - 1];
  local.delta[node - 1] = 0;
  return counters[level][node - 1].fetch_add(v) + v;
}

template <typename T>
class Flushable {
 protected:
 private:
  T *self() { return (T *) this; }

 public:

  void Flush() {
    std::bitset<NodeConfiguration::kMaxNrThreads + 1> flushed;
    bool need_do_flush = false;
    // Also flush the main go-routine
    auto nr_threads = NodeConfiguration::g_nr_threads + 1;

    while (flushed.count() < nr_threads) {
      int i = 0;
      for (auto i = 0; i < nr_threads; i++) {
        if (!flushed[i]) {
          if (self()->TryLock(i)) {
            auto [start, end] = self()->GetFlushRange(i);
            self()->UpdateFlushStart(i, end);

            if (self()->PushRelease(i, start, end)) {
              need_do_flush = true;
            }
            flushed.set(i);
          }
        }
      }
    }
    if (need_do_flush)
      self()->DoFlush();
  }
};

class TransportImpl : public Flushable<TransportImpl> {
  static constexpr size_t kBufferSize = 16383;
  struct Queue {
    // Putting these per-core task buffer simply because it's too large and we
    // can't put them on the stack!
    struct {
      std::array<PromiseRoutineWithInput, kBufferSize> routines;
      size_t nr;
    } task_buffer[NodeConfiguration::kMaxNrThreads];

    std::array<PromiseRoutineWithInput, kBufferSize> routines;
    std::atomic_uint append_start = 0;
    unsigned int flusher_start = 0;
    std::atomic_bool need_scan = false;
    util::SpinLock lock;
  };

  std::array<Queue *, NodeConfiguration::kMaxNrThreads + 1> queues;
  std::atomic_ulong dice;
  int idx;

 public:
  TransportImpl(int idx);
  void QueueRoutine(PromiseRoutine *routine, const VarStr &in);
  void QueueBubble();

  std::tuple<uint, uint> GetFlushRange(int tid) {
    return {
      queues[tid]->flusher_start,
      queues[tid]->append_start.load(std::memory_order_acquire),
    };
  }
  void UpdateFlushStart(int tid, unsigned int flush_start) {
    queues[tid]->flusher_start = flush_start;
  }

  bool PushRelease(int tid, unsigned int start, unsigned int end);
  void DoFlush();

  bool TryLock(int i) {
    return queues[i]->lock.TryLock();
  }
  void Unlock(int i) {
    queues[i]->lock.Unlock();
  }

 private:
  void FlushOnCore(int thread, unsigned int start, unsigned int end);
  void SubmitOnCore(PromiseRoutineWithInput *routines, unsigned int start, unsigned int end, int thread);
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

  std::tuple<unsigned int, unsigned int> GetFlushRange(int tid) {
    return {
      channels[tid]->flusher_start,
      channels[tid]->append_start.load(std::memory_order_acquire),
    };
  }
  void UpdateFlushStart(int tid, unsigned int flush_start) {
    channels[tid]->flusher_start = flush_start;
  }
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

TransportImpl::TransportImpl(int idx)
    : idx(idx), dice(0)
{
  auto mem =
      (Queue *) mem::MemMapAlloc(mem::EpochQueueItem, sizeof(Queue), -1);

  for (int i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    if (i > 0) {
      auto d = std::div(i - 1 + NodeConfiguration::g_core_shifting, mem::kNrCorePerNode);
      if (d.rem == 0) {
        mem = (Queue *) mem::MemMapAlloc(
            mem::EpochQueueItem, sizeof(Queue) * mem::kNrCorePerNode, d.quot);
      }
      queues[i] = new (mem + d.rem) Queue();
    } else {
      queues[0] = new (mem) Queue();
    }
  }
}

void TransportImpl::QueueRoutine(PromiseRoutine *routine, const VarStr &in)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  auto q = queues[tid];
  auto nr_threads = NodeConfiguration::g_nr_threads;
  auto pos = q->append_start.load(std::memory_order_acquire);
  q->routines[pos] = {routine, in};
  if (routine->affinity == std::numeric_limits<uint64_t>::max()) routine->affinity = tid - 1;
  if (tid != routine->affinity + 1 || tid == 0) q->need_scan = true;
  q->append_start.store(pos + 1, std::memory_order_release);

  if (pos == kBufferSize - 1) {
    q->lock.Lock();
    auto start = q->flusher_start, end = q->append_start.load(std::memory_order_acquire);
    q->flusher_start = 0;
    q->append_start.store(0, std::memory_order_release);
    PushRelease(tid, start, end);
    q->need_scan = false;
  }
}

void TransportImpl::QueueBubble()
{
  // Currently we don't batch the bubbles, and we consider bubbles are rare.
  util::Impl<PromiseRoutineDispatchService>().AddBubble();
}

void TransportImpl::DoFlush()
{
  BasePromise::FlushScheduler();
}

bool TransportImpl::PushRelease(int tid, unsigned int start, unsigned int end)
{
  FlushOnCore(tid, start, end);
  Unlock(tid);
  return end > start;
}

void TransportImpl::FlushOnCore(int tid, unsigned int start, unsigned int end)
{
  if (start == end) return;

  auto q = queues[tid];
  auto nr_threads = NodeConfiguration::g_nr_threads;

  if (!q->need_scan) {
    SubmitOnCore(q->routines.data(), start, end, tid);
  } else {
    for (int i = 0; i < nr_threads; i++)
      q->task_buffer[i].nr = 0;

    auto delta = dice.fetch_add((end - start) % nr_threads, std::memory_order_release);
    for (int j = start; j < end; j++) {
      auto &p = q->routines[j];
      auto [r, _] = p;
      auto core = 0;
      if (r->affinity < nr_threads) {
        core = r->affinity;
      } else {
        core = (delta + j - start) % nr_threads;
      }
      q->task_buffer[core]
          .routines[q->task_buffer[core].nr++] = p;
    }
    for (int i = 0; i < nr_threads; i++) {
      SubmitOnCore(q->task_buffer[i].routines.data(), 0, q->task_buffer[i].nr, i + 1);
    }
  }
}

void TransportImpl::SubmitOnCore(PromiseRoutineWithInput *routines, unsigned int start, unsigned int end, int thread)
{
  if (start == end) return;

  auto &transport = util::Impl<PromiseRoutineTransportService>();
  std::array<unsigned long, NodeConfiguration::kPromiseMaxLevels> cnts;
  cnts.fill(0);
  for (int j = start; j < end; j++) {
    auto [r, _] = routines[j];
    cnts[r->level]++;
  }
  abort_if(thread == 0 || thread > NodeConfiguration::g_nr_threads,
           "{} is invalid, start {} end {}", thread, start, end);
  for (auto i = 0; i < NodeConfiguration::kPromiseMaxLevels; i++) {
    if (cnts[i] == 0) continue;
    transport.PreparePromisesToQueue(thread - 1, i, cnts[i]);
  }
  // TODO: refact this into core_id instead of thread_id?
  BasePromise::QueueRoutine(routines + start, end - start, idx, thread, false);
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

void *SendChannel::Alloc(size_t sz)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  abort_if(tid < 0, "Have to call this within a go-routine");

  auto &chn = channels[tid];
retry:
  auto end = chn->append_start.load(std::memory_order_relaxed);
  if (end + sz >= kPerThreadBuffer) {
    while (!TryLock(tid)) _mm_pause();
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
bool NodeConfiguration::g_priority_txn = false;

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
    : lb(new TransportImpl(0))
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

  if (Options::kMaxNodeLimit) {
    max_node_id = std::min(max_node_id, (size_t) Options::kMaxNodeLimit.ToInt());
    for (auto i = max_node_id + 1; i < all_config.size(); i++) all_config[i] = nullopt;
    logger->info("Limit number of node to {}", max_node_id);
  }

  auto nr = kPromiseMaxLevels * nr_nodes() * nr_nodes();
  total_batch_counters = new std::atomic_ulong[nr];
  local_batch = new (malloc(sizeof(LocalBatch) + sizeof(std::atomic_ulong) * nr)) LocalBatch;

  std::fill(total_batch_counters, total_batch_counters + nr, 0);
  std::fill(local_batch->counters, local_batch->counters + nr, 0);
  local_batch->magic = PromiseRoutine::kUpdateBatchCounter;

  transport_meta.Init(nr_nodes(), g_nr_threads);

  ResetBufferPlan();
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
  TransportImpl lb;
  NodeConfiguration &conf;
  go::BufferChannel ctrl_chn;
 public:
  NodeServerThreadRoutine(TcpSocket *client_sock, int idx)
      : sock(client_sock),
        idx(idx), src_node_id(0),
        tid(1),
        lb(idx),
        conf(Instance<NodeConfiguration>()),
        ctrl_chn(go::BufferChannel(128)) {
    set_reuse(true);
    client_sock->OmitReadLock();
  }
  void Flush() {
    lb.Flush();
  }
  void ContinueEpoch() {
    uint8_t one = 1;
    ctrl_chn.Write(&one, 1);
  }
  void CloseChannel() {
    ctrl_chn.Close();
  }

  int thread_pool_id() const { return tid.load(std::memory_order_relaxed); }
  virtual void Run() final __attribute__((noinline));

 private:
  void UpdateBatchCounters();
  void UpdateSliceMappingTables(int nr_ops);
};

class NodeServerRoutine : public go::Routine {
 public:
  virtual void Run() final;
};

void NodeServerThreadRoutine::Run()
{

  std::array<ulong, NodeConfiguration::kPromiseMaxLevels> nr_recv;
  nr_recv.fill(0);

  while (true) {
    auto in = sock->input_channel();
    size_t promise_size = 0;
    in->Read(&promise_size, 8);

    if (promise_size == 0) {
      break;
    }

    uint8_t mode = 0xFF & (promise_size >> 56);

    if (mode == 0xFF) {
      // Begining of the epoch
      uint8_t all_present[conf.g_nr_threads];
      int nr_ops = promise_size & std::numeric_limits<int32_t>::max();
      int node_id;
      in->Read(&node_id, 4);
      src_node_id = node_id;

      UpdateSliceMappingTables(nr_ops);

      logger->info("Sleeping src node {}", src_node_id);
      // Waiting for the signal to start
      if (!ctrl_chn.Read(&all_present, conf.g_nr_threads))
        break;

      logger->info("Receiving from {} wakes up", src_node_id);
      continue;
    }

    if (promise_size == PromiseRoutine::kUpdateBatchCounter) {
      UpdateBatchCounters();
      continue;
    }

    abort_if(src_node_id == 0,
             "Protocol error. Should always send the node id first");

    uint8_t level = 0;

    if (promise_size & PromiseRoutine::kBubble) {
      level = (promise_size & 0x00FF);
      util::Impl<PromiseRoutineDispatchService>().AddBubble();
    } else {
      auto [r, input] = PromiseRoutine::CreateFromPacket(in, promise_size);
      level = r->level;

      lb.QueueRoutine(r, input);
      // nr_recv_bytes += 8 + promise_size;
    }

    auto cnt = ++nr_recv[level];
    auto idx = conf.BatchBufferIndex(level, src_node_id, conf.node_id());

    if (cnt == conf.total_batch_counters[idx].load()) {
      logger->info("Flush from node {}, level = {}, cnt = {}",
                   src_node_id, level, cnt);
      nr_recv[level] = 0;
      Flush();
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
  auto total_cnt = 0;

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
          total_cnt += cnt;
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

  // We now have the counter. We need to adjust the completion count with the
  // real counter.
  cmp->Complete(EpochClient::kMaxPiecesPerPhase - total_cnt);
}

void NodeServerThreadRoutine::UpdateSliceMappingTables(int nr_ops)
{
  auto in = sock->input_channel();
  auto &table = util::Instance<SliceMappingTable>();
  uint32_t op;
  for (int i = 0; i < nr_ops; i++) {
    in->Read(&op, 4);
    table.ReplayUpdate(op);
  }
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
  logger->info("Row Shipment receiving thread listening on {}:{}", host, port);
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
    auto &peer = config().row_shipper_peer;
    go::GetSchedulerFromPool(g_nr_threads + 1)->WakeUp(
        new NodeRowShipmentReceiverRoutine(peer.host, peer.port));
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
    if (routine == nullptr || (cnt == target_cnt && cnt > 0)) {
      // printf("cnt %lu, target %lu\n", cnt, target_cnt);
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
  local_batch_completed = 0;
  auto nr = kPromiseMaxLevels * nr_nodes() * nr_nodes();
  std::fill(local_batch->counters,
            local_batch->counters + nr,
            0);
  std::fill(total_batch_counters,
            total_batch_counters + nr,
            0);
  transport_meta.Reset(nr_nodes(), g_nr_threads);
}

void NodeConfiguration::CollectBufferPlan(BasePromise *root, unsigned long *cnts)
{
  auto src_node = node_id();
  for (size_t i = 0; i < root->nr_routines(); i++) {
    auto *routine = root->routine(i);
    CollectBufferPlanImpl(routine, cnts, 0, src_node);
  }
}

void NodeConfiguration::CollectBufferPlanImpl(PromiseRoutine *routine, unsigned long *cnts,
                                              int level, int src_node)
{
  abort_if(level >= kPromiseMaxLevels, "promise level {} too deep", level);
  routine->level = level;

  auto dst_node = routine->node_id;
  if (dst_node == 0)
    dst_node = src_node;
  cnts[BatchBufferIndex(level, src_node, dst_node)] += 1;

  if (routine->next == nullptr)
    return;

  for (size_t i = 0; i < routine->next->nr_routines(); i++) {
    auto *subroutine = routine->next->routine(i);
    CollectBufferPlanImpl(subroutine, cnts, level + 1, dst_node);
  }
}

bool NodeConfiguration::FlushBufferPlan(unsigned long *per_core_cnts)
{
  for (int i = 0; i < kPromiseMaxLevels; i++) {
    for (int src = 0; src < nr_nodes(); src++) {
      for (int dst = 0; dst < nr_nodes(); dst++) {
        auto idx = BatchBufferIndex(i, src + 1, dst + 1);
        auto counter = per_core_cnts[idx];
        local_buffer_plan_counters()[idx].fetch_add(counter);
        total_batch_counters[idx].fetch_add(counter);

        if (counter == 0) continue;

        if (dst + 1 == node_id()) {
          trace(TRACE_COMPLETION "Increment {} of pieces", counter);
          EpochClient::g_workload_client->completion_object()->Increment(counter);
        }
      }
    }
  }

  if (local_batch_completed.fetch_add(1) + 1 < g_nr_threads)
    return false;

  local_batch->node_id = (ulong) node_id();
  logger->info("Flushing buffer plan");

  for (int id = 1; id <= nr_nodes(); id++) {
    if (id == node_id()) continue;

    auto out = all_nodes[id]->output_channel();
    out->Write(
        local_batch,
        16 + kPromiseMaxLevels * nr_nodes() * nr_nodes() * sizeof(unsigned long));
    out->Flush();
  }

  return true;
}

void NodeConfiguration::SendStartPhase()
{
  auto &broadcast_buffer = util::Instance<SliceMappingTable>().broadcast_buffer;
  auto nr_ent = broadcast_buffer.size();
  auto buf_cnt = nr_ent * 4 + 12;
  auto buf = (uint8_t *) alloca(buf_cnt);
  uint64_t hdr = (uint64_t(0xFF) << 56) | nr_ent;
  memcpy(buf, &hdr, 8);
  memcpy(buf + 8, &id, 4);
  memcpy(buf + 12, broadcast_buffer.data(), nr_ent * 4);

  // Write out all the slice mapping table update commands.
  for (int i = 1; i <= nr_nodes(); i++) {
    if (i == node_id()) continue;
    auto out = all_nodes[i]->output_channel();
    out->Write(buf, buf_cnt);
    out->Flush();
  }

  broadcast_buffer.clear();
}

void NodeConfiguration::ContinueInboundPhase()
{
  for (auto tr: all_in_routines) {
    if (tr == nullptr) continue;
    tr->ContinueEpoch();
  }
}

void NodeConfiguration::CloseAndShutdown()
{
  for (auto tr: all_in_routines) {
    if (tr == nullptr) continue;
    tr->CloseChannel();
  }
}

}

namespace util {

felis::NodeConfiguration *InstanceInit<felis::NodeConfiguration>::instance;

}
