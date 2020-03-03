#include "tcp_node.h"

#include "shipping.h"
#include "slice.h"
#include "console.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"
#include "epoch.h"
#include "log.h"

namespace felis {
namespace tcp {

class SendChannel : public Flushable<SendChannel>, public OutgoingTraffic {
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
  void DoFlush(bool async = false);
  bool TryLock(int i) {
    bool locked = false;
    return channels[i]->lock.compare_exchange_strong(locked, true);
  }
  void Unlock(int i) {
    channels[i]->lock.store(false);
  }

  void WriteToNetwork(void *data, size_t cnt) final override {
    out->Write(data, cnt);
    out->Flush(true);
  }
};

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
    WriteToNetwork(buf, end - start);
    return true;
  } else {
    Unlock(tid);
    return channels[tid]->dirty.load();
  }
}

void SendChannel::DoFlush(bool async)
{
  if (async) {
    out->Flush(true);
    return;
  }

  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  auto &chn = channels[core_id];

  uint8_t signal = 0;
  logger->info("SendChannel signaling flusher");
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
    logger->info("FlusherRoutine done flushing SendChannel, next round");
  }
}

long SendChannel::PendingFlush(int core_id)
{
  // return channels[core_id]->flusher_cnt;
  return 0;
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

#if BG_RECEIVER
// Background receive coroutine
class NodeConnectionRoutine : public go::Routine {
  go::TcpSocket *sock;
  int idx;
  ulong src_node_id;
  std::atomic<long> tid;
  LocalDispatcherImpl lb;
  NodeConfiguration &conf;
  go::BufferChannel ctrl_chn;
 public:
  NodeConnectionRoutine(go::TcpSocket *client_sock, int idx)
      : sock(client_sock),
        idx(idx), src_node_id(0),
        tid(1),
        lb(idx),
        conf(util::Instance<NodeConfiguration>()),
        ctrl_chn(go::BufferChannel(128)) {
    set_reuse(true);
    client_sock->OmitReadLock();
  }
  void Flush() {
    lb.Flush();
  }
  int thread_pool_id() const { return tid.load(std::memory_order_relaxed); }
  virtual void Run() final;
  go::BufferChannel *control_channel() { return &ctrl_chn; }

 private:
  void UpdateBatchCounters();
  void UpdateSliceMappingTables(int nr_ops);
};

void NodeConnectionRoutine::Run()
{
  auto &conf = util::Instance<NodeConfiguration>();
  std::array<ulong, PromiseRoutineTransportService::kPromiseMaxLevels> nr_recv;
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

      logger->info("Sleeping src node {} because next phase isn't ready to start",
                   src_node_id);
      // Waiting for the signal to start
      if (!ctrl_chn.Read(&all_present, conf.g_nr_threads)) {
        logger->critical("EOF???");
        std::abort();
        break;
      }

      logger->info("Receiving from {} continues", src_node_id);
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

    if (cnt == conf.TotalBatchCounter(idx).load()) {
      logger->info("Flush from node {}, level = {}, cnt = {}",
                   src_node_id, level, cnt);
      nr_recv[level] = 0;
      Flush();
    }
  }
}

void NodeConnectionRoutine::UpdateBatchCounters()
{
  auto &conf = util::Instance<NodeConfiguration>();
  constexpr auto max_level = PromiseRoutineTransportService::kPromiseMaxLevels;
  auto nr_nodes = conf.nr_nodes();
  auto cmp = EpochClient::g_workload_client->completion_object();
  auto buffer_size = 8 + max_level * nr_nodes * nr_nodes * sizeof(ulong);
  auto *counters = (ulong *) alloca(buffer_size);
  auto in = sock->input_channel();
  auto total_cnt = 0;

  in->Read(counters, buffer_size);
  src_node_id = counters[0];

  for (int i = 0; i < max_level; i++) {
    fmt::memory_buffer buffer;
    fmt::format_to(buffer, "from node {} ", src_node_id);

    bool all_zero = true;
    for (int src = 0; src < nr_nodes; src++) {
      for (int dst = 0; dst < nr_nodes; dst++) {
        auto idx = conf.BatchBufferIndex(i, src + 1, dst + 1);
        auto cnt = counters[1 + idx];
        if (cnt == 0) continue;

        conf.TotalBatchCounter(idx).fetch_add(cnt);
        all_zero = false;

        if (dst + 1 == conf.node_id())
          total_cnt += cnt;
      }
    }

    if (all_zero) continue;

    // Print out debugging information
    fmt::format_to(buffer, "update: {}", i);
    for (int src = 0; src < nr_nodes; src++) {
      for (int dst = 0; dst < nr_nodes; dst++) {
        auto idx = conf.BatchBufferIndex(i, src + 1, dst + 1);
        auto cnt = counters[1 + idx];

        fmt::format_to(buffer, " {}->{}={}", src + 1, dst + 1,
                       conf.TotalBatchCounter(idx).load());
      }
    }
    logger->info("{}", std::string_view(buffer.begin(), buffer.size()));
  }
  logger->info("total_cnt {} from {}", total_cnt, src_node_id);
  // We now have the counter. We need to adjust the completion count with the
  // real counter.
  cmp->Complete(EpochClient::kMaxPiecesPerPhase - total_cnt);
}

void NodeConnectionRoutine::UpdateSliceMappingTables(int nr_ops)
{
  auto in = sock->input_channel();
  auto &table = util::Instance<SliceMappingTable>();
  uint32_t op;
  for (int i = 0; i < nr_ops; i++) {
    in->Read(&op, 4);
    table.ReplayUpdate(op);
  }
}

#endif

class ReceiverChannel : public IncomingTraffic {
  friend class felis::TcpNodeTransport;
  static constexpr auto kMaxMappingTableBuffer = 1024;
  go::TcpInputChannel *in;
  // We don't use the tcp socket lock, we use our own lock
  std::atomic_bool lock;
  felis::TcpNodeTransport *transport;
  std::atomic_long nr_left;
 public:
  ReceiverChannel(go::TcpSocket *sock, felis::TcpNodeTransport *transport)
      : IncomingTraffic(), in(sock->input_channel()), transport(transport) {
    sock->OmitReadLock();
    sock->OmitWriteLock();
    lock = false;
    nr_left = 0;
  }

  size_t Poll(PromiseRoutineWithInput *routines, size_t cnt);
 private:
  void Reset() {
    long expect = 0;
    if (!nr_left.compare_exchange_strong(expect, EpochClient::kMaxPiecesPerPhase)) {
      logger->info("Reset() failed, nr_left is {}", expect);
      std::abort();
    }
  }
  size_t PollRoutines(PromiseRoutineWithInput *routines, size_t cnt);
  bool PollMappingTable();
  void Complete(size_t n);
};

void ReceiverChannel::Complete(size_t n)
{
  if (n == 0) return;
  auto left = nr_left.fetch_sub(n) - n;
  abort_if(left < 0, "left {} < 0!", left);
  if (left == 0) {
    logger->info("Compelte {}", n);
    AdvanceStatus();
    abort_if(current_status() != Status::EndOfPhase, "Bogus current state! {}", (int) current_status());
  }
}

size_t ReceiverChannel::Poll(PromiseRoutineWithInput *routines, size_t cnt)
{
  bool keep_polling = false;
  size_t nr = 0;

  bool old = false;
  if (current_status() == Status::EndOfPhase
      || !lock.compare_exchange_strong(old, true))
    return 0;

  in->BeginPeek();
  do {
    auto s = current_status();
    switch (s) {
      case Status::PollMappingTable:
        keep_polling = PollMappingTable();
        break;
      case Status::PollRoutines:
        nr = PollRoutines(routines, cnt);
        keep_polling = false;
        break;
      case Status::EndOfPhase:
        break;
    }
  } while (keep_polling);
  in->EndPeek();

  lock = false;
  return nr;
}

size_t ReceiverChannel::PollRoutines(PromiseRoutineWithInput *routines, size_t cnt)
{
  uint64_t header;
  size_t i = 0;
  while (i < cnt) {
    if (in->Peek(&header, 8) < 8)
      break;

    if (((header >> 56) & 0xFF) == 0xFF) {
      abort_if (i == 0 && nr_left.load() > 0,
                "why there's a mapping table request??? nr_left {}",
                nr_left.load());
      // logger->info("Next phase comming up...");
      break;
    } else if (header == PromiseRoutine::kUpdateBatchCounter) {
      auto &conf = util::Instance<NodeConfiguration>();
      constexpr auto max_level = PromiseRoutineTransportService::kPromiseMaxLevels;
      auto nr_nodes = conf.nr_nodes();
      auto buffer_size = 8 + max_level * nr_nodes * nr_nodes * sizeof(ulong);
      auto buflen = 8 + buffer_size;
      auto buf = (uint8_t *) alloca(buflen);

      if (in->Peek(buf, buflen) < buflen)
        break;

      src_node_id = util::Instance<NodeConfiguration>().
                    UpdateBatchCountersFromReceiver((unsigned long *) (buf + 8));
      in->Skip(buflen);

      transport->OnCounterReceived();
    } else if (header & PromiseRoutine::kBubble) {
      // TODO:
      in->Skip(8);
    } else {
      abort_if(header % 8 != 0, "header isn't aligned {}", header);
      auto buflen = 8 + header;
      auto buf = (uint8_t *) alloca(buflen);
      if (in->Peek(buf, buflen) < buflen)
        break;
      routines[i++] = PromiseRoutine::CreateFromPacket(buf + 8, header);
      in->Skip(buflen);
    }
  }
  Complete(i);
  return i;
}

bool ReceiverChannel::PollMappingTable()
{
  uint64_t header;
  if (in->Peek(&header, 8) < 8)
    return false;
  abort_if(((header >> 56) & 0xFF) != 0xFF,
           "header isn't right for mappingtable update 0x{:x}", header);
  unsigned int nr_ops = header & std::numeric_limits<int32_t>::max();
  auto len = 4 + 4 * nr_ops;
  auto buflen = 8 + len;
  auto buf = (uint8_t *) alloca(buflen);
  abort_if(buflen > kMaxMappingTableBuffer,
           "MappingTable request is {}, larger than maximum {}",
           buflen, kMaxMappingTableBuffer);

  if (in->Peek(buf, buflen) < buflen)
    return false;

  Reset();
  auto data = (uint32_t *) (buf + 8);
  src_node_id = data[0];
  util::Instance<SliceMappingTable>()
      .UpdateSliceMappingTablesFromReceiver(nr_ops, data + 1);

  logger->info("Mapping table applied");
  AdvanceStatus();

  in->Skip(buflen);

  return true;
}

class NodeServerRoutine : public go::Routine {
  friend class felis::TcpNodeTransport;
  std::array<go::TcpSocket *, kMaxNrNode> incoming_socks;
  std::array<go::TcpSocket *, kMaxNrNode> outgoing_socks;

  std::array<SendChannel *, kMaxNrNode> outgoing_channels;
  std::array<ReceiverChannel *, kMaxNrNode> incoming_connection;
  felis::TcpNodeTransport *transport;
 public:
  NodeServerRoutine(felis::TcpNodeTransport *transport) : transport(transport) {}
  virtual void Run() final;
};

void NodeServerRoutine::Run()
{
  auto &console = util::Instance<Console>();

  auto server_sock = new go::TcpSocket(1024, 1024);
  auto &conf = util::Instance<NodeConfiguration>();
  auto &node_conf = conf.config();

  auto nr_nodes = conf.nr_nodes();

  // Reuse addr just for debugging
  int enable = 1;
  setsockopt(server_sock->fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

  abort_if(!server_sock->Bind("0.0.0.0", node_conf.worker_peer.port),
           "Cannot bind peer address");
  abort_if(!server_sock->Listen(kMaxNrNode),
           "Cannot listen");
  console.WaitForServerStatus(Console::ServerStatus::Connecting);
  // Now if anybody else tries to connect to us, it should be in the listener
  // queue. We are safe to call connect at this point. It shouldn't lead to
  // deadlock.
  for (auto &config: conf.all_configurations()) {
    if (!config) continue;
    util::Instance<SliceMappingTable>().InitNode(config->id);
    if (config->id == conf.node_id()) {
      util::Instance<SliceMappingTable>().AddEntry(100 + config->id, IndexOwner, config->id);
      continue;
    }

    logger->info("Connecting worker peer on node {}", config->id);
    go::TcpSocket *remote_sock = new go::TcpSocket(1024, 512 << 20);
    auto &peer = config->worker_peer;
    bool rs = remote_sock->Connect(peer.host, peer.port);
    abort_if(!rs, "Cannot connect to {}:{}", peer.host, peer.port);
    outgoing_socks[config->id] = remote_sock;
    outgoing_channels[config->id] = new SendChannel(remote_sock);
    conf.RegisterOutgoing(config->id, outgoing_channels[config->id]);
  }

  // Now we can begining to accept. Each client sock is a source for our Promise.
  // 0 is reserved for local source.
  //
  // The sources are different from nodes, and their orders are certainly
  // different from nodes too.
  for (size_t i = 1; i < nr_nodes; i++) {
    auto *client_sock = server_sock->Accept();
    if (client_sock == nullptr) continue;

    logger->info("New worker peer connection");
    incoming_socks[i - 1] = client_sock;

#if BG_RECEIVER
    auto *routine = new NodeConnectionRoutine(client_sock, i);
    incoming_connection_routines[i - 1] = routine;
    conf.RegisterIncomingControlChannel(i - 1, routine->control_channel());

    sched->WakeUp(routine);
#endif
    auto chn = new ReceiverChannel(client_sock, transport);
    logger->info("Incoming connection {}", (void *) chn);
    incoming_connection[i - 1] = chn;
    conf.RegisterIncoming(i - 1, chn);
  }
  console.UpdateServerStatus(Console::ServerStatus::Running);
}

}

TcpNodeTransport::TcpNodeTransport()
{
  if (NodeConfiguration::g_data_migration) {
    auto &peer = node_config().config().row_shipper_peer;
    go::GetSchedulerFromPool(node_config().g_nr_threads + 1)->WakeUp(
        new tcp::NodeRowShipmentReceiverRoutine(peer.host, peer.port));
  }
  logger->info("Starting node server with id {}", node_config().node_id());
  serv = new tcp::NodeServerRoutine(this);
  go::GetSchedulerFromPool(0)->WakeUp(serv);
}

void TcpNodeTransport::OnCounterReceived()
{
  auto &conf = util::Instance<NodeConfiguration>();
  if (counters.fetch_add(1) + 2 == conf.nr_nodes()) {
    for (int i = 0; i < conf.nr_nodes() - 1; i++) {
      auto r = serv->incoming_connection[i];
      auto s = conf.CalculateIncomingFromNode(r->src_node_id);
      logger->info("Counter stablized: src {} expecting {} pieces", r->src_node_id, s);
      r->Complete(EpochClient::kMaxPiecesPerPhase - s);
    }
    counters = 0;
  }
}

void TcpNodeTransport::TransportPromiseRoutine(PromiseRoutine *routine, const VarStr &in)
{
  auto &conf = node_config();
  auto src_node = conf.node_id();
  auto dst_node = routine->node_id == 0 ? src_node : routine->node_id;
  int level = routine->level;

  auto &meta = conf.batcher().GetLocalData(level, go::Scheduler::CurrentThreadPoolId() - 1);
  bool bubble = (in.data == (uint8_t *) PromiseRoutine::kBubblePointer);

  if (src_node != dst_node) {
    auto out = serv->outgoing_channels.at(dst_node);
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
    ltp.TransportPromiseRoutine(routine, in);
  }
  meta.AddRoute(dst_node);
}

void TcpNodeTransport::PreparePromisesToQueue(int core, int level, unsigned long nr)
{
  auto &conf = node_config();
  auto &meta = conf.batcher().GetLocalData(level, core);
  meta.IncrementExpected(nr);
}

void TcpNodeTransport::FinishPromiseFromQueue(PromiseRoutine *routine)
{
  if (routine == nullptr) {
    ltp.FinishPromiseFromQueue(nullptr);
    return;
  }
  auto &conf = node_config();
  auto src_node = conf.node_id();
  auto core = go::Scheduler::CurrentThreadPoolId() - 1;
  int level = routine->level;
  auto &meta = conf.batcher().GetLocalData(level, core);
  if (!meta.Finish())
    return;

  for (auto dst_node = 1; dst_node <= conf.nr_nodes(); dst_node++) {
    auto idx = conf.BatchBufferIndex(level, src_node, dst_node);
    auto target_cnt = conf.TotalBatchCounter(idx).load();
    auto cnt = conf.batcher().Merge(level, meta, dst_node);
    // printf("cnt %lu, target %lu\n", cnt, target_cnt);
    if (cnt == target_cnt && cnt > 0) {
      // Flush channels to this route
      if (dst_node != src_node) {
        auto chn = serv->outgoing_channels.at(dst_node);
        chn->Flush();
      } else {
        ltp.FinishPromiseFromQueue(routine);
      }
    }
  }
}

bool TcpNodeTransport::PeriodicIO(int core)
{
  auto &conf = node_config();

#if BG_RECEIVER
  // We don't need to flush from the background receiver thread's load
  // balancer buffer under cooperative IO. This is one of the reasons why
  // background receiver thread is a horrible idea. We should disable this
  // completely!
  for (int i = 1; i < conf.nr_nodes(); i++) {
    serv->incoming_connection_routines[i - 1]->Flush();
  }
#endif

  bool cont_io = false;
  for (int i = 1; i <= conf.nr_nodes(); i++) {
    if (i == conf.node_id()) continue;
    auto chn = serv->outgoing_channels.at(i);
    if (core == -1) {
      chn->Flush();
    } else {
      auto [success, did_flush] = chn->TryFlushForThread(core + 1);
      // chn->TryFlushForThread(0);
      if (success && did_flush)
        chn->DoFlush(true);
    }
  }

  for (int i = 0; i < conf.nr_nodes() - 1; i++) {
    auto recv = serv->incoming_connection.at(i);
    if (recv->current_status() == IncomingTraffic::Status::EndOfPhase) {
      continue;
    }

    cont_io = true;
    PromiseRoutineWithInput routines[128];
    auto nr_recv = recv->Poll(routines, 128);
    if (nr_recv > 0) {
      // We do not need to flush, because we are adding pieces to ourself!
      util::Impl<PromiseRoutineDispatchService>().Add(
          core, routines, nr_recv);
    }
  }

  ltp.FinishPromiseFromQueue(nullptr);
  return cont_io;
}


}
