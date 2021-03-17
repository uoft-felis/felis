#include <algorithm>
#include <sys/types.h>
#include <sys/socket.h>

#include "json11/json11.hpp"
#include "node_config.h"
#include "console.h"
#include "epoch.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"
// #include "index_common.h"

#include "slice.h"
#include "opts.h"

namespace felis {

void TransportBatcher::Init(int nr_nodes, int nr_cores)
{
  LocalMetadata *mem = nullptr;
  for (auto i = 0; i < nr_cores; i++) {
    auto d = std::div(i, mem::kNrCorePerNode);
    auto numa_node = d.quot;
    auto numa_offset = d.rem;
    if (numa_offset == 0) {
      mem = (LocalMetadata *) mem::AllocMemory(
          mem::Promise,
          sizeof(LocalMetadata) * kMaxLevels * mem::kNrCorePerNode,
          numa_node);
    }
    thread_local_data[i] = mem + kMaxLevels * numa_offset;
  }
}

void TransportBatcher::Reset(int nr_nodes, int nr_cores)
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

unsigned long TransportBatcher::Merge(int level, LocalMetadata &local, int node)
{
  auto v = local.delta[node - 1];
  local.delta[node - 1] = 0;
  return counters[level][node - 1].fetch_add(v) + v;
}

LocalDispatcherImpl::LocalDispatcherImpl(int idx)
    : idx(idx), dice(0)
{
  auto mem =
      (Queue *) mem::AllocMemory(mem::EpochQueueItem, sizeof(Queue), -1);

  for (int i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    if (i > 0) {
      auto d = std::div(i - 1, mem::kNrCorePerNode);
      if (d.rem == 0) {
        mem = (Queue *) mem::AllocMemory(
            mem::EpochQueueItem, sizeof(Queue) * mem::kNrCorePerNode, d.quot);
      }
      queues[i] = new (mem + d.rem) Queue();
    } else {
      queues[0] = new (mem) Queue();
    }
  }
}

void LocalDispatcherImpl::QueueRoutine(PieceRoutine *routine)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  auto q = queues[tid];
  auto nr_threads = NodeConfiguration::g_nr_threads;
  auto pos = q->append_start.load(std::memory_order_acquire);
  q->routines[pos] = routine;
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

void LocalDispatcherImpl::DoFlush()
{
  BasePieceCollection::FlushScheduler();
}

bool LocalDispatcherImpl::PushRelease(int tid, unsigned int start, unsigned int end)
{
  FlushOnCore(tid, start, end);
  Unlock(tid);
  return end > start;
}

void LocalDispatcherImpl::FlushOnCore(int tid, unsigned int start, unsigned int end)
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
      auto r = q->routines[j];
      auto core = 0;
      if (r->affinity < nr_threads) {
        core = r->affinity;
      } else {
        core = (delta + j - start) % nr_threads;
      }
      q->task_buffer[core]
          .routines[q->task_buffer[core].nr++] = r;
    }
    for (int i = 0; i < nr_threads; i++) {
      SubmitOnCore(q->task_buffer[i].routines.data(), 0, q->task_buffer[i].nr, i + 1);
    }
  }
}

void LocalDispatcherImpl::SubmitOnCore(PieceRoutine **routines, unsigned int start, unsigned int end, int thread)
{
  if (start == end) return;

  constexpr auto max_level = PromiseRoutineTransportService::kPromiseMaxLevels;
  abort_if(thread == 0 || thread > NodeConfiguration::g_nr_threads,
           "{} is invalid, start {} end {}", thread, start, end);

  BasePieceCollection::QueueRoutine(routines + start, end - start, thread - 1);
}


LocalTransport::LocalTransport() : lb(new LocalDispatcherImpl(0)) {}
LocalTransport::~LocalTransport() { delete lb; }

void LocalTransport::TransportPromiseRoutine(PieceRoutine *routine)
{
  lb->QueueRoutine(routine);
}

void LocalTransport::Flush() { lb->Flush(); }
bool LocalTransport::TryFlushForCore(int core_id)
{
  auto [success, ___] = lb->TryFlushForThread(core_id + 1);
  return success;
}

size_t NodeConfiguration::g_nr_threads = 8;
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
    for (auto i = max_node_id + 1; i < all_config.size(); i++) all_config[i] = std::nullopt;
    logger->info("Limit number of node to {}", max_node_id);
  }

  auto nr = PromiseRoutineTransportService::kPromiseMaxLevels * nr_nodes() * nr_nodes();
  total_batch_counters = new std::atomic_ulong[nr];
  local_batch = new (malloc(sizeof(LocalBatch) + sizeof(std::atomic_ulong) * nr)) LocalBatch;

  std::fill(total_batch_counters, total_batch_counters + nr, 0);
  std::fill(local_batch->counters, local_batch->counters + nr, 0);
  local_batch->magic = PieceRoutine::kUpdateBatchCounter;

  outgoing.fill(nullptr);
  incoming.fill(nullptr);

  BasePieceCollection::g_nr_threads = g_nr_threads;

  transport_batcher.Init(nr_nodes(), g_nr_threads);
  ResetBufferPlan();
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

void NodeConfiguration::ResetBufferPlan()
{
  local_batch_completed = 0;
  auto nr = PromiseRoutineTransportService::kPromiseMaxLevels * nr_nodes() * nr_nodes();
  std::fill(local_batch->counters,
            local_batch->counters + nr,
            0);
  std::fill(total_batch_counters,
            total_batch_counters + nr,
            0);
  transport_batcher.Reset(nr_nodes(), g_nr_threads);
}

void NodeConfiguration::CollectBufferPlan(BasePieceCollection *root, unsigned long *cnts)
{
  auto src_node = node_id();
  for (size_t i = 0; i < root->nr_routines(); i++) {
    auto *routine = root->routine(i);
    CollectBufferPlanImpl(routine, cnts, 0, src_node);
  }
}

void NodeConfiguration::CollectBufferPlanImpl(PieceRoutine *routine, unsigned long *cnts,
                                              int level, int src_node)
{
  abort_if(level >= PromiseRoutineTransportService::kPromiseMaxLevels,
           "promise level {} too deep", level);
  routine->level = level;

  auto dst_node = routine->node_id;
  if (dst_node == 0)
    dst_node = src_node;
  if (dst_node < 255) {
    cnts[BatchBufferIndex(level, src_node, dst_node)]++;
  } else {
    // Dynamic piece. We need to increment for all dst node.
    for (auto d = 1; d <= nr_nodes(); d++) {
      cnts[BatchBufferIndex(level, src_node, d)]++;
    }
  }

  if (routine->next == nullptr)
    return;

  for (size_t i = 0; i < routine->next->nr_routines(); i++) {
    auto *subroutine = routine->next->routine(i);
    CollectBufferPlanImpl(subroutine, cnts, level + 1, dst_node);
  }
}

bool NodeConfiguration::FlushBufferPlan(unsigned long *per_core_cnts)
{
  constexpr auto max_level = PromiseRoutineTransportService::kPromiseMaxLevels;
  for (int i = 0; i < max_level; i++) {
    for (int src = 0; src < nr_nodes(); src++) {
      for (int dst = 0; dst < nr_nodes(); dst++) {
        auto idx = BatchBufferIndex(i, src + 1, dst + 1);
        auto counter = per_core_cnts[idx];
        local_buffer_plan_counters()[idx].fetch_add(counter);
        total_batch_counters[idx].fetch_add(counter);

        if (counter == 0) continue;

        if (dst + 1 == node_id()) {
          trace(TRACE_COMPLETION "Increment {} of pieces from local counters", counter);
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
    auto out = outgoing[id];
    out->WriteToNetwork(
        local_batch,
        16 + max_level * nr_nodes() * nr_nodes() * sizeof(unsigned long));
    out->DoFlush(false);
  }

  logger->info("Done Flushing buffer plan on {}", node_id());
  return true;
}

int NodeConfiguration::UpdateBatchCountersFromReceiver(unsigned long *data)
{
  auto src_node_id = data[0];
  constexpr auto max_level = PromiseRoutineTransportService::kPromiseMaxLevels;
  auto total_cnt = 0;

  for (int i = 0; i < max_level; i++) {
    bool all_zero = true;
    for (int src = 0; src < nr_nodes(); src++) {
      for (int dst = 0; dst < nr_nodes(); dst++) {
        auto idx = BatchBufferIndex(i, src + 1, dst + 1);
        auto cnt = data[1 + idx];
        if (cnt == 0) continue;

        TotalBatchCounter(idx).fetch_add(cnt);
        all_zero = false;

        if (dst + 1 == node_id())
          total_cnt += cnt;
      }
    }

    if (all_zero) continue;

    // Print out debugging information
    fmt::memory_buffer buffer;
    fmt::format_to(buffer, "from node {} update level {}: ", src_node_id, i);
    for (int src = 0; src < nr_nodes(); src++) {
      for (int dst = 0; dst < nr_nodes(); dst++) {
        auto idx = BatchBufferIndex(i, src + 1, dst + 1);
        auto cnt = data[1 + idx];

        fmt::format_to(buffer, " {}->{}={}({})", src + 1, dst + 1, cnt, TotalBatchCounter(idx).load());
      }
    }
    logger->info("{}", std::string_view(buffer.begin(), buffer.size()));
  }

  logger->info("total_cnt {} from {}, adjusting the completion counter",
               total_cnt, src_node_id);
  // We now have the counter. We need to adjust the completion count with the
  // real counter.
  EpochClient::g_workload_client->completion_object()->Complete(
      EpochClient::kMaxPiecesPerPhase - total_cnt);
  return src_node_id;
}

size_t NodeConfiguration::CalculateIncomingFromNode(int src)
{
  int dst = node_id();
  size_t s = 0;
  constexpr auto max_level = PromiseRoutineTransportService::kPromiseMaxLevels;
  for (int i = 0; i < max_level; i++) {
    auto idx = BatchBufferIndex(i, src, dst);
    s += TotalBatchCounter(idx).load();
  }
  return s;
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

  logger->info("Send StartPhase");
  // Write out all the slice mapping table update commands.
  for (int i = 1; i <= nr_nodes(); i++) {
    if (i == node_id()) continue;
    auto out = outgoing[i];
    out->WriteToNetwork(buf, buf_cnt);
  }
  logger->info("Done Sending Start Phase");

  broadcast_buffer.clear();
}

void NodeConfiguration::ContinueInboundPhase()
{
  for (auto t: incoming) {
    if (t == nullptr) break;
    abort_if(t->current_status() != IncomingTraffic::Status::EndOfPhase,
             "Cannot tell the incoming traffic to start polling the next phase,"
             " the current phase has not finished!");
    logger->info("ContinueInbound");
    t->AdvanceStatus();
  }
}

void NodeConfiguration::CloseAndShutdown()
{
  // TODO:
  for (auto t: incoming) {
    if (t == nullptr) continue;
  }
}

}
