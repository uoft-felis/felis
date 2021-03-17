#include "pwv_graph.h"
#include "epoch.h"
#include "util/objects.h"
#include "gopp/gopp.h"
#include "log.h"
#include "index_common.h"
#include "piece.h"

namespace felis {

RVPInfo *RVPInfo::FromRoutine(PieceRoutine *r)
{
  return (RVPInfo *) r->__padding__;
}

void RVPInfo::MarkRoutine(PieceRoutine *r, uint8_t cnt)
{
  auto info = FromRoutine(r);
  info->indegree = cnt;
  info->is_rvp = true;
}

size_t PWVGraph::g_extra_node_brk_limit = 8 << 20;

PWVGraph::PWVGraph(int numa_node)
{
  auto p = mem::AllocMemory(mem::Epoch, g_extra_node_brk_limit, numa_node);
  nodes = (Node *) mem::AllocMemory(
      mem::Epoch, sizeof(Node) * EpochClient::g_txn_per_epoch, numa_node);
  brk = mem::Brk(p, g_extra_node_brk_limit);
  // AddResources may be called from multiple cores! Although they work on different txns.
  brk.set_thread_safe(true);
}

void PWVGraph::Reset()
{
#ifdef SAFETY_CHECK
  for (unsigned int seq = 1; seq <= EpochClient::g_txn_per_epoch; seq++) {
    auto node = &nodes[seq - 1];
    if (node->nr_resources == 0) continue;
    abort_if(node->in_degree > 0, "seq {} isn't empty!!!", seq);
  }
#endif
  brk.Reset();
  memset(nodes, 0, sizeof(Node) * EpochClient::g_txn_per_epoch);
}

void PWVGraph::AddResources(uint64_t sid, Resource *res, int nr_res)
{
  auto node = from_serial_id(sid);
  auto s = node->nr_resources;

  if (node->tot_resources > Node::kInlineEdges && node->extra == nullptr) {
    node->extra = (Edge *) brk.Alloc(sizeof(Edge) * (node->tot_resources - Node::kInlineEdges));
    std::fill(node->extra, node->extra + (node->tot_resources - Node::kInlineEdges), Edge());
  }

  node->nr_resources += nr_res;
  abort_if(node->nr_resources > node->tot_resources,
           "sid {}'s edge isn't allocated enough {}, {}",
           sid, node->nr_resources, node->tot_resources);
  for (int i = 0; i < nr_res; i++) {
    auto rc = res[i];
    auto e = node->at(s + i);
    e->resource = rc;
    e->node = nullptr;
  }
  node->mark = true;
}

void PWVGraph::Build()
{
  auto current_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  PerfLog p;
  p.Start();
  for (unsigned int seq = 1; seq <= EpochClient::g_txn_per_epoch; seq++) {
    auto node = &nodes[seq - 1];
    if (!node->mark)
      continue;

    int in_degree = 0;
    for (int i = 0; i < node->nr_resources; i++) {
      auto rc = node->at(i)->resource;

      auto last_sid = *rc;
      if ((last_sid >> 32) == current_epoch_nr) {
        auto e = from_serial_id(last_sid)->FindEdge(rc);
        abort_if(e == nullptr,
                 "current seq {} (nr_resources {}) parent seq {}/sid {} cannot find resource {}",
                 seq, node->nr_resources, 0xffffff & (last_sid >> 8), last_sid, (void *) rc);
        e->node = node;
        in_degree++;
      }

      *rc = (current_epoch_nr << 32) | (seq << 8);
    }

    if (in_degree > 0) {
      node->in_degree.fetch_add(in_degree);
    } else {
      // abort_if(node->sched_entry == nullptr, "WHY? seq {}", seq);
      NotifyFree(node);
    }
  }
  p.Show("PWVGraph::Build takes");
}

void PWVGraph::NotifyFree(Node *node)
{
  if (node->sched_entry && node->on_node_free)
    node->on_node_free(node->sched_entry);
  abort_if(node->sched_entry == nullptr, "node->sched_entry not ingested? node {}", (void *) node);
}

void PWVGraph::RegisterSchedEntry(uint64_t sid, void *sched_entry)
{
  from_serial_id(sid)->sched_entry.store(sched_entry, std::memory_order_release);
}

void PWVGraph::RegisterFreeListener(uint64_t sid, void (*on_node_free)(void *))
{
  from_serial_id(sid)->on_node_free = on_node_free;
}

void PWVGraph::RegisterRVPListener(uint64_t sid, void (*on_node_rvp_change)(void *))
{
  from_serial_id(sid)->on_node_rvp_change = on_node_rvp_change;
}

void PWVGraph::NotifyRVPChange(uint64_t sid)
{
  auto node = from_serial_id(sid);
  if (node->sched_entry && node->on_node_rvp_change)
    node->on_node_rvp_change(node->sched_entry);
}

void PWVGraph::ActivateResources(uint64_t sid, Resource *res, int nr_res)
{
  auto node = from_serial_id(sid);
  abort_if(node->in_degree > 0,
           "node {} seq {} indegree {} shouldn't be scheduled! nr_resources {} tot_resources {}",
           (void *) node, 0x00ffffff & (sid >> 8), node->in_degree, node->nr_resources,
           node->tot_resources);

  for (int i = 0; i < nr_res; i++) {
    auto e = node->FindEdge(res[i]);
    if (unlikely(e == nullptr)) {
      fmt::memory_buffer buf = node->DumpEdges();
      logger->error("Cannot find edge for {} with {}, resources {}. {}/{}",
                    sid, (void *) res[i], std::string_view(buf.data(), buf.size()),
                    i, nr_res);
      std::abort();
    }
    if (unlikely(e->node == (Node *) 0xdeadbeef)) {
      auto buf = node->DumpEdges();
      fmt::memory_buffer res_buf;
      for (int j = 0; j <= i; j++)
        fmt::format_to(res_buf, "{} ", (void *) res[j]);

      logger->error("Duplicate Activation on node {}, {} resources {}",
                    (void *) node,
                    std::string_view(res_buf.data(), res_buf.size()),
                    std::string_view(buf.data(), buf.size()));
    }
    if (e->node) {
      e->node->in_degree.fetch_sub(1);
      if (e->node->in_degree == 0) {
        NotifyFree(e->node);
      }
      e->node = (Node *) 0xdeadbeef;
    }
  }
}

PWVGraphManager::PWVGraphManager()
{
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    graphs[i] = new PWVGraph(i / mem::kNrCorePerNode);
  }
}

PWVGraph *PWVGraphManager::local_graph()
{
  return graphs.at(go::Scheduler::CurrentThreadPoolId() - 1);
}

PWVGraph::Resource PWVGraph::VHandleToResource(VHandle *vhandle)
{
  auto p = (uint8_t *) vhandle;
  return (Resource) (p + 48);
}

}
