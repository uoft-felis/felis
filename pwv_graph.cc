#include "pwv_graph.h"
#include "epoch.h"
#include "util/objects.h"
#include "gopp/gopp.h"
#include "log.h"
#include "index_common.h"

namespace felis {

PWVGraph::PWVGraph(int numa_node)
{
  auto p = mem::AllocMemory(
      mem::Epoch, 8 << 20, numa_node);
  nodes = (Node *) mem::AllocMemory(
      mem::Epoch, sizeof(Node) * EpochClient::g_txn_per_epoch, numa_node);
  brk = mem::Brk(p, 8 << 20);
}

void PWVGraph::Reset()
{
  brk.Reset();
  memset(nodes, 0, sizeof(Node) * EpochClient::g_txn_per_epoch);
}

void PWVGraph::AddResources(uint64_t sid, Resource *res, int nr_res)
{
  auto node = from_serial_id(sid);
  auto s = node->nr_resources;

  if (node->tot_resources > Node::kInlineEdges && node->extra == nullptr) {
    node->extra = (Edge *) brk.Alloc(sizeof(Edge) * (node->tot_resources - Node::kInlineEdges));
  }

  node->nr_resources += nr_res;
  abort_if(node->nr_resources > node->tot_resources,
           "sid {}'s edge isn't allocated enough {}, {}",
           sid, node->nr_resources, node->tot_resources);
  int in_degree = 0;
  for (int i = 0; i < nr_res; i++) {
    auto rc = res[i];
    node->at(s + i)->resource = rc;
  }
  node->in_degree.fetch_add(in_degree);
}

void PWVGraph::Build()
{
  auto current_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  PerfLog p;
  p.Start();
  for (unsigned int seq = 1; seq <= EpochClient::g_txn_per_epoch; seq++) {
    auto node = &nodes[seq - 1];
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
    node->in_degree.fetch_add(in_degree);
  }
  p.Show("PWVGraph::Build takes");
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
