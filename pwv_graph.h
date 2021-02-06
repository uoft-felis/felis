#ifndef PWV_GRAPH_H
#define PWV_GRAPH_H

#include <cstdint>
#include <atomic>
#include <array>
#include "node_config.h"
#include "mem.h"
#include "util/objects.h"
#include "util/linklist.h"
#include "index_common.h"

namespace felis {

class VHandle;

class PWVGraph {
 public:
  using Resource = uint64_t *;
  static Resource VHandleToResource(VHandle *handle);
 private:
  struct Node;
  struct Edge {
    Resource resource;
    Node *node;
  };
  struct Node {
    static constexpr auto kInlineEdges = 2;
    std::atomic_uint in_degree;
    uint16_t nr_resources; // out_degree
    uint16_t tot_resources;

    void *sched_entry;
    void (*on_node_free)(void *);

    Edge *extra;
    Edge inlined[kInlineEdges];

    Edge *at(uint8_t n) {
      if (n < kInlineEdges) return inlined + n;
      if (n < tot_resources) return extra + (n - kInlineEdges);
      return nullptr;
    }

    Edge *FindEdge(Resource r) {
      for (int i = 0; i < nr_resources; i++) {
        auto e = at(i);
        if (e->resource == r)
          return e;
      }
      return nullptr;
    }

    fmt::memory_buffer DumpEdges() {
      fmt::memory_buffer buf;
      for (int n = 0; n < nr_resources; n++)
        fmt::format_to(buf, "{}({}) ", (void *) at(n)->resource, (void *) at(n)->node);
      return buf;
    }
  };
  static_assert(sizeof(Node) == 64);

  util::GenericListNode<Node> active;
  int nr_active;
  mem::Brk brk;
  Node *nodes;
 public:
  PWVGraph(int numa_node);
  void Reset();
  void ReserveEdge(uint64_t sid, int n = 1) {
    auto node = from_serial_id(sid);
    abort_if(node->nr_resources != 0, "Cannot reserve edge existing resources added!");
    node->tot_resources += n;
  }

  void AddResource(uint64_t sid, Resource res) {
    AddResources(sid, &res, 1);
  }
  void AddResources(uint64_t sid, Resource *res, int nr_res);
  void Build();

  bool is_node_free(uint64_t sid) const {
    return from_serial_id(sid)->in_degree == 0;
  }

  void RegisterFreeListener(uint64_t sid, void *sched_entry, void (*on_node_free)(void *));

  void ActivateResources(uint64_t sid, Resource *res, int nr_res);
  void ActivateResource(uint64_t sid, Resource res) {
    ActivateResources(sid, &res, 1);
  }

  fmt::memory_buffer DumpEdges(uint64_t sid) {
    return from_serial_id(sid)->DumpEdges();
  }

 private:
  Node *from_serial_id(uint64_t sid) const {
    int seq = 0x00FFFFFF & (sid >> 8);
    return nodes + seq - 1;
  }
  void NotifyFree(Node *node) const;
};

class PWVGraphManager {
  std::array<PWVGraph *, NodeConfiguration::kMaxNrThreads> graphs;
 public:
  PWVGraphManager();
  PWVGraph *operator[](int idx) { return graphs.at(idx); }
  PWVGraph *local_graph();
};

}

namespace util {

template <> struct InstanceInit<felis::PWVGraphManager> {
  static constexpr bool kHasInstance = true;
  static inline felis::PWVGraphManager *instance = nullptr;
};

}

#endif /* PWV_GRAPH_H */
