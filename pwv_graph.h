#ifndef PWV_GRAPH_H
#define PWV_GRAPH_H

#include <cstdint>
#include <atomic>
#include <array>
#include "node_config.h"
#include "mem.h"
#include "util/objects.h"
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
    static constexpr auto kInlineEdges = 3;
    std::atomic_uint in_degree;
    uint16_t nr_resources; // out_degree
    uint16_t tot_resources;
    Edge inlined[kInlineEdges];
    Edge *extra;

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
  };
  static_assert(sizeof(Node) == 64);
  mem::Brk brk;
  Node *nodes;
 public:
  PWVGraph(int numa_node);
  void Reset();
  void ReserveEdge(uint64_t sid, int n = 1) {
    from_serial_id(sid)->tot_resources += n;
  }

  void AddResource(uint64_t sid, Resource res) {
    AddResources(sid, &res, 1);
  }
  void AddResources(uint64_t sid, Resource *res, int nr_res);
  void Build();
 private:
  Node *from_serial_id(uint64_t sid) const {
    int seq = 0x00FFFFFF & (sid >> 8);
    return nodes + seq - 1;
  }
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
