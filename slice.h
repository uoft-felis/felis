// -*- mode: c++ -*-

#ifndef _SLICE_H
#define _SLICE_H

#include <bitset>
#include <mutex>
#include <vector>

#include "util.h"
#include "node_config.h"
#include "shipping.h"

namespace felis {

static constexpr int kNrMaxSlices = 1 << 23;

class SliceScanner;
class ShippingHandle;

struct SliceQueue {
  std::mutex lock;
  util::ListNode queue;
  bool need_lock;
  size_t size;

  SliceQueue() : size(0), need_lock(false) {
    queue.Initialize();
  }

  void Append(ShippingHandle *handle);
};

/**
 * Slice is the granularity we handle the skew. Either by shipping data (which
 * is our baseline) or shipping index.
 *
 * Take TPC-C for example, a Slice will be a warehouse. Then the handles inside
 * of this slice will come from many different tables.
 *
 * To help the shipment scanner, we would like to sort the handles by their born
 * timestamp.
 */
class Slice {
  friend class SliceScanner;

  util::CacheAligned<SliceQueue> shared_q;
  std::array<util::CacheAligned<SliceQueue>, NodeConfiguration::kMaxNrThreads> per_core_q;
  int slice_id;
 public:
  Slice(int slice_id = 0);
  void Append(ShippingHandle *handle);
};

enum SliceOwnerType {
  PrimaryOwner, IndexOwner, DataOwner, NumOwnerTypes
};

class SliceMappingTable {
  struct {
    int id;
    std::bitset<kNrMaxSlices> owned[NumOwnerTypes];
  } slice_owners[NodeConfiguration::kMaxNrNode];
  // Mapping of real node id into an index for |slice_owners|.
  int node_compress[NodeConfiguration::kMaxNrNode];
  int nr_nodes;
  std::atomic<int> next_node;

 protected:
  void SetEntry(int slice_id, bool owned, SliceOwnerType type = IndexOwner,
                int node = -1, bool broadcast = true);
 public:
  // TODO: does this need to be locked?
  std::vector<int> broadcast_buffer;

  SliceMappingTable();
  void InitNode(int node_id);
  int LocateNodeLookup(int slice_id, SliceOwnerType = IndexOwner);
  std::vector<int> LocateNodeInsert(int slice_id, SliceOwnerType type = IndexOwner);

  void ReplayUpdate(int op);
  void AddEntry(int slice_id, SliceOwnerType type = IndexOwner, int node = -1,
                bool broadcast = true) {
    SetEntry(slice_id, true, type, node, broadcast);
  }

  void RemoveEntry(int slice_id, SliceOwnerType type = IndexOwner,
                   int node = -1, bool broadcast = true) {
    SetEntry(slice_id, false, type, node, broadcast);
  }
};


template <typename TableType>
class SliceLocator {
 public:
  int Locate(const typename TableType::Key &key);
  static SliceLocator<TableType> *instance;
};

}

namespace util {

template <typename TableType>
struct InstanceInit<felis::SliceLocator<TableType>> {
  static constexpr bool kHasInstance = true;
};

template <>
struct InstanceInit<felis::SliceMappingTable> {
  static constexpr bool kHasInstance = true;
  static felis::SliceMappingTable *instance;

  InstanceInit() {
    instance = new felis::SliceMappingTable();
  }
};

}

#endif
