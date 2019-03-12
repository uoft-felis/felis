// -*- mode: c++ -*-

#ifndef _SLICE_H
#define _SLICE_H

#include <mutex>
#include <vector>
#include "util.h"
#include "node_config.h"

namespace felis {

class SliceScanner;
class ShippingHandle;

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

  util::CacheAligned<SliceQueue> shared_q;
  std::array<util::CacheAligned<SliceQueue>, NodeConfiguration::kMaxNrThreads> per_core_q;
  int slice_id;
 public:
  Slice(int slice_id = 0);
  void Append(ShippingHandle *handle);
};

class SliceMappingTable {
 public:
  // TODO:
  int LocateNodeLookup(int slice_id);
  std::vector<int> LocateNodeInsert(int slice_id);
  void AddEntry();
  void RemoveEntry();
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
  static constexpr bool kHashInstance = true;
};

}

#endif
