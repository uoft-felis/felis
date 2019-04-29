// -*- mode: c++ -*-

#ifndef SLICE_H
#define SLICE_H

#include <bitset>
#include <mutex>
#include <vector>

#include "util.h"
#include "node_config.h"
#include "shipping.h"
#include "log.h"
#include "entity.h"

namespace felis {

static constexpr int kNrMaxSlices = 256;

class SliceScanner;
class ShippingHandle;

// SliceQueue is a queue of ShippingHandles, with /born/ in ascending order.
// It is in fact a queue for Entities.
// In a index slice, the queue are all IndexEntities; likewise in a row slice.
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
 *
 * As for implementation, Slice contains multiple SliceQueues: one for each core,
 * and one shared queue.
 *
 * All the Entities will be stored in the Slice right after the row has been
 * created, for the sake of quick scanning through when shipping is needed.
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

class VHandle;

// Global Instance, manages all the Slices and ObjectSliceScanners.
// Each Slice has several SliceQueues, and one Shipment (in ObjectSliceScanner).
class SliceManager {
 protected:
  Slice **row_slices;
  RowSliceScanner **row_slice_scanners;
  size_t nr_slices;
 private:
  template <typename T> friend T &util::Instance() noexcept;
  SliceManager() {}
 public:
  void Initialize(int nr_slices);
  void InstallRowSlice(int i, RowShipment *shipment) {
    row_slices[i] = new Slice();
    row_slice_scanners[i] = new RowSliceScanner(row_slices[i], shipment);
  }

  void OnNewRow(int slice_id, int table, VarStr *kstr, VHandle *handle) {
    OnNewRow(slice_id, new felis::RowEntity(table, kstr, handle, slice_id));
  }

  void OnUpdateRow(VHandle *handle) {
    auto *ent = handle->row_entity.get();
    OnUpdateRow(ent->slice_id(), ent);
  }

  std::vector<RowShipment*> all_row_shipments();

  // only the slices which (shipment != nullptr) will be scanned
  void ScanAllRow() { ScanAll(row_slice_scanners); }
  void ScanShippingHandle();

 private:
  RowEntity *OnNewRow(int slice_id, RowEntity *ent) {
    return OnNewRow(row_slices, row_slice_scanners, slice_id, ent);
  }
  RowEntity *OnUpdateRow(int slice_id, RowEntity *ent) {
    return OnUpdateRow(row_slice_scanners, slice_id, ent);
  }

  template <typename T, typename ScannerType>
  T *OnNewRow(Slice ** slices, ScannerType ** scanners, int slice_idx, T *ent) {
    slices[slice_idx]->Append(ent->shipping_handle());
    // We still need to call MarkDirty() just in case the scanner is running in
    // progress.
    return OnUpdateRow(scanners, slice_idx, ent);
  }

  template <typename T, typename ScannerType>
  T *OnUpdateRow(ScannerType **scanners, int slice_idx, T* ent) {
    if (ent->shipping_handle()->MarkDirty()) {
      scanners[slice_idx]->AddObject(ent);
    }
    return ent;
  }

  template <typename ScannerType> void ScanAll(ScannerType ** scanners) {
    SliceScanner::ScannerBegin();
    for (int i = 0; i < nr_slices; i++) {
      if (scanners[i] == nullptr)
        continue;
      scanners[i]->Scan();
    }
    SliceScanner::ScannerEnd();
  }
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
  int16_t Locate(const typename TableType::Key &key);
};

#define SHARD_TABLE(Table)                                              \
  template <> inline int16_t SliceLocator<Table>::Locate(const Table::Key &key) \

static constexpr int16_t kReadOnlySliceId = std::numeric_limits<int16_t>::min();

// Read only tables are replicated all over the place
#define READ_ONLY_TABLE(Table)                          \
  SHARD_TABLE(Table) { return kReadOnlySliceId; }       \

using SliceRoute = int (*)(int16_t slice_id);

}

namespace util {

template <typename TableType>
struct InstanceInit<felis::SliceLocator<TableType>> {
  static constexpr bool kHasInstance = true;
  static inline felis::SliceLocator<TableType> *instance;

  InstanceInit() {
    instance = new felis::SliceLocator<TableType>();
  }
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

#endif /* SLICE_H */
