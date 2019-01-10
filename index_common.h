// -*- C++ -*-
#ifndef INDEX_COMMON_H
#define INDEX_COMMON_H

#include <memory>
#include <mutex>
#include <atomic>
#include <cstdlib>

#include "mem.h"
#include "log.h"
#include "util.h"

#include "vhandle.h"
#include "node_config.h"
#include "shipping.h"

namespace felis {

using util::ListNode;

class Checkpoint {
  static std::map<std::string, Checkpoint *> impl;
 public:
  static void RegisterCheckpointFormat(std::string fmt, Checkpoint *pimpl) { impl[fmt] = pimpl; }
  static Checkpoint *checkpoint_impl(std::string fmt) { return impl[fmt]; }
  virtual void Export() = 0;
};

// Relations is an index or a table. Both are associates between keys and
// rows.
//
// Since we are doing replay, we need two layers of indexing.
// First layer is indexing the keys.
// Second layer is the versioning layer. The versioning layers could be simply
// kept as a sorted array
class BaseRelation {
 protected:
  int id;
  bool read_only;
  size_t key_len;
 public:
  BaseRelation() : id(-1), read_only(false) {}

  void set_id(int relation_id) { id = relation_id; }
  int relation_id() { return id; }

  void set_key_length(size_t l) { key_len = l; }
  size_t key_length() const { return key_len; }

  void set_read_only(bool v) { read_only = v; }
  bool is_read_only() const { return read_only; }
};

class RelationManagerBase {
 public:
  RelationManagerBase() {}
};

template <class T>
class RelationManagerPolicy : public RelationManagerBase {
 protected:
 public:
  static constexpr int kMaxNrRelations = 1024;
  RelationManagerPolicy() {}

  T &GetRelationOrCreate(int fid) {
#ifndef NDEBUG
    abort_if(fid < 0 || fid >= kMaxNrRelations,
             "Cannot access {}, limit {}", fid, kMaxNrRelations);
#endif

    if (relations[fid].relation_id() == -1)
      relations[fid].set_id(fid);
    return relations[fid];
  }

  T &operator[](int fid) {
#ifndef NDEBUG
    abort_if(fid < 0 || fid >= kMaxNrRelations || relations[fid].relation_id() == -1,
             "Cannot access {}? Is it initialized? limit {}", fid, kMaxNrRelations);
#endif
    return relations[fid];
  }

 protected:
  std::array<T, kMaxNrRelations> relations;
};

class IndexShipmentReceiver;

// A key-value pair, and thankfully, this is immutable.
class IndexEntity {
  friend class IndexShipmentReceiver;
  int rel_id;
  VarStr *k;
  VHandle *handle_ptr;
  ObjectShippingHandle<IndexEntity> shandle;
 public:
  IndexEntity()
      : rel_id(-1), k(nullptr), handle_ptr(nullptr), shandle(this) {}
  IndexEntity(int rel_id, VarStr *k, VHandle *handle)
      : rel_id(rel_id), k(k), handle_ptr(handle), shandle(this) {}
  ~IndexEntity();
  IndexEntity(const IndexEntity &rhs) = delete; // C++17 has gauranteed copy-ellision! :)

  ShippingHandle *shipping_handle() { return &shandle; }
  int EncodeIOVec(struct iovec *vec, int max_nr_vec);
  uint64_t encoded_len;

  void DecodeIOVec(struct iovec *vec);
};

using IndexSliceScanner = ObjectSliceScanner<IndexEntity>;
using IndexShipment = felis::Shipment<felis::IndexEntity>;

class IndexShipmentReceiver : public ShipmentReceiver<IndexEntity> {
 public:
  IndexShipmentReceiver(go::TcpSocket *sock) : ShipmentReceiver<IndexEntity>(sock) {}
  ~IndexShipmentReceiver();

  void Run() override final;
};

template <class IndexPolicy>
class RelationPolicy : public BaseRelation,
		       public IndexPolicy {
 public:

  VHandle *SetupReExec(const VarStr *k, uint64_t sid, uint64_t epoch_nr, VarStr *obj = (VarStr *) kPendingValue) {
    auto handle = this->InsertOrCreate(k);
    while (!handle->AppendNewVersion(sid, epoch_nr)) {
      asm("pause" : : :"memory");
    }
    if (obj != (void *) kPendingValue) {
      abort_if(!handle->WriteWithVersion(sid, obj, epoch_nr),
               "Diverging outcomes during setup setup");
    }
    return handle;
  }

  VHandle *InitValue(const VarStr *k, VarStr *obj) {
    return SetupReExec(k, 0, 0, obj);
  }

  bool SetupReExecSync(const VarStr *k, uint64_t sid, uint64_t epoch_nr) {
    SetupReExec(k, sid, epoch_nr);
    return true;
  }

  VHandle *SetupReExecAsync(const VarStr *k, uint64_t sid, uint64_t epoch_nr) {
    auto handle = this->InsertOrCreate(k);
    if (!handle->AppendNewVersion(sid, epoch_nr))
      return nullptr;
    return handle;
  }
};

class DataSlicer {
 protected:
  Slice **index_slices;
  IndexSliceScanner **index_slice_scanners;
  Slice **row_slices;
  RowSliceScanner **row_slice_scanners;
  size_t nr_slices;
 private:
  template <typename T> friend T &util::Instance() noexcept;
  DataSlicer() {}
 public:
  void Initialize(int nr_slices);
  void InstallIndexSlice(int i, IndexShipment *shipment) {
    index_slices[i] = new Slice();
    index_slice_scanners[i] = new IndexSliceScanner(index_slices[i], shipment);
  }
  void InstallRowSlice(int i, RowShipment *shipment) {
    row_slices[i] = new Slice();
    row_slice_scanners[i] = new RowSliceScanner(row_slices[i], shipment);
  }

  IndexEntity *OnNewRow(int slice_id, IndexEntity *ent) {
    return OnNewRow(index_slices, index_slice_scanners, slice_id, ent);
  }
  RowEntity *OnNewRow(int slice_id, RowEntity *ent) {
    return OnNewRow(row_slices, row_slice_scanners, slice_id, ent);
  }
  RowEntity *OnUpdateRow(int slice_id, RowEntity *ent) {
    return OnUpdateRow(row_slice_scanners, slice_id, ent);
  }

  template <typename TableType, typename KeyType>
  void OnNewRow(int slice_id, TableType table, const KeyType &k, VHandle *handle) {
    auto *kstr = k.Encode();
    OnNewRow(slice_id, new felis::IndexEntity(int(table), kstr, handle));
    OnNewRow(slice_id, new felis::RowEntity(int(table), kstr, handle, slice_id));
  }

  void OnUpdateRow(VHandle *handle) {
    auto *ent = handle->row_entity.get();
    OnUpdateRow(ent->slice_id(), ent);
  }

  std::vector<IndexShipment*> all_index_shipments();

  void ScanAllIndex() { ScanAll(index_slice_scanners); }
  void ScanAllRow() { ScanAll(row_slice_scanners); }

 private:
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

}

#endif /* INDEX_COMMON_H */
