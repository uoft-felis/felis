// -*- mode: c++ -*-

#ifndef ENTITY_H
#define ENTITY_H

#include "vhandle.h"

namespace felis {

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
  bool ShouldSkip() { return false; }
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


class RowEntity {
  friend class RowShipmentReceiver;
  friend class SortedArrayVHandle;

  int rel_id;
  int slice;
  VarStr *k;
  VHandle *handle_ptr;
  ObjectShippingHandle<RowEntity> shandle;
 public:
  RowEntity(int rel_id, VarStr *k, VHandle *handle, int slice_id);
  RowEntity() : RowEntity(-1, nullptr, nullptr, -1) {}
  ~RowEntity() { delete k; }
  RowEntity(const RowEntity &rhs) = delete; // C++17 has gauranteed copy-ellision! :)

  ShippingHandle *shipping_handle() { return &shandle; }
  bool ShouldSkip();
  int EncodeIOVec(struct iovec *vec, int max_nr_vec);
  uint64_t encoded_len;

  void DecodeIOVec(struct iovec *vec);

  int slice_id() const { return slice; }

  static void *operator new(size_t s) {
    return pool.Alloc();
  }

  static void operator delete(void *p) {
    pool.Free(p);
  }

  static void InitPools();
  static mem::Pool pool;
};

using RowSliceScanner = ObjectSliceScanner<RowEntity>;
using RowShipment = felis::Shipment<RowEntity>;

class RowShipmentReceiver : public ShipmentReceiver<RowEntity> {
 public:
  RowShipmentReceiver(go::TcpSocket *sock) : ShipmentReceiver<RowEntity>(sock) {}
  ~RowShipmentReceiver() {delete sock;}

  void Run() override final;
};


}
#endif /* ENTITY_H */
