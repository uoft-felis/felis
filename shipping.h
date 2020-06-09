#ifndef SHIPPING_H_
#define SHIPPING_H_

#include <atomic>
#include <mutex>
#include <list>
#include <climits>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/tcp.h>
#include <netinet/ip.h>

#include "gopp/channels.h"
#include "log.h"
#include "node_config.h"

namespace felis {

class Slice;
class SliceQueue;

class ShippingHandle : public util::ListNode {
  /**
   * Which scanning session was this handle born?
   *
   * If the scanning session X is still scanning, then only add to the shipment
   * when born == X. Because any born < X will be taken care of by the scan.
   *
   * If the scanning session X has finished scanning, then always add to the
   * shipment.
   */
  uint64_t born;
  std::atomic_ullong generation;
  std::atomic_ullong sent_generation;
 public:
  ShippingHandle();
  /**
   * This is not MT-Safe. You are not suppose to write to the same object
   * concurrently anyway.
   *
   * @return if the handle should be put into a queue for sending
   */
  bool MarkDirty();

  /**
   * This is not MT-safe either. You are not suppose to sent the same object
   * using multiple threads.
   */
  void PrepareSend();

  /**
   *
   * @return if the handle should be put into queue by the scanner
   */
  bool CheckSession();

  // for analysis in ScanShippingHandle()
  unsigned long long GetGeneration() { return generation.load(std::memory_order_acquire); }
};

template <typename T>
struct ObjectShippingHandle : public ShippingHandle {
  T *object;

  ObjectShippingHandle(T *object) : ShippingHandle(), object(object) {}
};


// SliceScanner is per slice, responsible for getting Entities from SliceQueues
// then ObjectSliceScanner can add them into its Shipment
class SliceScanner {
 protected:
  Slice * slice;

  SliceQueue * current_q;
  util::ListNode * current_node;

  SliceScanner(Slice * slice);

  ShippingHandle *GetNextHandle();
  void ResetCursor();
 public:
  static void ScannerBegin();
  static void ScannerEnd();
  static void MigrationApproachingEnd();
  static void MigrationEnd();
  static void StatAddObject();
  static bool IsConverging();
};

// Class for sending IOVec.
class BaseShipment {
 public:
  static constexpr int kSendBatch = 32 * IOV_MAX;
 protected:
  sockaddr_in addr;
  int fd;
  bool connected;
  bool finished;
  std::mutex lock;

  void SendIOVec(struct iovec *vec, int nr_vec);
  void ReceiveACK();

  bool has_finished() const { return finished; }
  std::mutex &mutex() { return lock; }
 public:
  BaseShipment(int fd) : fd(fd), connected(true), finished(false) {}
  BaseShipment(std::string host, unsigned int port, bool defer_connect = false);
 private:
  void Connect();
};

// g_objects_added includes shipped and skipped
static std::atomic_ulong g_objects_shipped = 0;
static std::atomic_ulong g_objects_skipped = 0;
static std::atomic_ullong g_bytes_sent = 0;

/**
 * T is a concept:
 *
 * // returns how many iovec encoded, 0 means not enough
 * int EncodeIOVec(struct iovec *vec, int max_nr_vec);
 * void DecodeIOVec(struct iovec *vec);
 *
 * uint64_t encoded_len; // Stored the total data length of EncodeIOVec();
 *
 * Shipment is a queue of Entity, waiting to be sent.
 * Calling it "Shipment Queue" would be easier to understand.
 */
template <typename T>
class Shipment : public BaseShipment {
 protected:
  std::list<T *> queue;
 public:
  using BaseShipment::BaseShipment;

  void AddObject(T *object) {
    std::lock_guard _(lock);
    queue.push_front(object);
  }

  bool RunSend() {
    T *obj[kSendBatch];
    int nr_obj = 0;
    struct iovec vec[IOV_MAX];
    {
      std::lock_guard _(lock);
      while (nr_obj < kSendBatch && !queue.empty()) {
        obj[nr_obj++] = queue.back();
        queue.pop_back();
      }
      if (nr_obj == 0) {
        finished = true;
        close(fd);
        SliceScanner::MigrationEnd();
        return true;
      }
    }

    int i = 0;
    int cur_iov = 0;
    int enabled = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_CORK, &enabled, 4);

    while (i < nr_obj) {
      if (obj[i]->ShouldSkip()) {
        g_objects_skipped.fetch_add(1);
        i++;
        continue;
      }
      int n = 0;
      if (cur_iov == IOV_MAX
          || (n = obj[i]->EncodeIOVec(&vec[cur_iov + 1], IOV_MAX - cur_iov - 1)) == 0) {
        SendIOVec(vec, cur_iov);
        cur_iov = 0;
        continue;
      }
      vec[cur_iov].iov_len = 8;
      vec[cur_iov].iov_base = &obj[i]->encoded_len;

      cur_iov += n + 1;
      g_bytes_sent.fetch_add(obj[i]->encoded_len);
      g_objects_shipped.fetch_add(1);
      i++;
    }
    SendIOVec(vec, cur_iov);
    ReceiveACK();
    return false;
  }
};

//
// Making the ShipmentReceiver a go-routine has a lot of benefits.
//
// 1. We have tons of very small read() in the process. The go-routine channel
// can help us buffer and issue less system calls.
//
// 2. We need to create ThreadInfo to use MassTree and the index will create
// that for you if you are a go-routine.
//
template <typename T>
class ShipmentReceiver : public go::Routine {
 protected:
  go::TcpSocket *sock;
 public:
  ShipmentReceiver(go::TcpSocket *sock) : sock(sock) {}

  bool Receive(T *shipment) {
    auto *in = sock->input_channel();
    auto *out = sock->output_channel();

 again:
    uint64_t psz;
    if (!in->Read(&psz, 8)) {
      return false;
    }

    if (psz == 0) {
      uint8_t done = 0;
      out->Write(&done, 1);
      out->Flush();

      goto again;
    }
    auto buffer = (uint8_t *) malloc(psz);
    if (!in->Read(buffer, psz)) {
      logger->critical("Unexpected EOF while reading {} bytes", psz);
      std::abort();
    }

    struct iovec vec = {
      .iov_base = buffer,
      .iov_len = psz,
    };
    shipment->DecodeIOVec(&vec);

    free(buffer);
    return true;
  }
};

class RowEntity;

class RowShipmentReceiver : public ShipmentReceiver<RowEntity> {
 public:
  RowShipmentReceiver(go::TcpSocket *sock) : ShipmentReceiver<RowEntity>(sock) {}
  ~RowShipmentReceiver() { delete sock; }

  void Run() override final;
};

// ObjectSliceScanner is per Slice, it contains one Shipment (which is a queue for entities)
// it will scan Entities from the SliceQueues (via SliceScanner) and add them to the Shipment
// then the SliceManager can grab all the Shipments, and Shipment::RunSend()
template <typename T>
class ObjectSliceScanner : public SliceScanner {
  Shipment<T> *ship;
 public:
  ObjectSliceScanner(Slice * slice, Shipment<T> *shipment)
      : SliceScanner(slice), ship(shipment) {}

  Shipment<T> *shipment() { return ship; }

  void AddObject(T *object) {
    if (ship) {
      ship->AddObject(object);
      StatAddObject();
    }
  }

  // only the slices which (shipment != nullptr) will be scanned
  void Scan() {
    if (!ship) return;
    ShippingHandle *handle = nullptr;
    size_t cnt = 0;
    while ((handle = GetNextHandle())) {
      AddObject(((ObjectShippingHandle<T> *) handle)->object);
      cnt++;
    }
    logger->info("[mig]ObjectSliceScanner found {} objects", cnt);
  }

  // scan the slice for analyzing the max time a row got send
  void ScanShippingHandle() {
    if (!ship) return;
    ResetCursor();
    ShippingHandle *handle = nullptr;
    unsigned long long max = 0;
    size_t cnt = 0;
    while ((handle = GetNextHandle())) {
      auto cur = handle->GetGeneration();
      max = cur > max ? cur : max;
      cnt++;
    }
    logger->info("[mig]In all the {} objects in slice, most shipped row: {} times", cnt, max);
  }
};

using RowSliceScanner = ObjectSliceScanner<RowEntity>;
using RowShipment = felis::Shipment<RowEntity>;

class RowScannerRoutine : public go::Routine {
public:
  void Run() final override;
};

}

#endif
