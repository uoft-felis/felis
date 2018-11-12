#ifndef SHIPPING_H_
#define SHIPPING_H_

#include <atomic>
#include <mutex>
#include <list>
#include <climits>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/tcp.h>
#include <netinet/ip.h>

#include "util.h"
#include "log.h"

namespace felis {

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
  uint64_t generation;
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

};

class BaseShipment {
 public:
  static constexpr int kSendBatch = 4 * __IOV_MAX;
 protected:
  int fd;
  bool finished;
  std::mutex lock;

  void SendIOVec(struct iovec *vec, int nr_vec);
  void ReceiveACK();

  bool has_finished() const { return finished; }
  std::mutex &mutex() { return lock; }
 public:
  BaseShipment(int fd) : fd(fd), finished(false) {}
  BaseShipment(std::string host, unsigned int port);
};

/**
 * T is a concept:
 * ShippingHandle *shipping_handle();
 * // returns how many iovec encoded, 0 means not enough
 * int EncodeIOVec(struct iovec *vec, int max_nr_vec);
 * void DecodeIOVec(struct iovec *vec);
 *
 * uint64_t encoded_len; // Stored the total data length of EncodeIOVec();
 */
template <typename T>
class Shipment : public BaseShipment {
 protected:
  std::list<T *> queue;
 public:
  using BaseShipment::BaseShipment;

  void AddShipment(T *shipment) {
    std::lock_guard _(lock);
    queue.push_front(shipment);
  }

  bool RunSend() {
    T *obj[kSendBatch];
    int nr_obj = 0;
    struct iovec vec[__IOV_MAX];
    {
      std::lock_guard _(lock);
      while (nr_obj < kSendBatch && !queue.empty()) {
        obj[nr_obj++] = queue.back();
        queue.pop_back();
      }
      if (nr_obj == 0) {
        finished = true;
        close(fd);
        return true;
      }
    }

    int i = 0;
    int cur_iov = 0;
    while (i < nr_obj) {
      int n = 0;
      if (cur_iov == __IOV_MAX
          || (n = obj[i]->EncodeIOVec(&vec[cur_iov + 1], __IOV_MAX - cur_iov - 1)) == 0) {
        SendIOVec(vec, cur_iov);
        cur_iov = 0;
        continue;
      }
      vec[cur_iov].iov_len = 8;
      vec[cur_iov].iov_base = &obj[i]->encoded_len;

      cur_iov += n + 1;
      i++;
    }
    SendIOVec(vec, cur_iov);
    ReceiveACK();
    return false;
  }
};

template <typename T>
class ShipmentReceiver {
  int fd;
 public:
  ShipmentReceiver(int fd) : fd(fd) {
    int enable = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(int));
  }

  bool Receive(T *shipment) {
 again:
    uint64_t psz;
    ssize_t res = recv(fd, &psz, 8, MSG_WAITALL);

    abort_if(res < 0, "recv failed, errno {} {}", errno, strerror(errno));
    if (res == 0) {
      return false;
    }
    abort_if(res < 8, "incomplete read!");

    if (psz == 0) {
      uint8_t done = 0;
      send(fd, &done, 1, 0);

      goto again;
    }
    auto buffer = (uint8_t *) malloc(psz);
    ulong off = 0;
    res = 0;
    while (off < psz) {
      res = recv(fd, buffer + off, psz, MSG_WAITALL);
      abort_if(res < 0, "error on receive, errno {} {}", errno, strerror(errno));
      off += res;
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

}

#endif
