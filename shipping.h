#ifndef SHIPPING_H_
#define SHIPPING_H_

#include <atomic>
#include <mutex>
#include <list>
#include <climits>
#include <sys/socket.h>
#include <sys/uio.h>
#include "util.h"
#include "log.h"

namespace felis {

class ShippingHandle {
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
 * void EncodeIOVec(struct iovec *vec);
 * void DecodeIOVec(struct iovec *vec);
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

    int step = __IOV_MAX / 2;
    for (int j = 0; j < nr_obj; j += step) {
      int nr = std::min(nr_obj - j, step);

      for (int i = 0; i < nr; i++) {
        obj[j + i]->shipping_handle()->PrepareSend();
        obj[j + i]->EncodeIOVec(&vec[2 * i + 1]);
        vec[2 * i].iov_len = 8;
        vec[2 * i].iov_base = &(vec[2 * i + 1].iov_len);
      }
      SendIOVec(vec, 2 * nr);
    }

    ReceiveACK();
    return false;
  }
};

template <typename T>
class ShipmentReceiver {
  int fd;
 public:
  ShipmentReceiver(int fd) : fd(fd) {}

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
