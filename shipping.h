#ifndef SHIPPING_H_
#define SHIPPING_H_

#include <atomic>
#include <mutex>
#include <list>
#include <climits>
#include <sys/uio.h>
#include "util.h"

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

  BaseShipment(int fd) : fd(fd), finished(false) {}
  void SendIOVec(struct iovec *vec, int nr_vec);
  void ReceiveACK();

  bool has_finished() const { return finished; }
  std::mutex &mutex() { return lock; }
};

/**
 * T is a concept:
 * ShippingHandle *shipping_handle() const;
 * void EncodeIOVec(struct iovec *vec);
 */
template <typename T>
class Shipment : public BaseShipment {
 protected:
  std::list<T *> queue;
 public:
  Shipment(int fd) : BaseShipment(fd) {}

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

}

#endif
