#include "shipping.h"
#include "log.h"

namespace felis {

ShippingHandle::ShippingHandle()
    : generation(1), sent_generation(0)
{}

bool ShippingHandle::MarkDirty()
{
  if (generation == sent_generation.load(std::memory_order_acquire)) {
    generation++;
    return true;
  }
  return false;
}

void ShippingHandle::PrepareSend()
{
  sent_generation.fetch_add(1, std::memory_order_seq_cst);
}

void BaseShipment::SendIOVec(struct iovec *vec, int nr_vec)
{
  while (nr_vec > 0) {
    ssize_t res = writev(fd, vec, nr_vec);
    abort_if(res < 0, "writev() failed {}", errno);

    while (res > 0) {
      if (vec[0].iov_len <= res) {
        res -= vec[0].iov_len;
        vec++;
        nr_vec--;
      } else {
        vec[0].iov_len -= res;
        vec[0].iov_base = (uint8_t *) vec[0].iov_base + res;
        res = 0;
      }
    }
  }
}

void BaseShipment::ReceiveACK()
{
  uint8_t done = 0;
  ssize_t res = 0;
  for (int i = 0; i < 8; i++) {
    res = write(fd, &done, 1);
    if (res != 1) goto error;
  }
  res = read(fd, &done, 1);
error:
  abort_if(res == 0, "EOF from the receiver side?");
  abort_if(res < 0, "Error receiving ack from the reciver side... errno={}", errno);
}

}
