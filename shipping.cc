#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <netinet/in.h>

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

BaseShipment::BaseShipment(std::string host, unsigned int port)
{
  fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr;
  memset(&addr, 0, sizeof(sockaddr_in));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(host.c_str());
  abort_if(connect(fd, (sockaddr *) &addr, sizeof(sockaddr_in)) < 0,
           "Cannot connect {}:{}, errno {} {}", host, port, errno, strerror(errno));
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
  uint64_t done = 0;
  ssize_t res = 0;

  res = send(fd, &done, 8, 0);
  if (res != 8) goto error;

  res = recv(fd, &done, 1, MSG_WAITALL);
error:
  abort_if(res == 0, "EOF from the receiver side?");
  abort_if(res < 0, "Error receiving ack from the reciver side... errno={}", errno);
}

}
