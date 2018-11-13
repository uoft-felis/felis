#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "gopp/gopp.h"
#include "shipping.h"
#include "log.h"

namespace felis {

static constexpr ulong kScanningSessionStatusBits = 1;
static constexpr ulong kScanningSessionMask = (1 << kScanningSessionStatusBits) - 1;
static constexpr ulong kScanningSessionActive = 0x00;

static std::atomic_ulong g_scanning_session = 1;

ShippingHandle::ShippingHandle()
    : born(g_scanning_session.load() >> kScanningSessionStatusBits),
      generation(0), sent_generation(0)
{
  Initialize();
  // TODO: adding to the slice queue based on the current go-routine ID.
}

bool ShippingHandle::MarkDirty()
{
  if (generation == sent_generation.load(std::memory_order_acquire)) {
    generation++;

    auto session = g_scanning_session.load();
    if ((session & kScanningSessionMask) == kScanningSessionActive) {
      // if born < session, then this handle will be scanned from the slice.
      return born >= (session >> kScanningSessionStatusBits);
    } else {
      return true;
    }
  }
  return false;
}

void ShippingHandle::PrepareSend()
{
  sent_generation.fetch_add(1, std::memory_order_seq_cst);
}

void Slice::SliceQueue::Append(ShippingHandle *handle)
{
  std::unique_lock l(lock, std::defer_lock);
  if (need_lock)
    l.lock();
  handle->InsertAfter(queue.prev);
}

Slice::Slice()
{
  shared_q->need_lock = true;
}

void Slice::Append(ShippingHandle *handle)
{
  // Detect current execution environment
  auto sched = go::Scheduler::Current();
  SliceQueue *q = &shared_q.elem;
  if (sched && !sched->current_routine()->is_share()) {
    int coreid = sched->CurrentThreadPoolId() - 1;
    if (coreid > 0)
      q = &per_core_q[coreid].elem;
  }

  q->Append(handle);
}

ShippingHandle *SliceScanner::GetNextHandle()
{
  TBD();
}

BaseShipment::BaseShipment(std::string host, unsigned int port, bool defer_connect)
{
  fd = socket(AF_INET, SOCK_STREAM, 0);

  memset(&addr, 0, sizeof(sockaddr_in));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(host.c_str());
  if (!defer_connect)
    Connect();
}

void BaseShipment::Connect()
{
  abort_if(connect(fd, (sockaddr *)&addr, sizeof(sockaddr_in)) < 0,
           "Cannot connect errno {} {}", errno,
           strerror(errno));
  connected = true;
}

void BaseShipment::SendIOVec(struct iovec *vec, int nr_vec)
{
  if (nr_vec > 0 && !connected)
    Connect();

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
  int disabled = 0;

  setsockopt(fd, IPPROTO_TCP, TCP_CORK, &disabled, 4);

  res = send(fd, &done, 8, 0);
  if (res != 8) goto error;

  res = recv(fd, &done, 1, MSG_WAITALL);
error:
  abort_if(res == 0, "EOF from the receiver side?");
  abort_if(res < 0, "Error receiving ack from the reciver side... errno={}", errno);
}

}
