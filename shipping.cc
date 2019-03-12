#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "gopp/gopp.h"
#include "shipping.h"
#include "log.h"

namespace felis {

static constexpr uint64_t kScanningSessionStatusBits = 2;
static constexpr uint64_t kScanningSessionMask = (1 << kScanningSessionStatusBits) - 1;

static constexpr uint64_t kScanningSessionConverged = 0x03;
static constexpr uint64_t kScanningSessionActive = 0x00;
static constexpr uint64_t kScanningSessionInactive = 0x01;

/**
 * Scanning session timestamp uses laste two bits for shipping/scanning status.
 *
 * [   ts   ] 11: currently there is nothing to ship. Either we have not started
 * the shipping process, or we have already migrated to the new sharding plan,
 * which means the live-migration has converged.
 *
 * [ ts + 1 ] 00: a new live-migration session has just started, so a scanner is
 * currently active. The scanner might take some of the newly inserted rows, but
 * most importantly, it will take ALL the old rows.
 *
 * [ ts + 1 ] 01: the scanner has finished, but the live-migration is still
 * going on now.
 */
static std::atomic_ulong g_scanning_session = kScanningSessionConverged;

ShippingHandle::ShippingHandle()
    : born(g_scanning_session.load() >> kScanningSessionStatusBits),
      generation(0), sent_generation(0)
{
  Initialize();
}

bool ShippingHandle::MarkDirty()
{
  if (generation.load(std::memory_order_acquire) == sent_generation.load(std::memory_order_acquire)) {
    generation.fetch_add(1, std::memory_order_seq_cst);

    auto session = g_scanning_session.load();

    if ((session & kScanningSessionMask) == kScanningSessionActive) {
      // if born < session, then this handle will be scanned from the slice.
      printf("born %lu, session %lu\n", born, session >> kScanningSessionStatusBits);
      return born >= (session >> kScanningSessionStatusBits);
    } else if ((session & kScanningSessionMask) == kScanningSessionConverged) {
      // It's already converged, you don't need to send this at all. If it is
      // necessary, it will be picked up by the next scanner.
      return false;
    }  else {
      return true;
    }
  }
  return false;
}

void ShippingHandle::PrepareSend()
{
  sent_generation.fetch_add(1, std::memory_order_seq_cst);
}

bool ShippingHandle::CheckSession()
{
  auto session = g_scanning_session.load();
  // if born < session, then this handle should be added by the scanner.
  return born < (session >> kScanningSessionStatusBits);
}

static std::atomic_ulong g_objects_added = 0;

void SliceScanner::ScannerBegin()
{
  auto session = g_scanning_session.fetch_add(1);
  abort_if((session & kScanningSessionMask) != kScanningSessionConverged,
           "Cannot begin scanner because last session ({}) has not converged",
           session);
}

void SliceScanner::ScannerEnd()
{
  auto session = g_scanning_session.fetch_add(1);
  abort_if((session & kScanningSessionMask) != kScanningSessionActive,
           "Cannot end scanner because last session ({}) is not active",
           session);
}

void SliceScanner::StatAddObject()
{
  g_objects_added.fetch_add(1);
}

void SliceScanner::MigrationEnd()
{
  logger->info("Migration finishes {} objects were added", g_objects_added.load());
  auto session = g_scanning_session.fetch_add(2);
  abort_if((session & kScanningSessionMask) != kScanningSessionInactive,
           "Cannot end migration because scanner isn't inactive {}",
           session);
  g_objects_added.store(0);
}

SliceScanner::SliceScanner(Slice * slice) : slice(slice)
{
  current_q = &slice->shared_q.elem;
  current_node = current_q->queue.next;
}

ShippingHandle *SliceScanner::GetNextHandle()
{
  while (current_q != nullptr) {
    if (current_node != &current_q->queue) {
      auto h = (ShippingHandle *) current_node;
      if (h->CheckSession()) {
        current_node = current_node->next;
        return h;
      }
    }

    // scan of current queue is over, switch to the next queue
    // queue order: shared_q, per_core_q[]
    if (current_q == &slice->shared_q.elem) {
      current_q = &slice->per_core_q[0].elem;
    } else if (current_q == &slice->per_core_q[NodeConfiguration::g_nr_threads - 1].elem) {
      current_q = nullptr;
    } else {
      for (int i = 0; i < NodeConfiguration::g_nr_threads - 1; i++) {
        if (current_q == &slice->per_core_q[i].elem) {
          current_q = &slice->per_core_q[i + 1].elem;
          break;
        }
      }
    }
    current_node = current_q ? current_q->queue.next : nullptr;
  }
  return nullptr;
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
  else
    connected = false;
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
