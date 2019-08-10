#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "gopp/gopp.h"
#include "shipping.h"
#include "slice.h"
#include "masstree_index_impl.h"
#include "epoch.h"

namespace felis {

static constexpr uint64_t kScanningSessionStatusBits = 2;
static constexpr uint64_t kScanningSessionMask = (1 << kScanningSessionStatusBits) - 1;

static constexpr uint64_t kScanningSessionConverged = 0x03;
static constexpr uint64_t kScanningSessionActive = 0x00;
static constexpr uint64_t kScanningSessionInactive = 0x01;
static constexpr uint64_t kScanningSessionConverging = 0x02;

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
 *
 * [ ts + 1 ] 10: the shipping data of the live-migration is about to converge,
 * meaning the epoch client should wait for this migration session to end, before
 * ending current epoch.
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

void SliceScanner::MigrationApproachingEnd()
{
  auto session = g_scanning_session.fetch_add(1);
  abort_if((session & kScanningSessionMask) != kScanningSessionInactive,
           "Cannot set converging because scanner isn't inactive {}",
           session);
}

void SliceScanner::MigrationEnd()
{
  auto session = g_scanning_session.fetch_add(1);
  auto status = session & kScanningSessionMask;
  abort_if((status != kScanningSessionConverging &&
            status != kScanningSessionInactive),
           "Cannot end migration because scanner isn't converging or isn't active {}",
           session);
  if (status == kScanningSessionInactive)
    // in index, will go from Inactive straight to Converged, so add 2
    g_scanning_session.fetch_add(1);
  g_objects_added.store(0);
}

bool SliceScanner::IsConverging() {
  auto session = g_scanning_session.load();
  return ((session & kScanningSessionMask) == kScanningSessionConverging);
}

SliceScanner::SliceScanner(Slice * slice) : slice(slice)
{
  current_q = &slice->shared_q.elem;
  current_node = current_q->queue.next;
}

// reset the cursor used in GetNextHandle, so you can scan the slice again
void SliceScanner::ResetCursor() {
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

void RowShipmentReceiver::Run()
{
// clear the affinity
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  logger->info("[mig]New Row Shipment has arrived!");
  PerfLog perf;
  int count = 0;


//#define DISCARD_RECEIVE
#ifdef DISCARD_RECEIVE
  RowEntity ent;
  ent.Prepare(VarStr::FromAlloca(alloca(64), 64), VarStr::FromAlloca(alloca(768), 768));
  int maxK = 0, maxV = 0;
  while (Receive(&ent)) {
    count++;
    maxK = ent.k->len > maxK ? ent.k->len : maxK;
    maxV = ent.v->len > maxV ? ent.v->len : maxV;
  }
  logger->info("Max Key length {}, Max Value length {}", maxK, maxV);
#endif


#define BUFFER_RECEIVE
#ifdef BUFFER_RECEIVE
  int worker_tid = 0;
  static constexpr int recvBufSize = 512;
  static constexpr int batchSize = 32; // entity batch num per lambda
  RowEntity ent[recvBufSize];
  for (int i = 0; i < recvBufSize; i++) {
    ent[i].Prepare(VarStr::FromAlloca(alloca(64), 64), VarStr::FromAlloca(alloca(768), 768));
  }

  while (true) {
    int recvCount = recvBufSize;
    bool lastBatch = false;
    for (int i = 0; i < recvBufSize; i++) {
      if (!Receive(&ent[i])) {
        recvCount = i;
        lastBatch = true;
        break;
      }
      count++;
    }

    if (recvCount == 0)
      break;

    int workerNeeded = (recvCount - 1) / batchSize + 1;
    go::BufferChannel *complete = new go::BufferChannel(workerNeeded);
    for (int i = 0; i < recvCount; i += batchSize) {
      RowEntity *en = ent + i;
      int entCount = (i + batchSize > recvCount) ? (recvCount - i) : batchSize;
      auto r = go::Make(
          [en, entCount, complete] {
            auto &mgr = util::Instance<RelationManager>();
            for (int i = 0; i < entCount; i++) {
              auto rel_id = en[i].get_rel_id();
              auto slice_id = en[i].slice_id();

              VarStr *k = VarStr::New(en[i].k->len), *v = VarStr::New(en[i].v->len);
              memcpy((uint8_t *)k->data, en[i].k->data, en[i].k->len);
              memcpy((uint8_t *)v->data, en[i].v->data, en[i].v->len);

              // InsertOrDefault:
              //   If the key exists in the masstree, then return the value
              //   If the key does not exist, then execute the lambda function, insert the
              //     (key, return value of lambda) into the masstree, and return the value
              auto &rel = mgr[rel_id];
              bool exist = true;
              auto handle = rel.SearchOrDefault(k, [&exist]() { exist = false; return new VHandle(); });
              if (exist) {
                auto epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
                auto sid = handle->last_version() + 1;
                handle->AppendNewVersion(sid, epoch_nr);
                handle->WriteWithVersion(sid, v, epoch_nr);
              } else {
                InitVersion(handle, v);
              }

              RowEntity *entity = new felis::RowEntity(rel_id, k, handle, slice_id);

              // TODO: add row to its slice

            }
            uint8_t done = 0;
            complete->Write(&done, 1);
          });
      r->set_urgent(true);
      go::GetSchedulerFromPool(worker_tid + 1)->WakeUp(r);
      worker_tid = (worker_tid + 1) % NodeConfiguration::g_nr_threads;
    }
    //logger->info("[Receive]dispatched {} RowEntities, total {}", recvCount, count);

    uint8_t buf[recvBufSize / batchSize];
    complete->Read(buf, workerNeeded);
    //logger->info("[Receive]worker finished {} RowEntities, total {}", recvCount, count);

    if (lastBatch)
      break;
  }
#endif

  perf.End();
  perf.Show("[mig]RowShipment processing takes");
  logger->info("[mig]processed {} RowEntities, speed {} row/s", count, count * 1000 / perf.duration_ms());
  sock->Close();

}

void RowScannerRoutine::Run()
{
  PerfLog perf_scan;
  logger->info("[mig]Scanning row...");
  auto &slicer = util::Instance<SliceManager>();
  slicer.ScanAllRow();
  perf_scan.End();
  perf_scan.Show("[mig]Scanning row done, takes");

  auto all_shipments = slicer.all_row_shipments();
  if (all_shipments.size() == 0) {
    logger->info("[mig]No active shipment. Ending scanning session {}", g_scanning_session.load());
    SliceScanner::MigrationApproachingEnd();
    SliceScanner::MigrationEnd();
    return;
  }

  PerfLog perf_ship;
  bool approachingEndSet = false;
  logger->info("[mig]Shipping row...");
  for (auto shipment: all_shipments) {
    int iter = 0;
    while (!shipment->RunSend()) {
      iter++;
      if (g_objects_shipped + g_objects_skipped > 650000 && !approachingEndSet) {
        // TODO: determine converging or not by metrics, rather than magic number
        SliceScanner::MigrationApproachingEnd();
        approachingEndSet = true;
      }

    // logger->info("[mig]iter {}: shipped {}/skipped {}/total {}, sent {} KB",
    //    iter, g_objects_shipped, g_objects_skipped, g_objects_shipped + g_objects_skipped, g_bytes_sent / 1024);

    }
    logger->info("[mig]Shipping row done, shipped {}/skipped {}/total {}, iter {}, sent {} KB",
      g_objects_shipped, g_objects_skipped, g_objects_shipped + g_objects_skipped, iter, g_bytes_sent / 1024);
  }
  perf_ship.End();
  perf_ship.Show("[mig]Shipping row takes");

  slicer.ScanShippingHandle();
}

}
