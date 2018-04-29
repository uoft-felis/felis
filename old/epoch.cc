#include <cassert>
#include <fstream>
#include <future>
#include <iomanip>
#include <sstream>
#include <dlfcn.h>
#include "log.h"

#include "epoch.h"
// #include "net-io.h"
#include "index.h"
#include "util.h"

#include "json11/json11.hpp"

#include "gopp/gopp.h"
#include "gopp/channels.h"

#include "csum.h"

using util::Instance;

namespace dolly {

uint64_t Epoch::kGlobSID = 0ULL;

static uint64_t gGlobalEpoch = 0;
const size_t Epoch::kBrkSize = 32 << 20;

const int Epoch::kNrThreads;

void Epoch::InitPools()
{
  const int tot_nodes = kNrThreads / mem::kNrCorePerNode;
  logger->info("setting up memory pools and regions. {} NUMA nodes in total", tot_nodes);

  pools = (mem::Pool *) malloc(tot_nodes * sizeof(mem::Pool));

  for (int nid = 0; nid < tot_nodes; nid++) {
    new (&pools[nid]) mem::Pool(kBrkSize, 2 * mem::kNrCorePerNode, nid);
  }
}

Epoch::Epoch(std::vector<go::TcpSocket *> socks)
{
  InitBrks();

  PerfLog p;
  wait_channel = new go::BufferChannel(kNrThreads);
  wait_barrier = new go::WaitBarrier(kNrThreads);

  for (int i = 0; i < kNrThreads; i++) {
    auto sock = socks[i];
    reuse_q[i].Init();

    readers[i] = new TxnIOReader(socks[i], wait_barrier, this, &reuse_q[i]);
    go::GetSchedulerFromPool(i + 1)->WakeUp(readers[i]);
  }

  {
    bool eof = false;
    bool need_throw = false;
    uint8_t res[kNrThreads];

    wait_channel->Read(res, kNrThreads);
    for (int i = 0; i < kNrThreads; i++) {
      if (res[i] != 0) throw ParseBufferEOF();
    }
  }
  p.Show("IO takes");
}

Epoch::~Epoch()
{
  delete wait_barrier;
  for (int i = 0; i < kNrThreads; i++) {
    delete readers[i];
  }
  DestroyBrks();
}

int Epoch::IssueReExec()
{
  ++gGlobalEpoch;
  int nr_wait = 0;
  uint64_t tot_txns = 0;
  for (int i = 0; i < kNrThreads; i++) {
    if (reuse_q[i].is_empty()) continue;
    nr_wait++;
  }

  wait_barrier->Adjust(nr_wait);

  for (int i = 0; i < kNrThreads; i++) {
    auto runner = new TxnRunner(&reuse_q[i]);
    // runner->StartOn(i + 1);
    go::GetSchedulerFromPool(i + 1)->WakeUp(runner);
    tot_txns += readers[i]->finish_counter()->max;
  }
  logger->info("epoch contains {}", tot_txns);
  return nr_wait;
}

void Epoch::WaitForReExec(int tot)
{
  uint8_t res[tot];
  wait_channel->Read(res, tot);
}

mem::Pool *Epoch::pools;

void Epoch::InitBrks()
{
  for (int i = 0; i < kNrThreads; i++) {
    auto p = pools[i / mem::kNrCorePerNode].Alloc();
    brks[i] = mem::Brk(p, kBrkSize);
  }
}

void Epoch::DestroyBrks()
{
  for (int i = 0; i < kNrThreads; i++) {
    pools[i / mem::kNrCorePerNode].Free(brks[i].ptr());
  }
}

uint64_t Epoch::CurrentEpochNumber()
{
  return gGlobalEpoch;
}

BaseRequest *BaseRequest::CreateRequestFromChannel(go::TcpInputChannel *channel, Epoch *epoch)
{
  uint8_t type = 0;
  if (!channel->Read(&type, 1)) {
    logger->critical("channel closed earlier");
    std::abort();
  }
  logger->debug("txn req type {0:d}", type);
  if (type == 0 || type > GetGlobalFactoryMap().rbegin()->first) {
    logger->critical("Unknown txn type {} on thread {}", type,
                     go::Scheduler::CurrentThreadPoolId());
    return nullptr;
  }
  auto req = GetGlobalFactoryMap().at(type)(epoch);
  req->type = type;
  req->ParseFromChannel(channel);

#ifdef CALVIN_REPLAY
  // read-set keys. Just for Calvin
  uint32_t read_key_pkt_size;
  channel->Read(&read_key_pkt_size, sizeof(uint32_t));
  logger->debug("calvin keys, total len {}", read_key_pkt_size);
  req->InitializeReadSet(channel, read_key_pkt_size, epoch);
#endif
  // write-set keys
  uint16_t key_pkt_size;
  channel->Read(&key_pkt_size, sizeof(uint16_t));
  logger->debug("output keys, total len {}", key_pkt_size);
  req->Initialize(channel, key_pkt_size, epoch);

  return req;
}

BaseRequest::FactoryMap BaseRequest::factory_map;

void BaseRequest::MergeFactoryMap(const FactoryMap &extra)
{
  auto &factory_map = GetGlobalFactoryMap();
  for (auto it = extra.begin(); it != extra.end(); ++it) {
    if (factory_map.find(it->first) != factory_map.end()) {
      logger->error("Cannot register transaction type {} twice!", it->first);
      std::abort();
    }
    logger->info("registered tx with type {}", (int) it->first);
    factory_map.insert(*it);
  }
}

}
