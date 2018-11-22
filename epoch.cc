#include "epoch.h"
#include "txn.h"
#include "log.h"

namespace felis {

EpochClient *EpochClient::gWorkloadClient = nullptr;

void EpochCallback::operator()()
{
  perf.End();
  perf.Show("Hoola");
}

EpochClient::EpochClient() noexcept
    : callback(EpochCallback()), completion(0, callback)
{
  callback.perf.End();
}

void EpochClient::Start()
{
  auto worker = go::Make(std::bind(&EpochClient::Worker, this));
  go::GetSchedulerFromPool(0)->WakeUp(worker);
}

void EpochClient::Worker()
{
  // TODO: Add epoch management here? At least right now this is essential.
  util::Instance<EpochManager>().DoAdvance(this);

  auto base = 1000; // in 100 times
  printf("load percentage %d\n", LoadPercentage());
  auto total_nr_txn = base * LoadPercentage();
  completion.Increment(total_nr_txn);
  callback.perf.Start();
  BasePromise::g_enable_batching = true;
  for (int i = 0; i < total_nr_txn; i++) {
    if (i == total_nr_txn - 1)
      BasePromise::g_enable_batching = false;
    auto *txn = RunCreateTxn();
    txn->completion.Complete();
  }
}

static constexpr size_t kEpochMemoryLimit = 256 << 20;

EpochMemory::EpochMemory(mem::Pool *pool)
    : pool(pool)
{
  logger->info("Setting up epoch memory pool and brks");
  auto &conf = util::Instance<NodeConfiguration>();
  for (int i = 0; i < conf.nr_nodes(); i++) {
    brks[i].mem = (uint8_t *) pool->Alloc();
    brks[i].off = 0;
  }
}

EpochMemory::~EpochMemory()
{
  auto &conf = util::Instance<NodeConfiguration>();
  for (int i = 0; i < conf.nr_nodes(); i++) {
    pool->Free(brks[i].mem);
  }
}

EpochManager *EpochManager::instance = nullptr;

Epoch *EpochManager::epoch(uint64_t epoch_nr) const
{
  Epoch *e = concurrent_epochs[epoch_nr % kMaxConcurrentEpochs];
  abort_if(e == nullptr, "current epoch is null");
  abort_if(e->epoch_nr != epoch_nr, "epoch number mismatch {} != {}",
           e->epoch_nr, epoch_nr);
  return e;
}

uint8_t *EpochManager::ptr(uint64_t epoch_nr, int node_id, uint64_t offset) const
{
  return epoch(epoch_nr)->brks[node_id - 1].mem + offset;
}

void EpochManager::DoAdvance(EpochClient *client)
{
  cur_epoch_nr++;
  delete concurrent_epochs[cur_epoch_nr % kMaxConcurrentEpochs];
  concurrent_epochs[cur_epoch_nr % kMaxConcurrentEpochs] = new Epoch(cur_epoch_nr, client,pool);
}

EpochManager::EpochManager()
    : pool(new mem::Pool(kEpochMemoryLimit,
                         util::Instance<NodeConfiguration>().nr_nodes() * kMaxConcurrentEpochs)),
      cur_epoch_nr(0)
{
}

}
