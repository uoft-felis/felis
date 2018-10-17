#include "epoch.h"
#include "txn.h"
#include "log.h"

namespace felis {

EpochClient *EpochClient::gWorkloadClient = nullptr;

EpochClient::EpochClient() noexcept
{
}

void EpochClient::Start()
{
  auto worker = go::Make(std::bind(&EpochClient::Worker, this));
  go::GetSchedulerFromPool(1)->WakeUp(worker);
}

void EpochClient::Worker()
{
  // TODO: Add epoch management here? At least right now this is essential.
  util::Instance<EpochManager>().DoAdvance();

  for (int i = 0; i < 1000; i++) {
    fprintf(stderr, "Creating txn %d\n", i);
    auto *txn = RunCreateTxn();
    // TODO: wait
    delete txn;
  }
}

static constexpr size_t kEpochMemoryLimit = 12 << 20;

EpochMemory::EpochMemory(mem::Pool *pool)
    : pool(pool)
{
  auto &conf = util::Instance<NodeConfiguration>();
  for (int i = 0; i < conf.nr_nodes(); i++) {
    brks[i].mem = (uint8_t *) pool->Alloc();
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

void EpochManager::DoAdvance()
{
  cur_epoch_nr++;
  delete concurrent_epochs[cur_epoch_nr % kMaxConcurrentEpochs];
  concurrent_epochs[cur_epoch_nr % kMaxConcurrentEpochs] = new Epoch(cur_epoch_nr, pool);
}

EpochManager::EpochManager()
    : pool(new mem::Pool(kEpochMemoryLimit,
                         util::Instance<NodeConfiguration>().nr_nodes() * kMaxConcurrentEpochs)),
      cur_epoch_nr(0)
{
}

}
