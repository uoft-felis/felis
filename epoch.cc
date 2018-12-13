#include "epoch.h"
#include "txn.h"
#include "log.h"

namespace felis {

EpochClient *EpochClient::g_workload_client = nullptr;

void EpochCallback::operator()()
{
  perf.End();
  perf.Show("Hoola");
}

EpochClient::EpochClient() noexcept
    : callback(EpochCallback()), completion(0, callback), disable_load_balance(false)
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
  auto &conf = util::Instance<NodeConfiguration>();

  auto base = 3000; // in 100 times
  printf("load percentage %d\n", LoadPercentage());
  auto total_nr_txn = base * LoadPercentage();
  auto watermark = base * 70;
  auto nr_threads = NodeConfiguration::g_nr_threads;
  BasePromise **roots = new BasePromise*[total_nr_txn];

  // disable_load_balance = true;
  for (int i = 0; i < total_nr_txn; i++) {
    if (i == watermark)
      disable_load_balance = true;
    auto *txn = RunCreateTxn(i + 1);
    conf.CollectBufferPlan(txn->root_promise());
    roots[i] = txn->root_promise();
  }

  conf.FlushBufferPlan();
  callback.perf.Start();
  for (ulong i = 0; i < nr_threads; i++) {
    auto r = go::Make(
        [i, watermark, total_nr_txn, nr_threads, roots] {
          logger->info("Beginning to dispatch {}", i + 1);
          for (ulong j = i * watermark / nr_threads;
               j < (i + 1) * watermark / nr_threads;
               j++) {
            roots[j]->Complete(VarStr());
            roots[j] = nullptr;
          }
          ulong left_over = total_nr_txn - watermark;
          for (ulong j = i * left_over / nr_threads;
               j < (i + 1) * left_over / nr_threads;
               j++) {
            roots[j + watermark]->Complete(VarStr());
            roots[j + watermark] = nullptr;
          }
          logger->info("Dispatch done {}", i + 1);
        });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }

  disable_load_balance = false;
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
