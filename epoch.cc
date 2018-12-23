#include "epoch.h"
#include "txn.h"
#include "log.h"

namespace felis {

EpochClient *EpochClient::g_workload_client = nullptr;

void EpochCallback::operator()()
{
  perf.End();
  perf.Show(label + std::string(" finishes"));

  // Don't call the continuation directly.
  //
  // This is because we might Reset() the BasePromise::g_brk, which would free
  // the current go::Routine.
  go::Scheduler::Current()->WakeUp(go::Make(continuation));
}

EpochClient::EpochClient() noexcept
    : callback(EpochCallback(this)),
      completion(0, callback),
      disable_load_balance(false),
      conf(util::Instance<NodeConfiguration>())
{
  callback.perf.End();
}

void EpochClient::Start()
{
  auto worker = go::Make(std::bind(&EpochClient::InitializeEpoch, this));
  go::GetSchedulerFromPool(0)->WakeUp(worker);
}

uint64_t EpochClient::GenerateSerialId(uint64_t sequence)
{
  return (sequence << 8) | (conf.node_id() & 0x00FF);
}

void EpochClient::RunTxnPromises(std::string label, std::function<void ()> continuation)
{
  callback.label = label;
  callback.continuation = continuation;
  callback.perf.Start();
  auto nr_threads = NodeConfiguration::g_nr_threads;
  for (ulong i = 0; i < nr_threads; i++) {
    auto r = go::Make(
        [i, nr_threads, this] {
          for (ulong j = i * total_nr_txn / nr_threads;
               j < (i + 1) * total_nr_txn / nr_threads;
               j++) {
            txns[j]->root_promise()->Complete(VarStr());
            txns[j]->ResetRoot();
          }
        });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }
}

void EpochClient::InitializeEpoch()
{
  // TODO: Add epoch management here? At least right now this is essential.
  util::Instance<EpochManager>().DoAdvance(this);

  BasePromise::g_brk.Reset();

  conf.FlushBufferPlanCompletion(0);

  auto base = 3000; // in 100 times
  printf("load percentage %d\n", LoadPercentage());
  total_nr_txn = base * LoadPercentage();

  txns.reset(new BaseTxn*[total_nr_txn]);

  disable_load_balance = true;
  for (uint64_t i = 0; i < total_nr_txn; i++) {
    auto sequence = i + 1;
    auto *txn = RunCreateTxn(GenerateSerialId(i + 1));
    conf.CollectBufferPlan(txn->root_promise());
    txns[i] = txn;
  }
  conf.FlushBufferPlan(false);

  RunTxnPromises("Epoch Initialization",
                 std::bind(&EpochClient::ExecuteEpoch, this));
}

void EpochClient::ExecuteEpoch()
{
  BasePromise::g_brk.Reset();

  conf.FlushBufferPlanCompletion(1);

  for (ulong i = 0; i < total_nr_txn; i++) {
    txns[i]->Run();
    txns[i]->root_promise()->AssignSchedulingKey(txns[i]->serial_id());
    conf.CollectBufferPlan(txns[i]->root_promise());
  }
  conf.FlushBufferPlan(true);
  RunTxnPromises("Epoch Execution", []() {});
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
