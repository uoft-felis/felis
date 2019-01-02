#include <sys/mman.h>

#include "epoch.h"
#include "txn.h"
#include "log.h"
#include "vhandle.h"
#include "iface.h"
namespace felis {

EpochClient *EpochClient::g_workload_client = nullptr;

void EpochCallback::operator()()
{
  perf.End();
  perf.Show(label + std::string(" finishes"));

  // Don't call the continuation directly.
  //
  // This is because we might Reset() the PromiseAllocationService, which would free
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
          conf.IncrementExtraIOPending(i);
          for (ulong j = i * total_nr_txn / nr_threads;
               j < (i + 1) * total_nr_txn / nr_threads;
               j++) {
            txns[j]->root_promise()->Complete(VarStr());
            txns[j]->ResetRoot();
          }
          conf.DecrementExtraIOPending(i);
          logger->info("core {} finished issuing promises", i);
        });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }
}

void EpochClient::InitializeEpoch()
{
  // TODO: Add epoch management here? At least right now this is essential.
  util::Instance<EpochManager>().DoAdvance(this);

  util::Impl<PromiseAllocationService>().Reset();

  conf.ResetBufferPlan();
  conf.FlushBufferPlanCompletion(0);

  printf("load percentage %d\n", LoadPercentage());
  total_nr_txn = kEpochBase * LoadPercentage();

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
  // util::Impl<PromiseAllocationService>().Reset();

  conf.ResetBufferPlan();
  conf.FlushBufferPlanCompletion(1);

  for (ulong i = 0; i < total_nr_txn; i++) {
    txns[i]->Run();
    txns[i]->root_promise()->AssignSchedulingKey(txns[i]->serial_id());
    conf.CollectBufferPlan(txns[i]->root_promise());
  }
  conf.FlushBufferPlan(true);
  RunTxnPromises("Epoch Execution", []() {});
}

EpochExecutionDispatchService::EpochExecutionDispatchService()
{
}

void EpochExecutionDispatchService::Reset()
{
  for (size_t i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    mappings[i].clear();
  }
}

void EpochExecutionDispatchService::Dispatch(int core_id,
                                             BasePromise::ExecutionRoutine *exe,
                                             go::Scheduler::Queue *q)
{
  auto key = exe->schedule_key();
  auto &mapping = mappings[core_id];
  auto it = mapping.upper_bound(key);
  if (it == mapping.begin()) {
    // Add to the head. However, we need to skip all urgent routines!!!
    auto head = q;
    while (q->next != head && ((go::Routine *) q->next)->is_urgent())
      q = q->next;

    exe->Add(q);
    mapping.insert(std::make_pair(key, Entity{1, exe}));
    auto &sync = util::Impl<VHandleSyncService>();
    sync.Notify(1 << core_id);
  } else {
    --it;
    exe->Add(it->second.last);
    if (it->first != key) {
      mapping.insert(it, std::make_pair(key, Entity{1, exe}));
    } else {
      auto &entity = it->second;
      entity.dupcnt++;
      entity.last = exe;
    }
  }
}

void EpochExecutionDispatchService::Detach(int core_id,
                                           BasePromise::ExecutionRoutine *exe)
{
  auto key = exe->schedule_key();
  auto &mapping = mappings[core_id];

  auto it = mapping.find(key);

#if 0
  if (!mapping.empty() && mapping.begin()->first != key) {
    printf("Why not %lu?\n", mapping.begin()->first);
    std::abort();
  }

  abort_if(it == mapping.end(), "Cannot find {} in the scheduler! on core {}", key, core_id);
#endif
  if (--it->second.dupcnt == 0) {
    // printf("Erasing %lu from core %d\n", key, core_id);
    mapping.erase(it);
  }
}

void EpochExecutionDispatchService::PrintInfo()
{
  puts("===================================");
  for (int core_id = 0; core_id < NodeConfiguration::g_nr_threads; core_id++) {
    int i = 0;
    for (auto it = mappings[core_id].begin();
         it != mappings[core_id].end() && i < 3; ++it, ++i) {
      printf("DEBUG: %lu on this core (core %d)\n", it->first, core_id);
    }
  }
  puts("===================================");
}

static constexpr size_t kEpochPromiseAllocationPerThreadLimit = 2ULL << 30;
static constexpr size_t kEpochPromiseAllocationMainLimit = 4UL << 30;

EpochPromiseAllocationService::EpochPromiseAllocationService()
{
  brks = new mem::Brk[NodeConfiguration::g_nr_threads + 1];
  size_t acc = 0;
  for (size_t i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    auto s = kEpochPromiseAllocationPerThreadLimit;
    if (i == 0) s = kEpochPromiseAllocationMainLimit;
    brks[i].move(mem::Brk(
        mmap(NULL, s,
             PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE,
             -1, 0),
        s));
    acc += s;
  }
  logger->info("Memory used: PromiseAllocator {}GB", acc >> 30);
}

void *EpochPromiseAllocationService::Alloc(size_t size)
{
  int thread_id = go::Scheduler::CurrentThreadPoolId();
  return brks[thread_id].Alloc(size);
}

void EpochPromiseAllocationService::Reset()
{
  for (size_t i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    brks[i].Reset();
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
