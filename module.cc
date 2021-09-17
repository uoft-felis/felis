#include <thread>

#include "module.h"
#include "opts.h"

#include "node_config.h"
#include "epoch.h"
#include "routine_sched.h"
#include "console.h"
#include "index.h"
#include "log.h"
#include "piece.h"
#include "slice.h"
#include "txn.h"
#include "gc.h"
#include "gc_dram.h"
#include "vhandle_sync.h"
#include "contention_manager.h"
#include "pwv_graph.h"

#include "util/os.h"

#include "gopp/gopp.h"
#include "gopp/channels.h"
#include "literals.h"

namespace felis {

class LoggingModule : public Module<CoreModule> {
 public:
  LoggingModule() {
    info = {
      .name = "logging",
      .description = "Logging",
    };
    required = true;
  }
  void Init() override {
    auto &console = util::Instance<Console>();
    InitializeLogger(console.server_node_name());

    std::string pid_filename;

    auto username = getenv("USER");
    if (username) {
      pid_filename = fmt::format("/tmp/{}-felis-{}.pid", username, console.server_node_name());
    } else {
      pid_filename = fmt::format("/tmp/felis-{}.pid", username, console.server_node_name());
    }

    FILE *fp = fopen(pid_filename.c_str(), "w");
    fprintf(fp, "%d", getpid());
    fclose(fp);
  }
};

static LoggingModule logging_module;

class AllocatorModule : public Module<CoreModule> {
 public:
  AllocatorModule() {
    info = {
      .name = "allocator",
      .description = "Memory Allocator"
    };
  }
  void Init() override {
    Module<CoreModule>::InitModule("config");

    auto &console = util::Instance<Console>();

    // An extra one region for the ShipmentReceivers
    std::vector<std::thread> tasks;

    mem::InitTotalNumberOfCores(NodeConfiguration::g_nr_threads);
    mem::InitSlab(Options::kMem.ToLargeNumber("4G"));
    //shirley: can't be too big (e.g. 4_G) or else alloc memory fails, use command line options later
    mem::InitTransientPool(128_M);
    mem::InitTransientPmemPool(128_M);
    // Legacy
    //shirley: data region not used in this design anymore?
    mem::GetDataRegion().ApplyFromConf(console.FindConfigSection("mem"));
    //mem::GetDataRegion(true).ApplyFromConf(console.FindConfigSection("mem"));
    // mem::GetPersistentPool().ApplyFromConf(console.FindConfigSection("mem"));

    if (Options::kEpochQueueLength)
      EpochExecutionDispatchService::g_max_item = Options::kEpochQueueLength.ToLargeNumber();

    if (Options::kVHandleLockElision)
      VHandleSyncService::g_lock_elision = true;

    if (Options::kNrEpoch)
      EpochClient::g_max_epoch = Options::kNrEpoch.ToInt();

    if (Options::kEnableGranola) {
      abort_if(!Options::kEnablePartition, "EnablePartition should also be on with Granola");
      abort_if(!Options::kVHandleLockElision, "VHandleLockElision should also be on with Granola");

      abort_if(Options::kEnablePWV, "PWV and Granola cannot be on at the same time");
      EpochClient::g_enable_granola = true;
    }

    if (Options::kEnablePWV) {
      abort_if(!Options::kEnablePartition, "EnablePartition should also be on with PWV");
      abort_if(!Options::kVHandleLockElision, "VHandleLockElision should also be on with PWV");

      abort_if(Options::kEnableGranola, "Granola and PWV cannot be on at the same time");
      EpochClient::g_enable_pwv = true;

      if (Options::kPWVGraphAlloc)
        PWVGraph::g_extra_node_brk_limit = Options::kPWVGraphAlloc.ToLargeNumber();

      util::InstanceInit<PWVGraphManager>::instance = new PWVGraphManager();
    }

    if (Options::kBatchAppendAlloc) {
      ContentionManager::g_prealloc_count = Options::kBatchAppendAlloc.ToLargeNumber();
      abort_if(ContentionManager::g_prealloc_count % 64 != 0, "BatchAppend Memory must align to 64 bytes");
    }

    // Setup GC
    //shirley: set to 1 if want K = every epoch?
    GC::g_gc_every_epoch = 1;// 2 + Options::kMajorGCThreshold.ToLargeNumber("600K") / EpochClient::g_txn_per_epoch;
    GC::g_lazy = Options::kMajorGCLazy;
    GC_Dram::g_gc_every_epoch = 20; // shirley: can't set to 1?
    GC_Dram::g_lazy = false; // shirley todo: can modify this later

    // logger->info("setting up regions {}", i);
    tasks.emplace_back([]() { mem::GetDataRegion().InitPools(); });
    //tasks.emplace_back([]() { mem::GetDataRegion(true).InitPools(true); });
    //shirley test: don't set to true to put everything in dram
    // tasks.emplace_back([]() { mem::GetPersistentPool().InitPools(true); });
    tasks.emplace_back([]() { mem::InitPmemPersistInfo(); });
    tasks.emplace_back([]() { mem::InitTxnInputLog(); });
    tasks.emplace_back([]() { mem::InitExternalPmemPool(); });
    tasks.emplace_back(VHandle::InitPool);
    // shirley todo: add IndexInfo::InitPool
    tasks.emplace_back(IndexInfo::InitPool);
    tasks.emplace_back(RowEntity::InitPool);
    tasks.emplace_back(GC::InitPool);
    tasks.emplace_back(GC_Dram::InitPool);
    tasks.emplace_back([]() { BaseTxn::InitBrk(EpochClient::g_max_epoch - 1); });

    tasks.emplace_back(util::Impl<PromiseAllocationService>);
    tasks.emplace_back(util::Impl<PromiseRoutineDispatchService>);
    tasks.emplace_back(
        []() {
          util::InstanceInit<EpochManager>();
          util::InstanceInit<SliceMappingTable>();
          util::InstanceInit<SpinnerSlot>();
          util::InstanceInit<SimpleSync>();
          if (Options::kVHandleBatchAppend || Options::kOnDemandSplitting)
            util::InstanceInit<ContentionManager>();
        });
    for (auto &t: tasks) t.join();

    mem::PrintMemStats();

    logger->info("Memory allocated: {}MB in total", mem::TotalMemoryAllocated() >> 20);

    // // shirley check: print address of mmap
    // mem::TestSlabMmapAddress();
  }
};

static AllocatorModule allocator_module;

class CoroutineModule : public Module<CoreModule> {
  // TODO: make this NUMA friendly
  class CoroutineStackAllocator : public go::RoutineStackAllocator {
    mem::Pool pools[NodeConfiguration::kMaxNrThreads / mem::kNrCorePerNode];
    static constexpr int kMaxRoutines = 1024;
    static constexpr int kStackSize = 500_K;

    struct Chunk {
      uint8_t stack_space[kStackSize];
      uint64_t alloc_node;
      uint8_t ctx_space[];
    };
   public:
    CoroutineStackAllocator() {
      auto nr_numa_nodes = (NodeConfiguration::g_nr_threads - 1) / mem::kNrCorePerNode + 1;
      for (int node = 0; node < nr_numa_nodes; node++) {
        pools[node] = mem::Pool(
            mem::Coroutine,
            util::Align(sizeof(Chunk) + kContextSize, 8192),
            kMaxRoutines, node);
        pools[node].Register();
      }
    }
    void AllocateStackAndContext(
        size_t &stack_size, ucontext * &ctx_ptr, void * &stack_ptr) override final {
      int tid = go::Scheduler::CurrentThreadPoolId();
      int node = 0;
      if (tid > 0) node = (tid - 1) / mem::kNrCorePerNode;
      stack_size = kStackSize;

      auto ch = (Chunk *) pools[node].Alloc();
      abort_if(ch == nullptr, "pool on numa node {} is empty", node);
      ch->alloc_node = node;
      ctx_ptr = (ucontext *) ch->ctx_space;
      stack_ptr = ch->stack_space;
      memset(ctx_ptr, 0, kContextSize);
    }
    void FreeStackAndContext(ucontext *ctx_ptr, void *stack_ptr) override final {
      auto ch = (Chunk *) stack_ptr;
      pools[ch->alloc_node].Free(ch);
    }
  };

 public:
  CoroutineModule() {
    info = {
      .name = "coroutine",
      .description = "Coroutine Thread Pool",
    };
    required = true;
  }
  ~CoroutineModule() {
  }
  void Init() override {
    // By default, gopp will create kNrThreads + 1 threads. Thread 0 is for
    // background work, which we use for the Peer/Promise Server.
    //
    // In addition to that, we also need one extra shipper thread.
    //
    // In the future, we might need another GC thread?
    Module<CoreModule>::InitModule("config");

    static CoroutineStackAllocator alloc;
    go::InitThreadPool(NodeConfiguration::g_nr_threads + 1, &alloc);

    for (int i = 1; i <= NodeConfiguration::g_nr_threads; i++) {
      // We need to change core affinity by kCoreShifting
      auto r = go::Make(
          [i]() {
            util::Cpu info;
            info.set_affinity(i - 1);
            info.Pin();
          });
      go::GetSchedulerFromPool(i)->WakeUp(r);
    }

    auto r = go::Make(
        []() {
          util::Cpu info;
          for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
            info.set_affinity(i);
          }
          info.Pin();
        });
    go::GetSchedulerFromPool(0)->WakeUp(r);
  }
};

static CoroutineModule coroutine_module;

class NodeServerModule : public Module<CoreModule> {
 public:
  NodeServerModule() {
    info = {
      .name = "node-server",
      .description = "Server for a database node",
    };
  }
  void Init() final override {
    Module<CoreModule>::InitModule("config");
    Module<CoreModule>::InitModule("coroutine");
  }
};

static NodeServerModule server_module;

}
