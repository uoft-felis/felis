#include <iostream>
#include <fstream>
#include <sys/mman.h>

#include "module.h"

#include "node_config.h"
#include "epoch.h"
#include "console.h"
#include "index.h"
#include "log.h"
#include "promise.h"
#include "slice.h"
#include "txn.h"
#include "gc.h"
#include "vhandle_sync.h"

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

    std::stringstream ss;
    ss << "/tmp/";

    auto username = getenv("USER");
    if (username)
      ss << username << "-";
    ss << "felis-" << console.server_node_name() << ".pid";

    std::ofstream pid_fout(ss.str());
    pid_fout << (unsigned long) getpid();
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

    mem::InitTotalNumberOfCores(NodeConfiguration::g_nr_threads,
                                NodeConfiguration::g_core_shifting);
    mem::InitSlab(8_G);
    mem::GetDataRegion().ApplyFromConf(console.FindConfigSection("mem"));
    // logger->info("setting up regions {}", i);
    tasks.emplace_back([]() { mem::GetDataRegion().InitPools(); });
    tasks.emplace_back(VHandle::InitPool);
    tasks.emplace_back(RowEntity::InitPool);
    tasks.emplace_back(GC::InitPool);
    tasks.emplace_back([]() { BaseTxn::InitBrk(EpochClient::kMaxEpoch - 1); });

    tasks.emplace_back(util::Impl<PromiseAllocationService>);
    tasks.emplace_back(util::Impl<PromiseRoutineDispatchService>);
    tasks.emplace_back(
        []() {
          util::InstanceInit<EpochManager>();
          util::InstanceInit<SliceMappingTable>();
          util::InstanceInit<SpinnerSlot>();
        });
    for (auto &t: tasks) t.join();

    mem::PrintMemStats();

    logger->info("Memory allocated: {}MB in total", mem::TotalMemoryAllocated() >> 20);
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
      auto nr_numa_nodes = (NodeConfiguration::g_core_shifting
                            + NodeConfiguration::g_nr_threads) / mem::kNrCorePerNode;
      for (int node = NodeConfiguration::g_core_shifting / mem::kNrCorePerNode;
           node < nr_numa_nodes; node++) {
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
    go::WaitThreadPool();
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
            util::PinToCPU(i - 1 + NodeConfiguration::g_core_shifting);
          });
      go::GetSchedulerFromPool(i)->WakeUp(r);
    }

    auto r = go::Make(
        []() {
          std::vector<int> cpus;
          for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
            cpus.push_back(i + NodeConfiguration::g_core_shifting);
          }
          util::PinToCPU(cpus);
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

    util::Instance<NodeConfiguration>().RunAllServers();
  }
};

static NodeServerModule server_module;

}
