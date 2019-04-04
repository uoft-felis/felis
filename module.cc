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

#include "gopp/gopp.h"
#include "gopp/channels.h"

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

    mem::ParallelPool::InitTotalNumberOfCores(NodeConfiguration::g_nr_threads,
                                              NodeConfiguration::g_core_shifting);
    mem::GetDataRegion().ApplyFromConf(console.FindConfigSection("mem"));
    // logger->info("setting up regions {}", i);
    tasks.emplace_back([]() { mem::GetDataRegion().InitPools(); });
    tasks.emplace_back(VHandle::InitPool);
    tasks.emplace_back(RowEntity::InitPool);
    tasks.emplace_back(IndexEntity::InitPool);

    tasks.emplace_back(util::Impl<PromiseAllocationService>);
    tasks.emplace_back(util::Impl<PromiseRoutineDispatchService>);
    tasks.emplace_back(
        []() {
          util::InstanceInit<EpochManager>();
          util::InstanceInit<SliceMappingTable>();
        });
    for (auto &t: tasks) t.join();

    mem::PrintMemStats();

    logger->info("Memory allocated: {}MB in total", mem::TotalMemoryAllocated() >> 20);
  }
};

static AllocatorModule allocator_module;

class CoroutineModule : public Module<CoreModule> {
  class CoroutineStackAllocator : public go::RoutineStackAllocator {
    mem::Pool stack_pool;
    mem::Pool context_pool;
    static constexpr int kMaxRoutines = 512;
    static constexpr int kStackSize = 512 << 10;
   public:
    CoroutineStackAllocator() {
      stack_pool = mem::Pool(mem::Coroutine, kStackSize, kMaxRoutines);
      context_pool = mem::Pool(mem::Coroutine, kContextSize, kMaxRoutines);
      stack_pool.Register();
      context_pool.Register();
    }
    void AllocateStackAndContext(
        size_t &stack_size, ucontext * &ctx_ptr, void * &stack_ptr) override final {
      stack_size = kStackSize;
      ctx_ptr = (ucontext *) context_pool.Alloc();
      stack_ptr = stack_pool.Alloc();
      memset(ctx_ptr, 0, kContextSize);
    }
    void FreeStackAndContext(ucontext *ctx_ptr, void *stack_ptr) override final {
      context_pool.Free(ctx_ptr);
      stack_pool.Free(stack_ptr);
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
    go::InitThreadPool(NodeConfiguration::g_nr_threads + 1, new CoroutineStackAllocator());

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
