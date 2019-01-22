#include <iostream>
#include <fstream>
#include <sys/mman.h>

#include "module.h"

#include "node_config.h"
#include "console.h"
#include "index.h"
#include "log.h"
#include "promise.h"

#include "gopp/gopp.h"
#include "gopp/channels.h"

#include "iface.h"

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
    InitializeLogger();

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
    mem::InitThreadLocalRegions(NodeConfiguration::g_nr_threads + 1);
    for (int i = 0; i < NodeConfiguration::g_nr_threads + 1; i++) {
      auto &r = mem::GetThreadLocalRegion(i);
      r.ApplyFromConf(console.FindConfigSection("mem"));
      // logger->info("setting up regions {}", i);
      tasks.emplace_back(
          [&r, i]() {
            auto node = (i + NodeConfiguration::g_core_shifting) / mem::kNrCorePerNode;
            r.InitPools(node);
          });
    }
    tasks.emplace_back(VHandle::InitPools);
    // tasks.emplace_back(SkipListVHandle::Block::InitPool);
    tasks.emplace_back(util::Impl<PromiseAllocationService>);
    tasks.emplace_back(util::Impl<PromiseRoutineDispatchService>);
    tasks.emplace_back(RowEntity::InitPools);
    for (auto &t: tasks) t.join();

    logger->info("Memory used: {}MB in regular pages, {}MB in huge pages.",
                 mem::Pool::g_total_page_mem.load() / (1 << 20),
                 mem::Pool::g_total_hugepage_mem.load() / (1 << 20));
  }
};

static AllocatorModule allocator_module;

class CoroutineModule : public Module<CoreModule> {
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
    go::InitThreadPool(NodeConfiguration::g_nr_threads + 1);

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
