#include <iostream>
#include <fstream>

#include "module.h"

#include "node_config.h"
#include "console.h"
#include "index.h"
#include "log.h"

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
    InitializeLogger();

    std::ofstream pid_fout("/tmp/dolly.pid");
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
    mem::InitThreadLocalRegions(NodeConfiguration::kNrThreads + 1);
    for (int i = 0; i < NodeConfiguration::kNrThreads + 1; i++) {
      auto &r = mem::GetThreadLocalRegion(i);
      r.ApplyFromConf(console.FindConfigSection("mem"));
      // logger->info("setting up regions {}", i);
      r.InitPools(i / mem::kNrCorePerNode);
    }

    VHandle::InitPools();
    logger->info("Memory used {}MB", mem::Pool::gTotalAllocatedMem.load() / (1 << 20));
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
    go::InitThreadPool(NodeConfiguration::kNrThreads + 1);

    for (int i = 1; i <= NodeConfiguration::kNrThreads; i++) {
      // We need to change core affinity by kCoreShifting
      auto r = go::Make(
          [i] {
            util::PinToCPU(i - 1 + NodeConfiguration::kCoreShifting);
          });
      go::GetSchedulerFromPool(i)->WakeUp(r);
    }
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
