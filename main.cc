#include <unistd.h>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "epoch.h"
#include "index.h"
#include "client.h"
#include "node_config.h"

#include "module.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

#include "log.h"

void show_usage(const char *progname)
{
  printf("Usage: %s [-t core_count] [-d]\n\n", progname);
  puts("\t-t\tnumber of cores used");
  puts("\t-d\tdump only");
  puts("\t-r\treplay from trace files");

  std::exit(-1);
}

namespace dolly {

class LoggingModule : public Module<CoreModule> {
 public:
  void Init() override {
    InitializeLogger();

    std::ofstream pid_fout("/tmp/dolly.pid");
    pid_fout << (unsigned long) getpid();
  }
  std::string name() const override {
    return "Logging";
  }
};

static LoggingModule logging_module;

class MemoryPoolModule : public Module<CoreModule> {
 public:
  void Init() override {
    Epoch::InitPools();

    mem::InitThreadLocalRegions(Epoch::kNrThreads);
    for (int i = 0; i < Epoch::kNrThreads; i++) {
      auto &r = mem::GetThreadLocalRegion(i);
      r.ApplyFromConf("mem.json");
      // logger->info("setting up regions {}", i);
      r.InitPools(i / mem::kNrCorePerNode);
    }

    VHandle::InitPools();
    Txn::InitPools();
  }
  std::string name() const override {
    return "Memory Pool";
  }
};

static MemoryPoolModule mem_pool_module;

class CoroutineModule : public Module<CoreModule> {
 public:
  void Init() override {
    go::InitThreadPool(Epoch::kNrThreads);
  }
  std::string name() const override {
    return "Coroutine Thread Pool";
  }
};

static CoroutineModule coroutine_module;

class NodeServerModule : public Module<CoreModule> {
 public:
  void Init() override {
    util::Instance<NodeConfiguration>().RunAllServers();
  }
  std::string name() const override {
    return std::string("DB Node Server");
  }
};

static NodeServerModule server_module;

}

int main(int argc, char *argv[])
{
  int opt;
  std::string workload_name;

  while ((opt = getopt(argc, argv, "rw:s:c:t:")) != -1) {
    switch (opt) {
      case 'w':
        workload_name = std::string(optarg);
        break;
      default:
        show_usage(argv[0]);
        break;
    }
  }

  if (workload_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  logger->info("Running {} workload", workload_name);
  dolly::Module<dolly::CoreModule>::ShowModules();
  dolly::Module<dolly::WorkloadModule>::ShowModules();

  dolly::Module<dolly::CoreModule>::InitAllModules();
  dolly::Module<dolly::WorkloadModule>::InitAllModules();

  return 0;
}
