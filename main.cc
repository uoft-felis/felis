#include <unistd.h>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "index.h"
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
    mem::InitThreadLocalRegions(NodeConfiguration::kNrThreads);
    for (int i = 0; i < NodeConfiguration::kNrThreads; i++) {
      auto &r = mem::GetThreadLocalRegion(i);
      r.ApplyFromConf("mem.json");
      // logger->info("setting up regions {}", i);
      r.InitPools(i / mem::kNrCorePerNode);
    }

    VHandle::InitPools();
  }
  std::string name() const override {
    return "Memory Pool";
  }
};

static MemoryPoolModule mem_pool_module;

class CoroutineModule : public Module<CoreModule> {
 public:
  void Init() override {
    go::InitThreadPool(NodeConfiguration::kNrThreads);
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

// Platform Supports
// We need to support EL6, which uses glibc 2.12

extern "C" {
  // Looks like we are not using memcpy anyway, so why don't we just make EL6 happy?
  asm (".symver memcpy, memcpy@GLIBC_2.2.5");
  asm (".symver getenv, getenv@GLIBC_2.2.5");
  asm (".symver clock_gettime, clock_gettime@GLIBC_2.2.5");
  void *__wrap_memcpy(void *dest, const void *src, size_t n) { return memcpy(dest, src, n); }
  char *__wrap_secure_getenv(const char *name) { return getenv(name); }
  int __wrap_clock_gettime(clockid_t clk_id, struct timespec *tp) { return clock_gettime(clk_id, tp); }
}

int main(int argc, char *argv[])
{
  int opt;
  std::string workload_name;
  std::string node_name;
  while ((opt = getopt(argc, argv, "w:n:")) != -1) {
    switch (opt) {
      case 'w':
        workload_name = std::string(optarg);
        break;
      case 'n':
        node_name = std::string(optarg);
        break;
      default:
        show_usage(argv[0]);
        break;
    }
  }

  if (node_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  if (workload_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  util::Instance<dolly::NodeConfiguration>().set_node_id(std::stoi(node_name));

  logger->info("Running {} workload", workload_name);
  dolly::Module<dolly::CoreModule>::ShowModules();
  dolly::Module<dolly::WorkloadModule>::ShowModules();

  dolly::Module<dolly::CoreModule>::InitAllModules();
  dolly::Module<dolly::WorkloadModule>::InitAllModules();

  return 0;
}
