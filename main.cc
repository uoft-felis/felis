#include <unistd.h>
#include <cstdio>

#include "module.h"
#include "node_config.h"
#include "console.h"

void show_usage(const char *progname)
{
  printf("Usage: %s -w workload -n node_name -c controller_ip\n\n", progname);
  puts("\t-w\tworkload name");
  puts("\t-n\tnode name");
  puts("\t-c\tcontroller IP address");

  std::exit(-1);
}

#ifdef EL6_COMPAT

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
#endif

namespace felis {

void ParseControllerAddress(std::string arg);

}

int main(int argc, char *argv[])
{
  int opt;
  std::string workload_name;
  std::string node_name;
  while ((opt = getopt(argc, argv, "w:n:c:")) != -1) {
    switch (opt) {
      case 'w':
        workload_name = std::string(optarg);
        break;
      case 'n':
        node_name = std::string(optarg);
        break;
      case 'c':
        felis::ParseControllerAddress(std::string(optarg));
        break;
      default:
        show_usage(argv[0]);
        break;
    }
  }

  felis::Module<felis::CoreModule>::ShowAllModules();
  felis::Module<felis::WorkloadModule>::ShowAllModules();
  puts("\n");

  if (node_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  if (workload_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  auto &console = util::Instance<felis::Console>();
  console.set_server_node_name(node_name);

  felis::Module<felis::CoreModule>::InitRequiredModules();

  util::Instance<felis::NodeConfiguration>().SetupNodeName(node_name);
  felis::Module<felis::CoreModule>::InitModule("node-server");

  console.WaitForServerStatus(felis::Console::ServerStatus::Exiting);

  return 0;
}
