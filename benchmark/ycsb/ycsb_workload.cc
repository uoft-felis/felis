#include "ycsb.h"
#include "module.h"
#include "opts.h"

namespace felis {

class YcsbModule : public Module<WorkloadModule> {
 public:
  YcsbModule() {
    info = {
      .name = "ycsb",
      .description = "YCSB (Single Node Only)",
    };
  }
  void Init() override final {
    Module<CoreModule>::InitModule("node-server");
    Module<CoreModule>::InitModule("allocator");

    if (Options::kYcsbContentionKey) {
      ycsb::Client::g_contention_key = Options::kYcsbContentionKey.ToInt();
    }
    if (Options::kYcsbSkewFactor) {
      ycsb::Client::g_theta = 0.01 * Options::kYcsbSkewFactor.ToInt();
    }
    if (Options::kYcsbReadOnly)
      ycsb::Client::g_extra_read = Options::kYcsbReadOnly.ToInt();

    ycsb::Client::g_dependency = Options::kYcsbDependency;

    auto loader = new ycsb::YcsbLoader();
    go::GetSchedulerFromPool(1)->WakeUp(loader);
    loader->Wait();

    if (felis::Options::kRecovery) {
      std::atomic_int count_down(NodeConfiguration::g_nr_threads);
      for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
        auto sched_recovery = go::GetSchedulerFromPool(i + 1);
        auto loader_recovery = new ycsb::YcsbLoaderRecovery(&count_down);
        sched_recovery->WakeUp(loader_recovery);
      }
      int load_elapse = 0;
      while (count_down.load() > 0) {
        sleep(1);
        load_elapse++;
      }
      logger->info("recovery loader done {} seconds", load_elapse);
    }

    EpochClient::g_workload_client = new ycsb::Client();
  }
};

static YcsbModule ycsb_module;

}
