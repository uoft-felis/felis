#include "ycsb.h"
#include "module.h"
#include "opts.h"

#include "benchmark/ycsb/ycsb_priority.h"

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
    ycsb::GeneratePriorityTxn();

    EpochClient::g_workload_client = new ycsb::Client();
  }
};

static YcsbModule ycsb_module;

}
