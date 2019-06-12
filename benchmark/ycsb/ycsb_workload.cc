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

    if (Options::kYcsbTableSize) {
      ycsb::Client::g_table_size = Options::kYcsbTableSize.ToLargeNumber();
    }
    if (Options::kYcsbSkewFactor) {
      ycsb::Client::g_theta = 0.01 * Options::kYcsbSkewFactor.ToInt();
    }

    ycsb::Client::g_enable_partition = Options::kYcsbEnablePartition;

    auto loader = new ycsb::YcsbLoader();
    go::GetSchedulerFromPool(1)->WakeUp(loader);
    loader->Wait();

    EpochClient::g_workload_client = new ycsb::Client();
  }
};

static YcsbModule ycsb_module;

}
