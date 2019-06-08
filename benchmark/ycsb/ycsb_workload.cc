#include "ycsb.h"
#include "module.h"

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

    auto loader = new ycsb::YcsbLoader();
    go::GetSchedulerFromPool(1)->WakeUp(loader);
    loader->Wait();

    EpochClient::g_workload_client = new ycsb::Client();
  }
};

static YcsbModule ycsb_module;

}
