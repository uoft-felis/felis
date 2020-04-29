#include "table_decl.h"

#include "tpcc.h"
#include "epoch.h"
#include "log.h"
#include "util.h"
#include "index.h"
#include "module.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

using util::MixIn;
using util::Instance;

namespace felis {

class LoaderBuilder {
  std::atomic_int *count_down;
 public:
  LoaderBuilder(std::atomic_int *count_down)
      : count_down(count_down) {}

  template <enum tpcc::loaders::LoaderType TLT>
  tpcc::loaders::Loader<TLT> *CreateLoader(unsigned long seed) {
    return new tpcc::loaders::Loader<TLT>(seed, count_down);
  }
};

// use Loader to generate initial data into the tables
static void LoadTPCCDataSet()
{
  std::atomic_int count_down(6 * NodeConfiguration::g_nr_threads);

  logger->info("Loading initial data...");

  LoaderBuilder builder(&count_down);

  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto sched = go::GetSchedulerFromPool(i + 1);
    sched->WakeUp(builder.CreateLoader<tpcc::loaders::Warehouse>(9324));
    sched->WakeUp(builder.CreateLoader<tpcc::loaders::Item>(235443));
    sched->WakeUp(builder.CreateLoader<tpcc::loaders::Stock>(89785943));
    sched->WakeUp(builder.CreateLoader<tpcc::loaders::District>(129856349));
    sched->WakeUp(builder.CreateLoader<tpcc::loaders::Customer>(923587856425));
    sched->WakeUp(builder.CreateLoader<tpcc::loaders::Order>(2343352));
  }

  int load_elapse = 0;
  while (count_down.load() > 0) {
    sleep(1);
    load_elapse++;
  }
  logger->info("loader done {} seconds", load_elapse);
}

class TPCCModule : public Module<WorkloadModule> {
 public:
  TPCCModule() {
    info = {
      .name = "tpcc",
      .description = "TPC-C",
    };
  }
  void Init() override {
    Module<CoreModule>::InitModule("node-server");
    Module<CoreModule>::InitModule("allocator");

    tpcc::InitializeTPCC();
    tpcc::InitializeSliceManager();
    LoadTPCCDataSet();

    tpcc::TxnFactory::Initialize();

    EpochClient::g_workload_client = new tpcc::Client();
  }
};

static TPCCModule tpcc_module;

}
