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
  std::mutex *m;
  std::atomic_int *count_down;
 public:
  LoaderBuilder(std::mutex *m, std::atomic_int *count_down)
      : m(m), count_down(count_down) {}

  template <enum tpcc::loaders::LoaderType TLT>
  tpcc::loaders::Loader<TLT> *CreateLoader(unsigned long seed) {
    return new tpcc::loaders::Loader<TLT>(seed, m, count_down);
  }
};

static go::Scheduler *SelectThreadPool(int idx)
{
  int pool_id = 2 + idx % (NodeConfiguration::g_nr_threads - 1);
  return go::GetSchedulerFromPool(pool_id);
}

static void LoadTPCCDataSet()
{
  std::mutex m;
  std::atomic_int count_down(6);

  m.lock(); // use as a semaphore

  LoaderBuilder builder(&m, &count_down);

  int i = 1;
  SelectThreadPool(i++)->WakeUp(builder.CreateLoader<tpcc::loaders::Warehouse>(9324));
  SelectThreadPool(i++)->WakeUp(builder.CreateLoader<tpcc::loaders::Item>(235443));
  SelectThreadPool(i++)->WakeUp(builder.CreateLoader<tpcc::loaders::Stock>(89785943));
  SelectThreadPool(i++)->WakeUp(builder.CreateLoader<tpcc::loaders::District>(129856349));
  SelectThreadPool(i++)->WakeUp(builder.CreateLoader<tpcc::loaders::Customer>(923587856425));
  SelectThreadPool(i++)->WakeUp(builder.CreateLoader<tpcc::loaders::Order>(2343352));

  m.lock(); // waits
  tpcc::RunShipment();
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
    LoadTPCCDataSet();

    tpcc::TxnFactory::Initialize();

    EpochClient::gWorkloadClient = new tpcc::Client();
  }
};

static TPCCModule tpcc_module;

}
