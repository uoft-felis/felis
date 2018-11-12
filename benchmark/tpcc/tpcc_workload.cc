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
  felis::IndexShipment *shipment;
 public:
  LoaderBuilder(std::mutex *m, std::atomic_int *count_down, felis::IndexShipment *shipment)
      : m(m), count_down(count_down), shipment(shipment) {}

  template <enum tpcc::loaders::LoaderType TLT>
  tpcc::loaders::Loader<TLT> *CreateLoader(unsigned long seed) {
    return new tpcc::loaders::Loader<TLT>(seed, m, count_down, shipment);
  }
};

static void LoadTPCCDataSet()
{
  std::mutex m;
  std::atomic_int count_down(6);

  // HACK HACK HACK!
  felis::IndexShipment *preship = nullptr;
  auto &conf = Instance<NodeConfiguration>();
  if (conf.node_id() == 2) {
    preship = new felis::IndexShipment(conf.config(1).index_shipper_peer);
    logger->info("preship is ready!");
  }

  m.lock(); // use as a semaphore

  LoaderBuilder builder(&m, &count_down, preship);

  go::GetSchedulerFromPool(1)->WakeUp(builder.CreateLoader<tpcc::loaders::Warehouse>(9324));
  go::GetSchedulerFromPool(2)->WakeUp(builder.CreateLoader<tpcc::loaders::Item>(235443));
  go::GetSchedulerFromPool(3)->WakeUp(builder.CreateLoader<tpcc::loaders::Stock>(89785943));
  go::GetSchedulerFromPool(4)->WakeUp(builder.CreateLoader<tpcc::loaders::District>(129856349));
  go::GetSchedulerFromPool(5)->WakeUp(builder.CreateLoader<tpcc::loaders::Customer>(923587856425));
  go::GetSchedulerFromPool(6)->WakeUp(builder.CreateLoader<tpcc::loaders::Order>(2343352));

  m.lock(); // waits

  while (preship && !preship->RunSend());
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
    Module<CoreModule>::InitModule("allocator");

    tpcc::InitializeTPCC();
    LoadTPCCDataSet();

    tpcc::TxnFactory::Initialize();

    EpochClient::gWorkloadClient = new tpcc::Client();
  }
};

static TPCCModule tpcc_module;

}
