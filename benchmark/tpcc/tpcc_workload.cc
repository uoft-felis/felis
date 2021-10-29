#include <chrono>
#include <thread>

#include "table_decl.h"

#include "tpcc.h"
#include "epoch.h"
#include "log.h"
#include "util/objects.h"
#include "util/factory.h"
#include "index.h"
#include "module.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

#include "new_order.h"
#include "payment.h"
#include "delivery.h"
#include "stock_level.h"
#include "order_status.h"

using util::MixIn;
using util::Instance;

namespace util {

template <> struct FactoryTag<tpcc::TxnType, tpcc::TxnType::NewOrder> {
  using Type = tpcc::NewOrderTxn;
};

template <> struct FactoryTag<tpcc::TxnType, tpcc::TxnType::Payment> {
  using Type = tpcc::PaymentTxn;
};

template <> struct FactoryTag<tpcc::TxnType, tpcc::TxnType::Delivery> {
  using Type = tpcc::DeliveryTxn;
};

template <> struct FactoryTag<tpcc::TxnType, tpcc::TxnType::OrderStatus> {
  using Type = tpcc::OrderStatusTxn;
};

template <> struct FactoryTag<tpcc::TxnType, tpcc::TxnType::StockLevel> {
  using Type = tpcc::StockLevelTxn;
};

}

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

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

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
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // sleep(1);
    // load_elapse++;
  }
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  printf("(recovery) loader done %lld [ms]\n", std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count());
  // logger->info("loader done {} seconds", load_elapse);
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
