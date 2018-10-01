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

template <enum tpcc::loaders::LoaderType TLT>
static tpcc::loaders::Loader<TLT> *CreateLoader(unsigned long seed, std::mutex *m,
						std::atomic_int *count_down, int cpu)
{
  return new tpcc::loaders::Loader<TLT>(seed, m, count_down, cpu);
}

static void LoadTPCCDataSet()
{
  std::mutex m;
  std::atomic_int count_down(6);
  m.lock(); // use as a semaphore

  go::GetSchedulerFromPool(1)->WakeUp(CreateLoader<tpcc::loaders::Warehouse>(9324, &m, &count_down, 0));
  go::GetSchedulerFromPool(2)->WakeUp(CreateLoader<tpcc::loaders::Item>(235443, &m, &count_down, 1));
  go::GetSchedulerFromPool(3)->WakeUp(CreateLoader<tpcc::loaders::Stock>(89785943, &m, &count_down, 2));
  go::GetSchedulerFromPool(4)->WakeUp(CreateLoader<tpcc::loaders::District>(129856349, &m, &count_down, 3));
  go::GetSchedulerFromPool(5)->WakeUp(CreateLoader<tpcc::loaders::Customer>(923587856425, &m, &count_down, 4));
  go::GetSchedulerFromPool(6)->WakeUp(CreateLoader<tpcc::loaders::Order>(2343352, &m, &count_down, 5));

  m.lock(); // waits
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

    Instance<tpcc::TableHandles>();
    LoadTPCCDataSet();
  }
};

static TPCCModule tpcc_module;

}
