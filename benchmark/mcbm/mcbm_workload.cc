#include <chrono>
#include <thread>

#include "mcbm.h"
#include "module.h"
#include "opts.h"

#include "mcbm_insert.h"
#include "mcbm_lookup.h"
#include "mcbm_rangescan.h"
#include "mcbm_update.h"
#include "mcbm_delete.h"

#include <algorithm>
#include <random>

namespace util {

template <> struct FactoryTag<mcbm::TxnType, mcbm::TxnType::Insert> {
  using Type = mcbm::InsertTxn;
};

template <> struct FactoryTag<mcbm::TxnType, mcbm::TxnType::Lookup> {
  using Type = mcbm::LookupTxn;
};

template <> struct FactoryTag<mcbm::TxnType, mcbm::TxnType::Rangescan> {
  using Type = mcbm::RangescanTxn;
};

template <> struct FactoryTag<mcbm::TxnType, mcbm::TxnType::Update> {
  using Type = mcbm::UpdateTxn;
};

template <> struct FactoryTag<mcbm::TxnType, mcbm::TxnType::Delete> {
  using Type = mcbm::DeleteTxn;
};

}

namespace felis {


class McBmModule : public Module<WorkloadModule> {
 public:
  McBmModule() {
    info = {
      .name = "mcbm",
      .description = "McBm (Single Node Only)",
    };
  }
  void Init() override {
    Module<CoreModule>::InitModule("node-server");
    Module<CoreModule>::InitModule("allocator");

    // shirley: initialize the insert rows random order.
    for (uint64_t i = 0; i < g_mcbm_config.nr_rows; i++) {
      g_mcbm_config.insert_row_ids.push_back(i);
      // g_mcbm_config.insert_row_ids.push_back(i * (0xFFFFFFFFFFFFFFFF / g_mcbm_config.nr_rows));
    }
    // shuffle the insert rows.
    auto rng = std::default_random_engine {};
    std::shuffle(std::begin(g_mcbm_config.insert_row_ids), std::end(g_mcbm_config.insert_row_ids), rng);
    
    auto loader = new mcbm::McBmLoader();
    go::GetSchedulerFromPool(1)->WakeUp(loader);
    loader->Wait();

    if (felis::Options::kRecovery) {
      logger->info("Loading recovery data...");
      std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      std::atomic_int count_down(NodeConfiguration::g_nr_threads);
      for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
        auto sched_recovery = go::GetSchedulerFromPool(i + 1);
        auto loader_recovery = new mcbm::McBmLoaderRecovery(&count_down);
        sched_recovery->WakeUp(loader_recovery);
      }
      int load_elapse = 0;
      while (count_down.load() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        // sleep(1);
        // load_elapse++;
      }
      std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
      printf("recovery loader done %lld [ms]\n", std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count());
      // logger->info("recovery loader done {} seconds", load_elapse);
    }

    mcbm::TxnFactory::Initialize();

    EpochClient::g_workload_client = new mcbm::Client();
  }
};

static McBmModule mcbm_module;

}
