#include "smallbank.h"
#include "module.h"
#include "opts.h"

#include "balance.h"
#include "deposit_checking.h"
#include "transact_saving.h"
#include "amalgamate.h"
#include "write_check.h"

namespace util {

template <> struct FactoryTag<smallbank::TxnType, smallbank::TxnType::Balance> {
  using Type = smallbank::BalanceTxn;
};

template <> struct FactoryTag<smallbank::TxnType, smallbank::TxnType::DepositChecking> {
  using Type = smallbank::DepositCheckingTxn;
};

template <> struct FactoryTag<smallbank::TxnType, smallbank::TxnType::TransactSaving> {
  using Type = smallbank::TransactSavingTxn;
};

template <> struct FactoryTag<smallbank::TxnType, smallbank::TxnType::Amalgamate> {
  using Type = smallbank::AmalgamateTxn;
};

template <> struct FactoryTag<smallbank::TxnType, smallbank::TxnType::WriteCheck> {
  using Type = smallbank::WriteCheckTxn;
};

}

namespace felis {


class SmallBankModule : public Module<WorkloadModule> {
 public:
  SmallBankModule() {
    info = {
      .name = "smallbank",
      .description = "SmallBank (Single Node Only)",
    };
  }
  void Init() override {
    Module<CoreModule>::InitModule("node-server");
    Module<CoreModule>::InitModule("allocator");

    auto loader = new smallbank::SmallBankLoader();
    go::GetSchedulerFromPool(1)->WakeUp(loader);
    loader->Wait();

    if (felis::Options::kRecovery) {
      logger->info("Loading recovery data...");
      std::atomic_int count_down(NodeConfiguration::g_nr_threads);
      for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
        auto sched_recovery = go::GetSchedulerFromPool(i + 1);
        auto loader_recovery = new smallbank::SmallBankLoaderRecovery(&count_down);
        sched_recovery->WakeUp(loader_recovery);
      }
      int load_elapse = 0;
      while (count_down.load() > 0) {
        sleep(1);
        load_elapse++;
      }
      logger->info("recovery loader done {} seconds", load_elapse);
    }

    smallbank::TxnFactory::Initialize();

    EpochClient::g_workload_client = new smallbank::Client();
  }
};

static SmallBankModule smallbank_module;

}
