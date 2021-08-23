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

    smallbank::TxnFactory::Initialize();

    EpochClient::g_workload_client = new smallbank::Client();
  }
};

static SmallBankModule smallbank_module;

}
