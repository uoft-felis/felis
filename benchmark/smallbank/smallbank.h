#ifndef SMALLBANK_H
#define SMALLBANK_H

#include "table_decl.h"

#include "epoch.h"
#include "slice.h"
#include "index.h"

#include "util/objects.h"
#include "util/random.h"
#include "util/factory.h"



namespace smallbank {

using util::Instance;

class DummySliceRouter {
public:
  static int SliceToNodeId(int16_t slice_id) { return 1; } // Always on node 1
};

struct Config {
  size_t nr_accounts;
  int hotspot_percent;
  int hotspot_number;
  Config();
};

extern Config g_smallbank_config;

enum class TableType : int {
  SmallBankBase = 300,
  Account,
  Saving,
  Checking,
  NRTable
};

struct Account {
  static constexpr auto kTable = TableType::Account;
  static constexpr auto kIndexArgs = std::make_tuple(true);

  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::AccountKey;
  using Value = sql::AccountValue;
};

struct Saving {
  static constexpr auto kTable = TableType::Saving;
  static constexpr auto kIndexArgs = std::make_tuple(true);

  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::SavingKey;
  using Value = sql::SavingValue;
};

struct Checking {
  static constexpr auto kTable = TableType::Checking;
  static constexpr auto kIndexArgs = std::make_tuple(true);

  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::CheckingKey;
  using Value = sql::CheckingValue;
};


class ClientBase {
protected:
  util::FastRandom r;

protected:

  uint64_t PickAccount();

  static int CheckBetweenInclusive(int v, int lower, int upper);

  int RandomNumber(int min, int max);
  int RandomNumberExcept(int min, int max, int exception) {
    int r;
    do {
      r = RandomNumber(min, max);
    } while (r == exception);
    return r;
  }

  int NonUniformRandom(int A, int C, int min, int max);

  std::string RandomStr(uint len);
  std::string RandomNStr(uint len);

public:
  static felis::TableManager &tables() {
    return util::Instance<felis::TableManager>();
  }

public:
  ClientBase(const util::FastRandom &r);

  template <class T> T GenerateTransactionInput();
};


enum class TxnType : int {
  Balance,
  DepositChecking,
  TransactSaving,
  Amalgamate,
  WriteCheck,

  AllTxn,
};

class Client : public felis::EpochClient, public ClientBase {
  unsigned long dice;

public:
  static constexpr unsigned long kClientSeed = 0xdeadbeef;

  Client()
      : felis::EpochClient(), dice(0),
        ClientBase(kClientSeed) {}
  unsigned int LoadPercentage() final override { return 100; }

protected:
  felis::BaseTxn *CreateTxn(uint64_t serial_id, void *txntype_id, void *txn_struct_buffer) final override;
  felis::BaseTxn *CreateTxnRecovery(uint64_t serial_id, int txntype_id, void *txn_struct_buffer) final override;
  size_t TxnInputSize(int txn_id) final override;
};

using TxnFactory =
    util::Factory<felis::BaseTxn, TxnType, TxnType::AllTxn, Client *, uint64_t>;

class SmallBankLoader : public go::Routine {
  std::atomic_bool done = false;
 public:
  SmallBankLoader() {}
  void Run() override final;
  void Wait() { while (!done) sleep(1); }
};

class SmallBankLoaderRecovery : public go::Routine {
  std::atomic_int *count_down;
 public:
  SmallBankLoaderRecovery(std::atomic_int *count_down) : count_down(count_down) {}
  void DoLoadRecovery();
  void Run() {
    DoLoadRecovery();
    count_down->fetch_sub(1);
  }
};


}

namespace felis {

using namespace smallbank;

SHARD_TABLE(Account) { return 0; }
SHARD_TABLE(Saving) { return 0; }
SHARD_TABLE(Checking) { return 0; }
}

#endif
