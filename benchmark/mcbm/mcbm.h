#ifndef MCBM_H
#define MCBM_H

#include "table_decl.h"

#include "epoch.h"
#include "slice.h"
#include "index.h"
#include "sqltypes.h"

#include "util/objects.h"
#include "util/random.h"
#include "util/factory.h"



namespace mcbm {

using util::Instance;

class DummySliceRouter {
public:
  static int SliceToNodeId(int16_t slice_id) { return 1; } // Always on node 1
};

struct Config {
  size_t nr_rows;
  int hotspot_percent;
  int hotspot_number;
  std::vector<uint64_t> insert_row_ids;
  std::atomic_int insert_cnt; // used when generating
  Config();
};

extern Config g_mcbm_config;

enum class TableType : int {
  McBmBase = 400,
  MBTable,
  NRTable
};

struct MBTable {
  static constexpr auto kTable = TableType::MBTable;
  static constexpr auto kIndexArgs = std::make_tuple(true);

  using IndexBackend = felis::DptreeIndex;
  using Key = sql::McbmKey;
  using Value = sql::McbmValue;
};


class ClientBase {
protected:
  util::FastRandom r;

protected:

  uint64_t PickRow();

  uint64_t PickRowNoDup();

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
  Insert,
  Lookup,
  Rangescan,
  Update,
  Delete,

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
  void PersistTxnStruct(int txn_id, void *base_txn, void *txn_struct_buffer) final override;
  void PersistAutoInc() final override { return; }
  void IdxMerge() final override;
  void IdxLog() final override;
};

using TxnFactory =
    util::Factory<felis::BaseTxn, TxnType, TxnType::AllTxn, Client *, uint64_t>;

class McBmLoader : public go::Routine {
  std::atomic_bool done = false;
 public:
  McBmLoader() {}
  void Run() override final;
  void Wait() { while (!done) sleep(1); }
};

class McBmLoaderRecovery : public go::Routine {
  std::atomic_int *count_down;
 public:
  McBmLoaderRecovery(std::atomic_int *count_down) : count_down(count_down) {}
  void DoLoadRecovery();
  void Run() {
    DoLoadRecovery();
    count_down->fetch_sub(1);
  }
};


}

namespace felis {

using namespace mcbm;

SHARD_TABLE(MBTable) { return 0; }
}

#endif
