#ifndef YCSB_H
#define YCSB_H

#include "table_decl.h"
#include "epoch.h"
#include "slice.h"

#include "zipfian_random.h"

namespace ycsb {

enum class TableType : int {
  YCSBBase = 200,
  Ycsb,
};

struct Ycsb {
  static constexpr auto kTable = TableType::Ycsb;
  using Key = sql::YcsbKey;
  using Value = sql::YcsbValue;
};

using RandRng = foedus::assorted::ZipfianRandom;

class Client : public felis::EpochClient {
  // Zipfian random generator
  RandRng rand;

  static constexpr double kTheta = 0.00;
 public:
  static constexpr size_t kTableSize = 400;

  Client() noexcept;
  unsigned int LoadPercentage() final override { return 100; }
  felis::BaseTxn *CreateTxn(uint64_t serial_id) final override;

  template <typename T> T GenerateTransactionInput();
};

class YcsbLoader : public go::Routine {
  std::mutex finish;
 public:
  YcsbLoader() {
    finish.lock();
  }
  void Run() override final;
  void Wait() { finish.lock(); }
};

}

namespace felis {

using namespace ycsb;

SHARD_TABLE(Ycsb) { return 0; }

}

#endif
