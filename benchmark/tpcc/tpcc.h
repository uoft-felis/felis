// -*- c++ -*-

#ifndef TPCC_H
#define TPCC_H

#include "table_decl.h"

#include <array>
#include <cassert>
#include <map>
#include <string>
#include <vector>

#include "epoch.h"
#include "index.h"
#include "sqltypes.h"
#include "util.h"

#include "slice.h"
#include "xxHash/xxhash.h"

namespace tpcc {

using util::Instance;
using felis::NodeConfiguration;
template <typename TableType> using SliceLocator = felis::SliceLocator<TableType>;

struct Config {
  bool uniform_item_distribution;

  size_t nr_items;
  size_t nr_warehouses;
  size_t districts_per_warehouse;
  size_t customers_per_district;

  uint64_t hotspot_warehouse_bitmap; // If a warehouse is hot, the bit is 1
  std::vector<int> offload_nodes;

  uint hotspot_load_percentage;
  size_t max_supported_warehouse;

  bool shard_by_warehouse;

  Config();
};

extern Config g_tpcc_config;

enum class TableType : int {
  TPCCBase = 100,
  Customer,
  CustomerNameIdx,
  District,
  History,
  Item,
  NewOrder,
  OOrder,
  OOrderCIdIdx,
  OrderLine,
  Stock,
  StockData,
  Warehouse,
  NRTable
};

// table and schemas definition
struct Customer {
  static constexpr auto kTable = TableType::Customer;
  using Key = sql::CustomerKey;
  using Value = sql::CustomerValue;
};

struct CustomerNameIdx {
  static constexpr auto kTable = TableType::CustomerNameIdx;
  using Key = sql::CustomerNameIdxKey;
  using Value = sql::CustomerNameIdxValue;
};

struct District {
  static constexpr auto kTable = TableType::District;
  using Key = sql::DistrictKey;
  using Value = sql::DistrictValue;
};

struct History {
  static constexpr auto kTable = TableType::History;
  using Key = sql::HistoryKey;
  using Value = sql::HistoryValue;
};

struct Item {
  static constexpr auto kTable = TableType::Item;
  using Key = sql::ItemKey;
  using Value = sql::ItemValue;
};

struct NewOrder {
  static constexpr auto kTable = TableType::NewOrder;
  using Key = sql::NewOrderKey;
  using Value = sql::NewOrderValue;
};

struct OOrder {
  static constexpr auto kTable = TableType::OOrder;
  using Key = sql::OOrderKey;
  using Value = sql::OOrderValue;
};

struct OOrderCIdIdx {
  static constexpr auto kTable = TableType::OOrderCIdIdx;
  using Key = sql::OOrderCIdIdxKey;
  using Value = sql::OOrderCIdIdxValue;
};

struct OrderLine {
  static constexpr auto kTable = TableType::OrderLine;
  using Key = sql::OrderLineKey;
  using Value = sql::OrderLineValue;
};

struct Stock {
  static constexpr auto kTable = TableType::Stock;
  using Key = sql::StockKey;
  using Value = sql::StockValue;
};

struct StockData {
  static constexpr auto kTable = TableType::StockData;
  using Key = sql::StockDataKey;
  using Value = sql::StockDataValue;
};

struct Warehouse {
  static constexpr auto kTable = TableType::Warehouse;
  using Key = sql::WarehouseKey;
  using Value = sql::WarehouseValue;
};

void InitializeTPCC();
void InitializeSliceManager();
void SendIndexSnapshot();

class ClientBase {
 protected:
  util::FastRandom r;
  int node_id;

 protected:
  static constexpr double kWarehouseSpread = 0.0;
  static constexpr double kNewOrderRemoteItem = 0.01;
  static constexpr double kCreditCheckRemoteCustomer = 0.15;
  static constexpr double kPaymentRemoteCustomer = 0.15;

  // We simply disabled this payment by name. Our programming model does not
  // support this, and it sounds ridiculous in practice.

  // static constexpr double kPaymentByName = 0.60;

  size_t nr_warehouses() const;
  uint PickWarehouse();
  uint PickDistrict();

  uint LoadPercentageByWarehouse();

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

  int GetItemId();
  int GetCustomerId();

  size_t GetCustomerLastName(uint8_t *buf, int num);
  size_t GetCustomerLastName(char *buf, int num) {
    return GetCustomerLastName((uint8_t *)buf, num);
  }
  std::string GetCustomerLastName(int num) {
    std::string ret;
    // all tokens are at most 5 chars long
    ret.resize(5 * 3);
    ret.resize(GetCustomerLastName(&ret[0], num));
    return ret;
  }

  std::string GetNonUniformCustomerLastNameLoad() {
    return GetCustomerLastName(NonUniformRandom(255, 157, 0, 999));
  }

  size_t GetNonUniformCustomerLastNameRun(uint8_t *buf) {
    return GetCustomerLastName(buf, NonUniformRandom(255, 223, 0, 999));
  }
  size_t GetNonUniformCustomerLastNameRun(char *buf) {
    return GetNonUniformCustomerLastNameRun((uint8_t *)buf);
  }
  std::string GetNonUniformCustomerLastNameRun() {
    return GetCustomerLastName(NonUniformRandom(255, 223, 0, 999));
  }

  std::string RandomStr(uint len);
  std::string RandomNStr(uint len);

  uint GetCurrentTime();

 public:
  template <typename TableType, typename KeyType>
  static void OnNewRow(int slice_id, TableType table, const KeyType &k,
                       felis::VHandle *handle) {
    util::Instance<felis::SliceManager>().OnNewRow(
        slice_id, static_cast<int>(table), k.Encode(), handle);
  }

  static void OnUpdateRow(felis::VHandle *handle) {
    util::Instance<felis::SliceManager>().OnUpdateRow(handle);
  }
  ClientBase(const util::FastRandom &r, const int node_id, const int nr_nodes);
  static felis::Relation &relation(TableType table);
  static bool is_warehouse_hotspot(uint wid);

  template <class T> T GenerateTransactionInput();
};


class TpccSliceRouter {
 public:
  static int SliceToNodeId(int16_t slice_id);
  static int SliceToCoreId(int16_t slice_id);
};

// loaders for each table
namespace loaders {

enum LoaderType { Warehouse, Item, Stock, District, Customer, Order };

class BaseLoader : public tpcc::ClientBase {
 public:

  static void SetAllocAffinity(int aff) {
    mem::ParallelPool::SetCurrentAffinity(aff);
  }
  static void RestoreAllocAffinity() {
    mem::ParallelPool::SetCurrentAffinity(-1);
  }
  using tpcc::ClientBase::ClientBase;
};

template <enum LoaderType TLN>
class Loader : public go::Routine, public BaseLoader {
  std::mutex *m;
  std::atomic_int *count_down;

 public:
  Loader(unsigned long seed, std::mutex *w, std::atomic_int *c)
      : m(w), count_down(c),
        BaseLoader(util::FastRandom(seed),
                   Instance<NodeConfiguration>().node_id(),
                   Instance<NodeConfiguration>().nr_nodes()) {}

  void DoLoad();
  virtual void Run() {
    DoLoad();
    if (count_down->fetch_sub(1) == 1)
      m->unlock();
  }

  template <typename TableType, typename F>
  void DoOnSlice(const typename TableType::Key &k, F func) {
    auto slice_id = util::Instance<SliceLocator<TableType>>().Locate(k);
    if (TpccSliceRouter::SliceToNodeId(slice_id) == node_id) {
      auto core_id = TpccSliceRouter::SliceToCoreId(slice_id);
      // util::PinToCPU(core_id);
      func(slice_id, core_id);
      RestoreAllocAffinity();
    }
  }
};

} // namespace loaders

enum class TxnType : int {
  NewOrder,
  Payment,
  Delivery,
  // CreditCheck,

  AllTxn,
};

class Client : public felis::EpochClient, public ClientBase {
  unsigned long dice;

 public:
  static constexpr unsigned long kClientSeed = 0xdeadbeef;

  Client()
      : felis::EpochClient(), dice(0),
        ClientBase(kClientSeed,
                   util::Instance<felis::NodeConfiguration>().node_id(),
                   util::Instance<felis::NodeConfiguration>().nr_nodes()) {}

  unsigned int LoadPercentage() final override {
    return LoadPercentageByWarehouse();
  }

  // XXX: hack for delivery transaction
  int last_no_o_ids[10];

 protected:
  felis::BaseTxn *CreateTxn(uint64_t serial_id) final override;
};

using TxnFactory =
    util::Factory <felis::BaseTxn, int(TxnType::AllTxn), Client *, uint64_t>;

}

#define ROW_SLICE_MAPPING_DIRECT

namespace felis {

using namespace tpcc;

SHARD_TABLE(Customer) { return key.c_w_id - 1; }
SHARD_TABLE(CustomerNameIdx) { return key.c_w_id - 1; }
SHARD_TABLE(District) { return key.d_w_id - 1; }
SHARD_TABLE(History) { return key.h_w_id - 1; }
SHARD_TABLE(NewOrder) { return key.no_w_id - 1; }
SHARD_TABLE(OOrder) { return key.o_w_id - 1; }
SHARD_TABLE(OOrderCIdIdx) { return key.o_w_id - 1; }
SHARD_TABLE(OrderLine) { return key.ol_w_id - 1; }
READ_ONLY_TABLE(Item);
SHARD_TABLE(Stock) { return key.s_w_id - 1; }
SHARD_TABLE(StockData) { return key.s_w_id - 1; }
SHARD_TABLE(Warehouse) { return key.w_id - 1; }

} // namespace felis

#endif /* TPCC_H */
