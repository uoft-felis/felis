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
#include "util/objects.h"
#include "util/random.h"
#include "util/factory.h"

#include "slice.h"
#include "xxHash/xxhash.h"

namespace tpcc {

using util::Instance;
using felis::NodeConfiguration;
template <typename TableType> using SliceLocator = felis::SliceLocator<TableType>;

class TpccSliceRouter {
 public:
  static int SliceToNodeId(int16_t slice_id);
  static int SliceToCoreId(int16_t slice_id);
};

// Some tables doesn't have district_id, bohm need to partition them in a
// separate partition. So far, we have Stock(0), Warehouse(1)
// tables.
//
// Other than these. District(2-11), OrderLine(12-21), Customer(22-31),
// NewOrder(32-41), OOrder(42-51).
//
static constexpr int kPWVExtraPartitions = 2;

#define ASSERT_PWV_CONT                                            \
abort_if(g_tpcc_config.nr_warehouses != 1,                          \
           "PWV only support single warehouse for contention experiments")

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

  unsigned int max_slice_id() const;
  static unsigned int WarehouseToSliceId(int w) {
    return w - 1;
  }
  static unsigned int WarehouseToCoreId(int w) {
    return TpccSliceRouter::SliceToCoreId(WarehouseToSliceId(w));
  }
  bool IsWarehousePinnable();
  static int PWVDistrictToCoreId(int district, int base) {
    return (district - 1 + base) % (NodeConfiguration::g_nr_threads - kPWVExtraPartitions) + kPWVExtraPartitions;
  }

  template <typename T, int Suffix = 0>
  unsigned int HashKeyToSliceId(const T &k) const {
    static_assert(sizeof(T) > Suffix);
    return XXH32(&k, sizeof(T) - Suffix, 0x10cc) % max_slice_id();
  }
};

extern Config g_tpcc_config;

enum class TableType : int {
  TPCCBase = 100,
  Customer,
  CustomerInfo,
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
  static uint32_t HashKey(const felis::VarStrView &k) {
    uint8_t x[4];
    auto v = (uint32_t *) x;

    x[0] = k[11];
    x[1] = k[10] | (k[7] << 4);
    x[2] = k[3] - 1;
    x[3] = 0;

    return *v;
  }
  static constexpr auto kTable = TableType::Customer;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 8 << 20, true); //inlined was false
  using IndexBackend = felis::HashtableIndex;
  using Key = sql::CustomerKey;
  using Value = sql::CustomerValue;
};

struct CustomerInfo : public Customer {
  static constexpr auto kTable = TableType::CustomerInfo;
  using Value = sql::CustomerInfoValue;
};

#if 0

struct CustomerNameIdx {
  static constexpr auto kTable = TableType::CustomerNameIdx;
  static constexpr auto kIndexArgs = std::make_tuple(false);
  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::CustomerNameIdxKey;
  using Value = sql::CustomerNameIdxValue;
};

#endif

struct District {
  static uint32_t HashKey(const felis::VarStrView &k) {
    return (((k[3] - 1) << 9) | (k[7] - 1)) << 4;
  }
  static constexpr auto kTable = TableType::District;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 64 << 9, true); //inlined was false
  using IndexBackend = felis::HashtableIndex;
  using Key = sql::DistrictKey;
  using Value = sql::DistrictValue;
};

struct History {
  static constexpr auto kTable = TableType::History;
  static constexpr auto kIndexArgs = std::make_tuple(true); // inlined was false
  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::HistoryKey;
  using Value = sql::HistoryValue;
};

struct Item {
  static uint32_t HashKey(const felis::VarStrView &k) {
    uint8_t x[] = {(uint8_t) (k[3] - 1), k[2], k[1], 0};
    return *(uint32_t *) x;
  }

  static constexpr auto kTable = TableType::Item;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 2 << 20, true);
  using IndexBackend = felis::HashtableIndex;
  using Key = sql::ItemKey;
  using Value = sql::ItemValue;
};

struct NewOrder {
  static constexpr auto kTable = TableType::NewOrder;
  static constexpr auto kIndexArgs = std::make_tuple(true);
  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::NewOrderKey;
  using Value = sql::NewOrderValue;
};

struct OOrder {
  /*
  static uint32_t HashKey(const felis::VarStrView &k) {
    uint32_t p = District::HashKey(k) >> 9;
    uint8_t x[] = {k.data[11], k.data[10], (uint8_t) p, (uint8_t) (p >> 8)};
    return *(uint32_t *) x;
  }
  */
  static constexpr auto kTable = TableType::OOrder;
  // static constexpr auto kIndexArgs = std::make_tuple(HashKey, 32 << 20);
  static constexpr auto kIndexArgs = std::make_tuple(true);
  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::OOrderKey;
  using Value = sql::OOrderValue;
};

struct OOrderCIdIdx {
  static constexpr auto kTable = TableType::OOrderCIdIdx;
  static constexpr auto kIndexArgs = std::make_tuple(true);
  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::OOrderCIdIdxKey;
  using Value = sql::OOrderCIdIdxValue;
};

struct OrderLine {
  static constexpr auto kTable = TableType::OrderLine;
  static constexpr auto kIndexArgs = std::make_tuple(true);
  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::OrderLineKey;
  using Value = sql::OrderLineValue;
};

struct Stock {
  static uint32_t HashKey(const felis::VarStrView &k) {
    uint8_t x[] = {k[7], k[6], (uint8_t) (k[5] | ((k[3] - 1) << 1)), 0};
    return *(uint32_t *) x;
  }
  static constexpr auto kTable = TableType::Stock;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 16 << 20, true); //inlined was false
  using IndexBackend = felis::HashtableIndex;
  using Key = sql::StockKey;
  using Value = sql::StockValue;
};

// only for credit check?
#if 0
struct StockData {
  static constexpr auto kTable = TableType::StockData;
  static constexpr auto kIndexArgs = std::make_tuple();
  using IndexBackend = felis::MasstreeIndex;
  using Key = sql::StockDataKey;
  using Value = sql::StockDataValue;
};
#endif

struct Warehouse {
  static uint32_t HashKey(const felis::VarStrView &k) {
    return (k[3] - 1) << 9;
  }

  static constexpr auto kTable = TableType::Warehouse;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 64 << 9, true); //inlined was false
  using IndexBackend = felis::HashtableIndex;
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

  static std::atomic_ulong *g_last_no_start[32]; // max 32 nodes
  static std::atomic_ulong *g_last_no_end[32];

 public:
  static uint64_t *g_pwv_stock_resources;
  static felis::TableManager &tables() { return util::Instance<felis::TableManager>(); }

 public:
  template <typename TableType, typename KeyType>
  static void OnNewRow(int slice_id, TableType table, const KeyType &k,
                       felis::VHandle *handle) {
    if (!NodeConfiguration::g_data_migration)
      return;
    util::Instance<felis::SliceManager>().OnNewRow(
        slice_id, static_cast<int>(table), k.Encode(), handle);
  }

  static void OnUpdateRow(felis::VHandle *handle) {
    if (!NodeConfiguration::g_data_migration) return;
    util::Instance<felis::SliceManager>().OnUpdateRow(handle);
  }
  ClientBase(const util::FastRandom &r, const int node_id, const int nr_nodes);
  static bool is_warehouse_hotspot(uint wid);

  template <class T> T GenerateTransactionInput();

  static uint32_t AcquireLastNewOrderId(int warehouse, int district);
  // TODO: We need the following for random sharding.
  // static void AddLastNewOrderid(int warehouse, int district, uint32_t id);
};

// loaders for each table
namespace loaders {

enum LoaderType { Warehouse, Item, Stock, District, Customer, Order };

class BaseLoader : public tpcc::ClientBase {
 public:

  // Don't need?
#if 0
  static void SetAllocAffinity(int aff) {
    mem::ParallelPool::SetCurrentAffinity(aff);
  }
  static void RestoreAllocAffinity() {
    mem::ParallelPool::SetCurrentAffinity(-1);
  }
#endif

  using tpcc::ClientBase::ClientBase;
};

template <enum LoaderType TLN>
class Loader : public go::Routine, public BaseLoader {
  std::atomic_int *count_down;

 public:
  Loader(unsigned long seed, std::atomic_int *c)
      : count_down(c),
        BaseLoader(util::FastRandom(seed),
                   Instance<NodeConfiguration>().node_id(),
                   Instance<NodeConfiguration>().nr_nodes()) {}

  void DoLoad();
  virtual void Run() {
    DoLoad();
    count_down->fetch_sub(1);
  }

  template <typename TableType, typename F>
  void DoOnSlice(const typename TableType::Key &k, F func) {
    auto slice_id = util::Instance<SliceLocator<TableType>>().Locate(k);
    auto core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    if (TpccSliceRouter::SliceToNodeId(slice_id) == node_id
        && TpccSliceRouter::SliceToCoreId(slice_id) == core_id) {
      func(slice_id, core_id);
    }
  }
};

} // namespace loaders

enum class TxnType : int {
  NewOrder,
  Payment,
  Delivery,
  OrderStatus,
  StockLevel,

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

SHARD_TABLE(Customer) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.c_w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId(key);
  }
}
/*
SHARD_TABLE(CustomerNameIdx) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.c_w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId(key);
  }
}
*/

SHARD_TABLE(District) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.d_w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId(key);
  }
}

SHARD_TABLE(History) { std::abort(); } // we don't use this.
SHARD_TABLE(NewOrder) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.no_w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId(key);
  }
}

SHARD_TABLE(OOrder) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.o_w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId(key);
  }
}
SHARD_TABLE(OOrderCIdIdx) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.o_w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId(key);
  }
}
SHARD_TABLE(OrderLine) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.ol_w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId<OrderLine::Key, 4>(key);
  }
}
READ_ONLY_TABLE(Item);
SHARD_TABLE(Stock) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.s_w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId(key);
  }
}
/*
SHARD_TABLE(StockData) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.s_w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId(key);
  }
}
*/
SHARD_TABLE(Warehouse) {
  if (g_tpcc_config.shard_by_warehouse) {
    return g_tpcc_config.WarehouseToSliceId(key.w_id);
  } else {
    return g_tpcc_config.HashKeyToSliceId(key);
  }
}

} // namespace felis

#endif /* TPCC_H */
