#include "table_decl.h"

#include <string>
#include <cstdlib>
#include <unistd.h>

#include <sys/time.h>
#include <sys/types.h>

#include <set>
#include <vector>
#include <memory>
#include <string>

#include "util.h"
#include "node_config.h"
#include "console.h"
#include "tpcc.h"

using felis::Relation;
using felis::RelationManager;
using felis::NodeConfiguration;
using felis::Console;
using felis::VHandle;
using util::Instance;

namespace tpcc {

Config::Config()
{
  uniform_item_distribution = false;
  nr_items = 100000;
  nr_warehouses = 16;
  districts_per_warehouse = 10;
  customers_per_district = 3000;

  hotspot_warehouse_bitmap = 0;

  hotspot_load_percentage = 500;
  max_supported_warehouse = 64;
}

Config kTPCCConfig;

// install the TPC-C slices into the global SliceManager
void InitializeSliceManager()
{
  auto &manager = Instance<felis::SliceManager>();
  auto &conf = Instance<NodeConfiguration>();

  manager.Initialize(kTPCCConfig.nr_warehouses);

  for (int i = 0; i < kTPCCConfig.nr_warehouses; i++) {
    int wh = i + 1;

    // You, as node_id, should not touch these slices at all!
    if (ClientBase::warehouse_to_node_id(wh) != conf.node_id()) {
      continue;
    }

    felis::RowShipment *row_shipment = nullptr;
    if (ClientBase::is_warehouse_hotspot(wh)) {
      if (NodeConfiguration::g_data_migration) {
        auto &row_peer = conf.config(kTPCCConfig.offload_nodes[0]).row_shipper_peer;
        row_shipment = new felis::RowShipment(row_peer.host, row_peer.port, true);
      }
    }

    // if shipment is nullptr, the scanner won't scan when called ObjectSliceScanner::Scan()
    manager.InstallRowSlice(i, row_shipment);
    if (row_shipment)
      logger->info("Installed row shipment, slice id {}", i);
  }
}

// load TPC-C related configs from module "console" to kTPCCConfig
void InitializeTPCC()
{
  auto conf = Instance<Console>().FindConfigSection("tpcc").object_items();
  auto it = conf.find("hot_warehouses");
  if (it != conf.end()) {
    for (auto v: it->second.array_items()) {
      int hotspot_warehouse = v.int_value();
      if (hotspot_warehouse > kTPCCConfig.max_supported_warehouse)
        continue;
      kTPCCConfig.hotspot_warehouse_bitmap |= (1 << (hotspot_warehouse - 1));
    }
    logger->info("Hot Warehouses are {} (bitmap)", (void *) kTPCCConfig.hotspot_warehouse_bitmap);
  }

  it = conf.find("offload_nodes");
  if (it != conf.end()) {
    for (auto v: it->second.array_items()) {
      int node = v.int_value();
      if (node >= NodeConfiguration::kMaxNrNode)
        continue;
      kTPCCConfig.offload_nodes.push_back(node);
    }
  }

  logger->info("data migration mode {}", NodeConfiguration::g_data_migration);

  auto &mgr = Instance<RelationManager>();
  for (int table = 0; table < int(TableType::NRTable); table++) {
    std::string name = kTPCCTableNames[static_cast<int>(table)];
    mgr.GetRelationOrCreate(int(table));
  }
  logger->info("TPCC Table schemas created");

}

// TPC-C workload mix
// 0: NewOrder
// 1: Payment
// 2: CreditCheck
// 3: Delivery
// 4: OrderStatus
// 5: StockLevel
// 6: Microbenchmark - others will be set to 0 if g_microbench is set
// 7: Microbenchmark-simple - just do one insert, get, and put
// 8: Microbenchmark-random - same as Microbenchmark, but uses random read-set range

// unsigned g_txn_workload_mix[] = {41, 43, 4, 4, 4, 4, 0, 0, 0}; // default TPC-C workload mix
//static unsigned g_txn_workload_mix[] = {45, 43, 4, 4, 4, 0, 0, 0};
//static unsigned g_txn_workload_mix[] = {0, 100, 0, 0, 0, 0, 0, 0};

static int NMaxCustomerIdxScanElems = 512;

ClientBase::ClientBase(const util::FastRandom &r, const int node_id, const int nr_nodes)
    : r(r)
{
  this->node_id = node_id;

  min_warehouse = kTPCCConfig.nr_warehouses * (node_id - 1) / nr_nodes + 1;
  max_warehouse = kTPCCConfig.nr_warehouses * node_id / nr_nodes;


  auto cap =
      (max_warehouse - min_warehouse + 1) * kTPCCConfig.districts_per_warehouse;

  new_order_id_counters.reset(new ulong[cap]);
  memset(new_order_id_counters.get(), 0, sizeof(ulong) * cap);
}

// utils for generating random #s and strings
int ClientBase::CheckBetweenInclusive(int v, int lower, int upper)
{
  assert(v >= lower);
  assert(v <= upper);
  return v;
}

int ClientBase::RandomNumber(int min, int max)
{
  return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
}

int ClientBase::NonUniformRandom(int A, int C, int min, int max)
{
  return (((RandomNumber(0, A) | RandomNumber(min, max)) + C) % (max - min + 1)) + min;
}

int ClientBase::GetOrderLinesPerCustomer()
{
  return RandomNumber(5, 15);
}

int ClientBase::GetItemId()
{
  int id = 0;
  if (kTPCCConfig.uniform_item_distribution) {
    id = RandomNumber(1, kTPCCConfig.nr_items);
  } else {
    id = NonUniformRandom(8191, 7911, 1, (int) kTPCCConfig.nr_items);
  }
  return CheckBetweenInclusive(id, 1, kTPCCConfig.nr_items);
}

int ClientBase::GetCustomerId()
{
  return CheckBetweenInclusive(NonUniformRandom(1023, 259, 1, kTPCCConfig.customers_per_district),
                               1, kTPCCConfig.customers_per_district);
}

size_t ClientBase::GetCustomerLastName(uint8_t *buf, int num)
{
  static std::string NameTokens[] = {
    std::string("BAR"),
    std::string("OUGHT"),
    std::string("ABLE"),
    std::string("PRI"),
    std::string("PRES"),
    std::string("ESE"),
    std::string("ANTI"),
    std::string("CALLY"),
    std::string("ATION"),
    std::string("EING"),
  };

  const std::string &s0 = NameTokens[num / 100];
  const std::string &s1 = NameTokens[(num / 10) % 10];
  const std::string &s2 = NameTokens[num % 10];
  uint8_t *const begin = buf;
  const size_t s0_sz = s0.size();
  const size_t s1_sz = s1.size();
  const size_t s2_sz = s2.size();
  memcpy(buf, s0.data(), s0_sz); buf += s0_sz;
  memcpy(buf, s1.data(), s1_sz); buf += s1_sz;
  memcpy(buf, s2.data(), s2_sz); buf += s2_sz;
  return buf - begin;
}

std::string ClientBase::RandomStr(uint len)
{
  if (len == 0)
    return "";

  uint i = 0;
  std::string buf(len - 1, 0);
  while (i < (len - 1)) {
    const char c = (char) r.next_char();
    if (!isalnum(c))
      continue;
    buf[i++] = c;
  }
  return buf;
}

std::string ClientBase::RandomNStr(uint len)
{
  const char base = '0';
  std::string buf(len, 0);
  for (uint i = 0; i < len; i++)
    buf[i] = (char)(base + (r.next() % 10));
  return buf;
}

size_t ClientBase::nr_warehouses() const
{
  return kTPCCConfig.nr_warehouses;
}

uint ClientBase::PickWarehouse()
{
  if (kWarehouseSpread == 0 || r.next_uniform() >= kWarehouseSpread) {
    // Some warehouses are the hotspot, we need to extend such range for random
    // number generation.
    ulong rand_max = 0;
    for (auto w = min_warehouse; w <= max_warehouse; w++) {
      if (ClientBase::is_warehouse_hotspot(w)) rand_max += kTPCCConfig.hotspot_load_percentage;
      else rand_max += 100;
    }
    auto rand = long(r.next_uniform() * rand_max);
    for (auto w = min_warehouse; w <= max_warehouse; w++) {
      if (ClientBase::is_warehouse_hotspot(w)) rand -= kTPCCConfig.hotspot_load_percentage;
      else rand -= 100;
      if (rand < 0)
        return w;
    }
    return max_warehouse;
  }
  return r.next() % kTPCCConfig.nr_warehouses + 1;
}

uint ClientBase::LoadPercentageByWarehouse()
{
  uint load = 0;
  for (int w = min_warehouse; w <= max_warehouse; w++) {
    if (ClientBase::is_warehouse_hotspot(w)) load += kTPCCConfig.hotspot_load_percentage;
    else load += 100;
  }
  return load / (max_warehouse - min_warehouse + 1);
}

uint ClientBase::PickDistrict()
{
  return RandomNumber(1, kTPCCConfig.districts_per_warehouse);
}

ulong ClientBase::PickNewOrderId(uint warehouse_id, uint district_id)
{
  uint idx = (warehouse_id - min_warehouse) * kTPCCConfig.districts_per_warehouse + district_id - 1;
  return ++new_order_id_counters[idx];
}

uint ClientBase::GetCurrentTime()
{
  static __thread uint tl_hack = 0;
  return ++tl_hack;
}

struct Checker {
  // these sanity checks are just a few simple checks to make sure
  // the data is not entirely corrupted

  static inline  void
  SanityCheckCustomer(const Customer::Key *k, const Customer::Value *v) {
    assert(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= kTPCCConfig.nr_warehouses);
    assert(k->c_d_id >= 1 && static_cast<size_t>(k->c_d_id) <= kTPCCConfig.districts_per_warehouse);
    assert(k->c_id >= 1 && static_cast<size_t>(k->c_id) <= kTPCCConfig.customers_per_district);
    assert(v->c_credit == "BC" || v->c_credit == "GC");
    assert(v->c_middle == "OE");
  }

  static inline  void
  SanityCheckWarehouse(const Warehouse::Key *k, const Warehouse::Value *v) {
    assert(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= kTPCCConfig.nr_warehouses);
    assert(v->w_state.size() == 2);
    assert(v->w_zip == "123456789");
  }

  static inline  void
  SanityCheckDistrict(const District::Key *k, const District::Value *v) {
    assert(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= kTPCCConfig.nr_warehouses);
    assert(k->d_id >= 1 && static_cast<size_t>(k->d_id) <= kTPCCConfig.districts_per_warehouse);
    assert(v->d_next_o_id >= 3001);
    assert(v->d_state.size() == 2);
    assert(v->d_zip == "123456789");
  }

  static inline  void
  SanityCheckItem(const Item::Key *k, const Item::Value *v) {
    assert(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= kTPCCConfig.nr_items);
    assert(v->i_price >= 100 && v->i_price <= 10000);
  }

  static inline  void
  SanityCheckStock(const Stock::Key *k, const Stock::Value *v) {
    assert(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= kTPCCConfig.nr_warehouses);
    assert(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= kTPCCConfig.nr_items);
  }

  static inline  void
  SanityCheckNewOrder(const NewOrder::Key *k, const NewOrder::Value *v) {
    assert(k->no_w_id >= 1 && static_cast<size_t>(k->no_w_id) <= kTPCCConfig.nr_warehouses);
    assert(k->no_d_id >= 1 && static_cast<size_t>(k->no_d_id) <= kTPCCConfig.districts_per_warehouse);
  }

  static inline  void
   SanityCheckOOrder(const OOrder::Key *k, const OOrder::Value *v) {
    assert(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= kTPCCConfig.nr_warehouses);
    assert(k->o_d_id >= 1 && static_cast<size_t>(k->o_d_id) <= kTPCCConfig.districts_per_warehouse);
    assert(v->o_c_id >= 1 && static_cast<size_t>(v->o_c_id) <= kTPCCConfig.customers_per_district);
    assert(v->o_carrier_id >= 0 && static_cast<size_t>(v->o_carrier_id) <= kTPCCConfig.districts_per_warehouse);
    assert(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
  }

  static inline void
  SanityCheckOrderLine(const OrderLine::Key *k, const OrderLine::Value *v) {
    assert(k->ol_w_id >= 1 && static_cast<size_t>(k->ol_w_id) <= kTPCCConfig.nr_warehouses);
    assert(k->ol_d_id >= 1 && static_cast<size_t>(k->ol_d_id) <= kTPCCConfig.districts_per_warehouse);
    assert(k->ol_number >= 1 && k->ol_number <= 15);
    assert(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= kTPCCConfig.nr_items);
  }

};

felis::Relation &ClientBase::relation(TableType table)
{
  return Instance<RelationManager>()[static_cast<int>(table)];
}

int ClientBase::warehouse_to_node_id(unsigned int wid)
{
  // partition id also starts from 1 because 0 means local shard. See node_config.cc
  return (wid - 1) * Instance<NodeConfiguration>().nr_nodes() / kTPCCConfig.nr_warehouses + 1;
}

bool ClientBase::is_warehouse_hotspot(uint wid)
{
  return (kTPCCConfig.hotspot_warehouse_bitmap & (1 << (wid - 1))) != 0;
}

namespace loaders {

void BaseLoader::SetAllocAffinity(int w)
{
  auto aff = (w - min_warehouse) % NodeConfiguration::g_nr_threads;
  mem::ParallelPool::SetCurrentAffinity(aff);
}

template <>
void Loader<LoaderType::Warehouse>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint i = min_warehouse; i <= max_warehouse; i++) {

    // TODO: if multiple CPUs are sharing the same warehouse? This configuration
    // still make some sense, except it's not well balanced.

    util::PinToCPU(i - min_warehouse + NodeConfiguration::g_core_shifting);
    SetAllocAffinity(i);

    auto k = Warehouse::Key::New(i);
    auto v = Warehouse::Value();

    auto w_name = RandomStr(RandomNumber(6, 10));
    auto w_street_1 = RandomStr(RandomNumber(10, 20));
    auto w_street_2 = RandomStr(RandomNumber(10, 20));
    auto w_city = RandomStr(RandomNumber(10, 20));
    auto w_state = RandomStr(3);
    auto w_zip = std::string("123456789");

    v.w_ytd = 30000000;
    v.w_tax = RandomNumber(0, 2000) / 100;
    v.w_name.assign(w_name);
    v.w_street_1.assign(w_street_1);
    v.w_street_2.assign(w_street_2);
    v.w_city.assign(w_city);
    v.w_state.assign(w_state);
    v.w_zip.assign(w_zip);

    Checker::SanityCheckWarehouse(&k, &v);

    auto handle = relation(TableType::Warehouse).SearchOrCreate(k.EncodeFromAlloca(large_buf));
    auto slice_id = util::Instance<felis::SliceLocator<tpcc::Warehouse>>().Locate(k);
    OnNewRow(slice_id, TableType::Warehouse, k, handle);
    felis::InitVersion(handle, v.Encode());
  }

  util::PinToCPU(go::Scheduler::CurrentThreadPoolId() - 1 + NodeConfiguration::g_core_shifting);
  RestoreAllocAffinity();

  relation(TableType::Warehouse).set_key_length(sizeof(Warehouse::Key));
  logger->info("Warehouse Table loading done.");
}

template <>
void Loader<LoaderType::Item>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint i = 1; i <= kTPCCConfig.nr_items; i++) {
    // items don't "belong" to a certain warehouse, so no pinning
    // TODO: can we also replicate its entire index too?
    auto k = Item::Key::New(i);
    auto v = Item::Value();

    auto i_name = RandomStr(RandomNumber(14, 24));
    v.i_name.assign(i_name);
    v.i_price = RandomNumber(100, 10000);

    const int len = RandomNumber(26, 50);
    if (RandomNumber(1, 100) > 10) {
      const std::string i_data = RandomStr(len);
      v.i_data.assign(i_data);
    } else {
      const int startOriginal = RandomNumber(2, (len - 8));
      const std::string i_data = RandomStr(startOriginal + 1) + "ORIGINAL"
        + RandomStr(len - startOriginal - 7);
      v.i_data.assign(i_data);
    }
    v.i_im_id = RandomNumber(1, 10000);

    Checker::SanityCheckItem(&k, &v);
    auto handle = relation(TableType::Item).SearchOrCreate(k.EncodeFromAlloca(large_buf));
    // no OnNewRow() here, we don't ship Item table
    felis::InitVersion(handle, v.Encode());
  }
  relation(TableType::Item).set_key_length(sizeof(Item::Key));
  logger->info("Item Table loading done.");
}

template <>
void Loader<LoaderType::Stock>::DoLoad()
{
  void *large_buf = alloca(1024);
  VHandle *handle = nullptr;
  for (uint w = min_warehouse; w <= max_warehouse; w++) {
    util::PinToCPU(w - min_warehouse + NodeConfiguration::g_core_shifting);
    SetAllocAffinity(w);

    for(size_t i = 1; i <= kTPCCConfig.nr_items; i++) {
      const auto k = Stock::Key::New(w, i);
      const auto k_data =  StockData::Key::New(w, i);

      Stock::Value v;
      v.s_quantity = RandomNumber(10, 100);
      v.s_ytd = 0;
      v.s_order_cnt = 0;
      v.s_remote_cnt = 0;

      StockData::Value v_data;
      const int len = RandomNumber(26, 50);
      if (RandomNumber(1, 100) > 10) {
        const std::string s_data = RandomStr(len);
        v_data.s_data.assign(s_data);
      } else {
        const int startOriginal = RandomNumber(2, (len - 8));
        const std::string s_data = RandomStr(startOriginal + 1) + "ORIGINAL"
          + RandomStr(len - startOriginal - 7);
        v_data.s_data.assign(s_data);
      }
      v_data.s_dist_01.assign(RandomStr(24));
      v_data.s_dist_02.assign(RandomStr(24));
      v_data.s_dist_03.assign(RandomStr(24));
      v_data.s_dist_04.assign(RandomStr(24));
      v_data.s_dist_05.assign(RandomStr(24));
      v_data.s_dist_06.assign(RandomStr(24));
      v_data.s_dist_07.assign(RandomStr(24));
      v_data.s_dist_08.assign(RandomStr(24));
      v_data.s_dist_09.assign(RandomStr(24));
      v_data.s_dist_10.assign(RandomStr(24));

      Checker::SanityCheckStock(&k, &v);

      handle = relation(TableType::Stock).SearchOrCreate(k.EncodeFromAlloca(large_buf));
      auto slice_id = util::Instance<felis::SliceLocator<tpcc::Stock>>().Locate(k);
      OnNewRow(slice_id, TableType::Stock, k, handle);
      felis::InitVersion(handle, v.Encode());

      handle = relation(TableType::StockData).SearchOrCreate(k_data.EncodeFromAlloca(large_buf));
      slice_id = util::Instance<felis::SliceLocator<tpcc::StockData>>().Locate(k_data);
      OnNewRow(slice_id, TableType::StockData, k_data, handle);
      felis::InitVersion(handle, v_data.Encode());
    }
  }
  relation(TableType::Stock).set_key_length(sizeof(Stock::Key));
  relation(TableType::StockData).set_key_length(sizeof(StockData::Key));

  util::PinToCPU(go::Scheduler::CurrentThreadPoolId() - 1 + NodeConfiguration::g_core_shifting);
  RestoreAllocAffinity();

  logger->info("Stock Table loading done.");
}

template <>
void Loader<LoaderType::District>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint w = min_warehouse; w <= max_warehouse; w++) {
    util::PinToCPU(w - min_warehouse + NodeConfiguration::g_core_shifting);
    SetAllocAffinity(w);

    for (uint d = 1; d <= kTPCCConfig.districts_per_warehouse; d++) {
      const auto k = District::Key::New(w, d);
      District::Value v;
      v.d_ytd = 3000000;
      v.d_tax = RandomNumber(0, 2000) / 100;
      v.d_next_o_id = 3001;
      v.d_name.assign(RandomStr(RandomNumber(6, 10)));
      v.d_street_1.assign(RandomStr(RandomNumber(10, 20)));
      v.d_street_2.assign(RandomStr(RandomNumber(10, 20)));
      v.d_city.assign(RandomStr(RandomNumber(10, 20)));
      v.d_state.assign(RandomStr(3));
      v.d_zip.assign("123456789");

      Checker::SanityCheckDistrict(&k, &v);

      auto handle = relation(TableType::District).SearchOrCreate(k.EncodeFromAlloca(large_buf));
      auto slice_id = util::Instance<felis::SliceLocator<tpcc::District>>().Locate(k);
      OnNewRow(slice_id, TableType::District, k, handle);
      felis::InitVersion(handle, v.Encode());
    }
  }
  relation(TableType::District).set_key_length(sizeof(District::Key));
  util::PinToCPU(go::Scheduler::CurrentThreadPoolId() - 1 + NodeConfiguration::g_core_shifting);
  RestoreAllocAffinity();

  logger->info("District Table loading done.");
}

template <>
void Loader<LoaderType::Customer>::DoLoad()
{
  void *large_buf = alloca(1024);
  VHandle *handle = nullptr;

  for (uint w = min_warehouse; w <= max_warehouse; w++) {
    util::PinToCPU(w - min_warehouse + NodeConfiguration::g_core_shifting);
    SetAllocAffinity(w);

    for (uint d = 1; d <= kTPCCConfig.districts_per_warehouse; d++) {
      for (uint cidx0 = 0; cidx0 < kTPCCConfig.customers_per_district; cidx0++) {
        const uint c = cidx0 + 1;
        auto k = Customer::Key::New(w, d, c);
        Customer::Value v;

        v.c_discount = RandomNumber(1, 5000) / 100;
        if (RandomNumber(1, 100) <= 10)
          v.c_credit.assign("BC");
        else
          v.c_credit.assign("GC");

        if (c <= 1000)
          v.c_last.assign(GetCustomerLastName(c - 1));
        else
          v.c_last.assign(GetNonUniformCustomerLastNameLoad());

        v.c_first.assign(RandomStr(RandomNumber(8, 16)));
        v.c_credit_lim = 50000;

        v.c_balance = -1000;
        v.c_ytd_payment = 1000;
        v.c_payment_cnt = 1;
        v.c_delivery_cnt = 0;

        v.c_street_1.assign(RandomStr(RandomNumber(10, 20)));
        v.c_street_2.assign(RandomStr(RandomNumber(10, 20)));
        v.c_city.assign(RandomStr(RandomNumber(10, 20)));
        v.c_state.assign(RandomStr(3));
        v.c_zip.assign(RandomNStr(4) + "11111");
        v.c_phone.assign(RandomNStr(16));
        v.c_since = GetCurrentTime();
        v.c_middle.assign("OE");
        v.c_data.assign(RandomStr(RandomNumber(300, 500)));

        Checker::SanityCheckCustomer(&k, &v);
        handle = relation(TableType::Customer).SearchOrCreate(k.EncodeFromAlloca(large_buf));
        auto slice_id = util::Instance<felis::SliceLocator<tpcc::Customer>>().Locate(k);
        OnNewRow(slice_id, TableType::Customer, k, handle);
        felis::InitVersion(handle, v.Encode());

        // customer name index
        auto k_idx = CustomerNameIdx::Key::New(k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));
        auto v_idx = CustomerNameIdx::Value::New(k.c_id);

        // index structure is:
        // (c_w_id, c_d_id, c_last, c_first) -> (c_id)

        handle = relation(TableType::CustomerNameIdx).SearchOrCreate(k_idx.EncodeFromAlloca(large_buf));
        slice_id = util::Instance<felis::SliceLocator<tpcc::CustomerNameIdx>>().Locate(k_idx);
        OnNewRow(slice_id, TableType::CustomerNameIdx, k_idx, handle);
        felis::InitVersion(handle, v_idx.Encode());

        History::Key k_hist;

        k_hist.h_c_id = c;
        k_hist.h_c_d_id = d;
        k_hist.h_c_w_id = w;
        k_hist.h_d_id = d;
        k_hist.h_w_id = w;
        k_hist.h_date = GetCurrentTime();

        History::Value v_hist;
        v_hist.h_amount = 1000;
        v_hist.h_data.assign(RandomStr(RandomNumber(10, 24)));

        handle = relation(TableType::History).SearchOrCreate(k_hist.EncodeFromAlloca(large_buf));
        slice_id = util::Instance<felis::SliceLocator<tpcc::History>>().Locate(k_hist);
        OnNewRow(slice_id, TableType::History, k_hist, handle);
        felis::InitVersion(handle, v_hist.Encode());

      }
    }
  }

  relation(TableType::Customer).set_key_length(sizeof(Customer::Key));
  relation(TableType::CustomerNameIdx).set_key_length(sizeof(CustomerNameIdx::Key));
  relation(TableType::History).set_key_length(sizeof(History::Key));
  logger->info("Customer Table loading done.");
  util::PinToCPU(go::Scheduler::CurrentThreadPoolId() - 1 + NodeConfiguration::g_core_shifting);
  RestoreAllocAffinity();
}

template <>
void Loader<LoaderType::Order>::DoLoad()
{
  void *large_buf = alloca(1024);
  VHandle *handle = nullptr;

  for (uint w = min_warehouse; w <= max_warehouse; w++) {
    util::PinToCPU(w - min_warehouse + NodeConfiguration::g_core_shifting);
    SetAllocAffinity(w);

    for (uint d = 1; d <= kTPCCConfig.districts_per_warehouse; d++) {
      std::set<uint> c_ids_s;
      std::vector<uint> c_ids;

      while (c_ids.size() != kTPCCConfig.customers_per_district) {
        const auto x = (r.next() % kTPCCConfig.customers_per_district) + 1;
        if (c_ids_s.count(x))
          continue;
        c_ids_s.insert(x);
        c_ids.emplace_back(x);
      }

      auto auto_inc_zone = w * 10 + d;

      for (uint c = 1; c <= kTPCCConfig.customers_per_district; c++) {
        const auto k_oo = OOrder::Key::New(
            w, d, relation(TableType::OOrder).AutoIncrement(auto_inc_zone));

        OOrder::Value v_oo;

        v_oo.o_c_id = c_ids[c - 1];
        if ((k_oo.o_id >> 8) < 2101)
          v_oo.o_carrier_id = RandomNumber(1, 10);
        else
          v_oo.o_carrier_id = 0;
        v_oo.o_ol_cnt = GetOrderLinesPerCustomer();
        v_oo.o_all_local = 1;
        v_oo.o_entry_d = GetCurrentTime();

        Checker::SanityCheckOOrder(&k_oo, &v_oo);

        handle = relation(TableType::OOrder).SearchOrCreate(k_oo.EncodeFromAlloca(large_buf));
        auto slice_id = util::Instance<felis::SliceLocator<tpcc::OOrder>>().Locate(k_oo);
        OnNewRow(slice_id, TableType::OOrder, k_oo, handle);
        felis::InitVersion(handle, v_oo.Encode());

        const auto k_oo_idx = OOrderCIdIdx::Key::New(k_oo.o_w_id, k_oo.o_d_id, v_oo.o_c_id, k_oo.o_id);
        const auto v_oo_idx = OOrderCIdIdx::Value::New(0);

        handle = relation(TableType::OOrderCIdIdx).SearchOrCreate(k_oo_idx.EncodeFromAlloca(large_buf));
        slice_id = util::Instance<felis::SliceLocator<tpcc::OOrderCIdIdx>>().Locate(k_oo_idx);
        OnNewRow(slice_id, TableType::OOrderCIdIdx, k_oo_idx, handle);
        felis::InitVersion(handle, v_oo_idx.Encode());

        if (c >= 2101) {
          auto k_no = NewOrder::Key::New(w, d, k_oo.o_id, v_oo.o_c_id);
          NewOrder::Value v_no;

          Checker::SanityCheckNewOrder(&k_no, &v_no);

          handle = relation(TableType::NewOrder).SearchOrCreate(k_no.EncodeFromAlloca(large_buf));
          slice_id = util::Instance<felis::SliceLocator<tpcc::NewOrder>>().Locate(k_no);
          OnNewRow(slice_id, TableType::NewOrder, k_no, handle);
          felis::InitVersion(handle, v_no.Encode());
        }

        for (uint l = 1; l <= uint(v_oo.o_ol_cnt); l++) {
          auto k_ol = OrderLine::Key::New(w, d, k_oo.o_id, l);
          OrderLine::Value v_ol;

          v_ol.ol_i_id = RandomNumber(1, 100000);
          if (k_ol.ol_o_id < 2101) {
            v_ol.ol_delivery_d = v_oo.o_entry_d;
            v_ol.ol_amount = 0;
          } else {
            v_ol.ol_delivery_d = 0;
            // random within [0.01 .. 9,999.99]
            v_ol.ol_amount = RandomNumber(1, 999999);
          }

          v_ol.ol_supply_w_id = k_ol.ol_w_id;
          v_ol.ol_quantity = 5;
          // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
          //v_ol.ol_dist_info = RandomStr(24);

          Checker::SanityCheckOrderLine(&k_ol, &v_ol);
          handle = relation(TableType::OrderLine).SearchOrCreate(k_ol.EncodeFromAlloca(large_buf));
          auto slice_id = util::Instance<felis::SliceLocator<tpcc::OrderLine>>().Locate(k_ol);
          OnNewRow(slice_id, TableType::OrderLine, k_ol, handle);
          felis::InitVersion(handle, v_ol.Encode());
        }
      }
    }
  }

  relation(TableType::OOrder).set_key_length(sizeof(OOrder::Key));
  relation(TableType::OOrderCIdIdx).set_key_length(sizeof(OOrderCIdIdx::Key));
  relation(TableType::NewOrder).set_key_length(sizeof(NewOrder::Key));
  relation(TableType::OrderLine).set_key_length(sizeof(OrderLine::Key));
  util::PinToCPU(go::Scheduler::CurrentThreadPoolId() - 1 + NodeConfiguration::g_core_shifting);
  RestoreAllocAffinity();

  logger->info("Order Table loading done.");
}

}

uint Client::warehouse_to_lookup_node_id(uint warehouse_id)
{
  auto &conf = Instance<NodeConfiguration>();
  if (disable_load_balance || !is_warehouse_hotspot(warehouse_id))
    return warehouse_to_node_id(warehouse_id);
  else
    return (dice++) % conf.nr_nodes() + 1;
}

static constexpr int kTPCCTxnMix[] = {
  45, 43, 4,
};

felis::BaseTxn *Client::CreateTxn(uint64_t serial_id)
{
  // TODO: generate standard TPC-C txn mix here. Currently, only NewOrder,
  // Delivery and Payment are available.
  int rd = r.next_u32() % 92;
  int txn_type_id = 0;
  while (true) {
    int threshold = kTPCCTxnMix[txn_type_id];
    if (rd < threshold)
      break;
    rd -= threshold;
    txn_type_id++;
  }
  return TxnFactory::Create(txn_type_id, this, serial_id);
}

}

// TODO:
#if 0
#define ROW_SLICE_MAPPING_DIRECT

namespace felis {

using namespace tpcc;

#ifdef ROW_SLICE_MAPPING_DIRECT
int SliceLocator<Customer>::Locate(const typename Customer::Key &key) {
  return key.c_w_id - 1;
}
int SliceLocator<CustomerNameIdx>::Locate(const typename CustomerNameIdx::Key &key) {
  return key.c_w_id - 1;
}
int SliceLocator<District>::Locate(const typename District::Key &key) {
  return key.d_w_id - 1;
}
int SliceLocator<History>::Locate(const typename History::Key &key) {
  // History currently follows order warehouse_id
  return key.h_w_id - 1;
}
int SliceLocator<NewOrder>::Locate(const typename NewOrder::Key &key) {
  return key.no_w_id - 1;
}
int SliceLocator<OOrder>::Locate(const typename OOrder::Key &key) {
  return key.o_w_id - 1;
}
int SliceLocator<OOrderCIdIdx>::Locate(const typename OOrderCIdIdx::Key &key) {
  return key.o_w_id - 1;
}
int SliceLocator<OrderLine>::Locate(const typename OrderLine::Key &key) {
  return key.ol_w_id - 1;
}
int SliceLocator<Stock>::Locate(const typename Stock::Key &key) {
  return key.s_w_id - 1;
}
int SliceLocator<StockData>::Locate(const typename StockData::Key &key) {
  return key.s_w_id - 1;
}
int SliceLocator<Warehouse>::Locate(const typename Warehouse::Key &key) {
  return key.w_id - 1;
}
#endif


#ifdef ROW_SLICE_MAPPING_2

#define DISTRICT_CUT 6
#define STOCK_CUT 50000
int SliceLocator<Customer>::Locate(const typename Customer::Key &key) {
  if (ClientBase::is_warehouse_hotspot(key.c_w_id)) {
    if (key.c_d_id < DISTRICT_CUT) {
      return key.c_w_id - 1;
    } else {
      return key.c_w_id;
    }
  }
  return key.c_w_id - 1;
}
int SliceLocator<CustomerNameIdx>::Locate(const typename CustomerNameIdx::Key &key) {
  if (ClientBase::is_warehouse_hotspot(key.c_w_id)) {
    if (key.c_d_id < DISTRICT_CUT) {
      return key.c_w_id - 1;
    } else {
      return key.c_w_id;
    }
  }
  return key.c_w_id - 1;
}
int SliceLocator<District>::Locate(const typename District::Key &key) {
  if (ClientBase::is_warehouse_hotspot(key.d_w_id)) {
    if (key.d_id < DISTRICT_CUT) {
      return key.d_w_id - 1;
    } else {
      return key.d_w_id;
    }
  }
  return key.d_w_id - 1;
}
int SliceLocator<History>::Locate(const typename History::Key &key) {
  // History currently follows order warehouse_id
  if (ClientBase::is_warehouse_hotspot(key.h_w_id)) {
    if (key.h_d_id < DISTRICT_CUT) {
      return key.h_w_id - 1;
    } else {
      return key.h_w_id;
    }
  }
  return key.h_w_id - 1;
}
int SliceLocator<NewOrder>::Locate(const typename NewOrder::Key &key) {
  if (ClientBase::is_warehouse_hotspot(key.no_w_id)) {
    if (key.no_d_id < DISTRICT_CUT) {
      return key.no_w_id - 1;
    } else {
      return key.no_w_id;
    }
  }
  return key.no_w_id - 1;
}
int SliceLocator<OOrder>::Locate(const typename OOrder::Key &key) {
  if (ClientBase::is_warehouse_hotspot(key.o_w_id)) {
    if (key.o_d_id < DISTRICT_CUT) {
      return key.o_w_id - 1;
    } else {
      return key.o_w_id;
    }
  }
  return key.o_w_id - 1;
}
int SliceLocator<OOrderCIdIdx>::Locate(const typename OOrderCIdIdx::Key &key) {
  if (ClientBase::is_warehouse_hotspot(key.o_w_id)) {
    if (key.o_d_id < DISTRICT_CUT) {
      return key.o_w_id - 1;
    } else {
      return key.o_w_id;
    }
  }
  return key.o_w_id - 1;
}
int SliceLocator<OrderLine>::Locate(const typename OrderLine::Key &key) {
  if (ClientBase::is_warehouse_hotspot(key.ol_w_id)) {
    if (key.ol_d_id < DISTRICT_CUT) {
      return key.ol_w_id - 1;
    } else {
      return key.ol_w_id;
    }
  }
  return key.ol_w_id - 1;
}
int SliceLocator<Stock>::Locate(const typename Stock::Key &key) {
  if (ClientBase::is_warehouse_hotspot(key.s_w_id)) {
    if (key.s_i_id < STOCK_CUT) {
      return key.s_w_id - 1;
    } else {
      return key.s_w_id;
    }
  }
  return key.s_w_id - 1;
}
int SliceLocator<StockData>::Locate(const typename StockData::Key &key) {
  if (ClientBase::is_warehouse_hotspot(key.s_w_id)) {
    if (key.s_i_id < STOCK_CUT) {
      return key.s_w_id - 1;
    } else {
      return key.s_w_id;
    }
  }
  return key.s_w_id - 1;
}
int SliceLocator<Warehouse>::Locate(const typename Warehouse::Key &key) {
  return key.w_id - 1;
}
}

#endif

#endif

namespace tpcc {

using namespace felis;

int SliceRouter(int16_t slice_id)
{
  auto &conf = util::Instance<NodeConfiguration>();
  auto warehouses_per_node = kTPCCConfig.nr_warehouses / conf.nr_nodes();
  return 1 + slice_id / warehouses_per_node; // Because node starts from 1
}

}
