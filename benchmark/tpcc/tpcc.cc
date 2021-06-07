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
#include "opts.h"

namespace tpcc {

using felis::Relation;
using felis::RelationManager;
using felis::Console;
using felis::VHandle;

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

  shard_by_warehouse = true;
}

Config g_tpcc_config;

static constexpr unsigned int kRandomShardingMaxSliceId = 101;

unsigned int Config::max_slice_id() const
{
  if (shard_by_warehouse)
    return nr_warehouses;
  else
    return kRandomShardingMaxSliceId;
}

// install the TPC-C slices into the global SliceManager
void InitializeSliceManager()
{
  auto &manager = Instance<felis::SliceManager>();
  auto &conf = Instance<NodeConfiguration>();

  manager.Initialize(g_tpcc_config.nr_warehouses);

  if (NodeConfiguration::g_data_migration) {
    for (int slc = 0; slc < g_tpcc_config.max_slice_id(); slc++) {
      // You, as node_id, should not touch these slices at all!
      if (TpccSliceRouter::SliceToNodeId(slc) != conf.node_id()) {
        continue;
      }

      felis::RowShipment *row_shipment = nullptr;

      // TODO: what if slc isn't a warehouse?
      abort_if(!g_tpcc_config.shard_by_warehouse,
               "Shipping without shard_by_warehouse is not implemented!");

      if (ClientBase::is_warehouse_hotspot(slc)) {
        auto &row_peer = conf.config(g_tpcc_config.offload_nodes[0]).row_shipper_peer;
        row_shipment = new felis::RowShipment(row_peer.host, row_peer.port, true);
      }

      // if shipment is nullptr, the scanner won't scan when called ObjectSliceScanner::Scan()
      manager.InstallRowSlice(slc, row_shipment);
      if (row_shipment)
        logger->info("Installed row shipment, slice id {}", slc);
    }
  }
}

// load TPC-C related configs from module "console" to kTPCCConfig
void InitializeTPCC()
{
  if (felis::Options::kTpccWarehouses)
    g_tpcc_config.nr_warehouses = felis::Options::kTpccWarehouses.ToInt();

  if (felis::Options::kTpccHotWarehouseBitmap)
    g_tpcc_config.hotspot_warehouse_bitmap = felis::Options::kTpccHotWarehouseBitmap.ToLargeNumber();

  if (felis::Options::kTpccHotWarehouseLoad)
    g_tpcc_config.hotspot_load_percentage = felis::Options::kTpccHotWarehouseLoad.ToInt();

  if (felis::Options::kTpccHashShard)
    g_tpcc_config.shard_by_warehouse = false;

  logger->info("Hot Warehouses are {:x} (bitmap), load {} %",
               g_tpcc_config.hotspot_warehouse_bitmap,
               g_tpcc_config.hotspot_load_percentage);

  /*
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
  */

  logger->info("data migration mode {}", NodeConfiguration::g_data_migration);

  auto &mgr = Instance<RelationManager>();
  for (int table = static_cast<int>(TableType::TPCCBase) + 1;
       table < static_cast<int>(TableType::NRTable);
       table++) {
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
    : r(r), node_id(node_id)
{}

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


int ClientBase::GetItemId()
{
  int id = 0;
  if (g_tpcc_config.uniform_item_distribution) {
    id = RandomNumber(1, g_tpcc_config.nr_items);
  } else {
    id = NonUniformRandom(8191, 7911, 1, (int) g_tpcc_config.nr_items);
  }
  return CheckBetweenInclusive(id, 1, g_tpcc_config.nr_items);
}

int ClientBase::GetCustomerId()
{
  return CheckBetweenInclusive(NonUniformRandom(1023, 259, 1, g_tpcc_config.customers_per_district),
                               1, g_tpcc_config.customers_per_district);
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
  return g_tpcc_config.nr_warehouses;
}

/*
 * 1. Randomly generate a warehouse key, using the hotspot configuration.
 * 2. Is this key belong to this node according to our sharding plan, unless Spread is On.
 */
uint ClientBase::PickWarehouse()
{
  long selw = -1;
  do {
    // Some warehouses are the hotspot, we need to extend such range for random
    // number generation.
    long rand_max = 0;
    for (auto w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
      if (ClientBase::is_warehouse_hotspot(w)) rand_max += g_tpcc_config.hotspot_load_percentage;
      else rand_max += 100;
    }

    auto rand = long(r.next_uniform() * rand_max);
    for (auto w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
      if (ClientBase::is_warehouse_hotspot(w)) rand -= g_tpcc_config.hotspot_load_percentage;
      else rand -= 100;

      if (rand < 0) {
        selw = w;
        break;
      }
    }

    if (kWarehouseSpread == 0 || r.next_uniform() >= kWarehouseSpread) {
      // Do we own this warehouse?
      auto wk = Warehouse::Key::New(selw);
      if (TpccSliceRouter::SliceToNodeId(Instance<SliceLocator<Warehouse>>().Locate(wk)) != node_id)
        selw = -1;
    }
  } while (selw == -1);
  return selw;
}

unsigned int ClientBase::LoadPercentageByWarehouse()
{
  if (!g_tpcc_config.shard_by_warehouse)
    return 100;
  unsigned int load = 0;
  unsigned int n = 0;
  for (int w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
    auto wk = Warehouse::Key::New(w);
    if (TpccSliceRouter::SliceToNodeId(Instance<SliceLocator<Warehouse>>().Locate(wk)) != node_id)
      continue;

    n++;
    if (ClientBase::is_warehouse_hotspot(w)) load += g_tpcc_config.hotspot_load_percentage;
    else load += 100;
  }
  return load / n;
}

uint ClientBase::PickDistrict()
{
  return RandomNumber(1, g_tpcc_config.districts_per_warehouse);
}

uint ClientBase::GetCurrentTime()
{
  static __thread uint32_t tl_hack = 100000;
  return ++tl_hack;
}

struct Checker {
  // these sanity checks are just a few simple checks to make sure
  // the data is not entirely corrupted

  static inline  void
  SanityCheckCustomer(const Customer::Key *k, const Customer::Value *v) {
    assert(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= g_tpcc_config.nr_warehouses);
    assert(k->c_d_id >= 1 && static_cast<size_t>(k->c_d_id) <= g_tpcc_config.districts_per_warehouse);
    assert(k->c_id >= 1 && static_cast<size_t>(k->c_id) <= g_tpcc_config.customers_per_district);
    assert(v->c_credit == "BC" || v->c_credit == "GC");
    assert(v->c_middle == "OE");
  }

  static inline  void
  SanityCheckWarehouse(const Warehouse::Key *k, const Warehouse::Value *v) {
    assert(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= g_tpcc_config.nr_warehouses);
    assert(v->w_state.size() == 2);
    assert(v->w_zip == "123456789");
  }

  static inline  void
  SanityCheckDistrict(const District::Key *k, const District::Value *v) {
    assert(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= g_tpcc_config.nr_warehouses);
    assert(k->d_id >= 1 && static_cast<size_t>(k->d_id) <= g_tpcc_config.districts_per_warehouse);
    assert(v->d_next_o_id >= 3001);
    assert(v->d_state.size() == 2);
    assert(v->d_zip == "123456789");
  }

  static inline  void
  SanityCheckItem(const Item::Key *k, const Item::Value *v) {
    assert(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= g_tpcc_config.nr_items);
    assert(v->i_price >= 100 && v->i_price <= 10000);
  }

  static inline  void
  SanityCheckStock(const Stock::Key *k, const Stock::Value *v) {
    assert(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= g_tpcc_config.nr_warehouses);
    assert(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= g_tpcc_config.nr_items);
  }

  static inline  void
  SanityCheckNewOrder(const NewOrder::Key *k, const NewOrder::Value *v) {
    assert(k->no_w_id >= 1 && static_cast<size_t>(k->no_w_id) <= g_tpcc_config.nr_warehouses);
    assert(k->no_d_id >= 1 && static_cast<size_t>(k->no_d_id) <= g_tpcc_config.districts_per_warehouse);
  }

  static inline  void
   SanityCheckOOrder(const OOrder::Key *k, const OOrder::Value *v) {
    assert(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= g_tpcc_config.nr_warehouses);
    assert(k->o_d_id >= 1 && static_cast<size_t>(k->o_d_id) <= g_tpcc_config.districts_per_warehouse);
    assert(v->o_c_id >= 1 && static_cast<size_t>(v->o_c_id) <= g_tpcc_config.customers_per_district);
    assert(v->o_carrier_id >= 0 && static_cast<size_t>(v->o_carrier_id) <= g_tpcc_config.districts_per_warehouse);
    assert(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
  }

  static inline void
  SanityCheckOrderLine(const OrderLine::Key *k, const OrderLine::Value *v) {
    assert(k->ol_w_id >= 1 && static_cast<size_t>(k->ol_w_id) <= g_tpcc_config.nr_warehouses);
    assert(k->ol_d_id >= 1 && static_cast<size_t>(k->ol_d_id) <= g_tpcc_config.districts_per_warehouse);
    assert(k->ol_number >= 1 && k->ol_number <= 15);
    assert(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= g_tpcc_config.nr_items);
  }

};

felis::Relation &ClientBase::relation(TableType table)
{
  return Instance<RelationManager>()[static_cast<int>(table)];
}

bool ClientBase::is_warehouse_hotspot(uint wid)
{
  return (g_tpcc_config.hotspot_warehouse_bitmap & (1 << (wid - 1))) != 0;
}

std::atomic_ulong *ClientBase::g_last_no_o_ids = nullptr;

namespace loaders {

template <>
void Loader<LoaderType::Warehouse>::DoLoad()
{
  void *large_buf = alloca(1024);
  logger->info("Warehouse Loader starts");
  for (uint i = 1; i <= g_tpcc_config.nr_warehouses; i++) {

    // TODO: if multiple CPUs are sharing the same warehouse? This configuration
    // still make some sense, except it's not well balanced.

    auto k = Warehouse::Key::New(i);

    DoOnSlice<tpcc::Warehouse>(
        k,
        [=](auto slice_id, auto core_id) {
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

          SetAllocAffinity(core_id);
          auto handle = relation(TableType::Warehouse).SearchOrCreate(k.EncodeFromAlloca(large_buf));
          OnNewRow(slice_id, TableType::Warehouse, k, handle);
          felis::InitVersion(handle, v.Encode());
        });
  }

  relation(TableType::Warehouse).set_key_length(sizeof(Warehouse::Key));
  logger->info("Warehouse Loader done.");
}

template <>
void Loader<LoaderType::Item>::DoLoad()
{
  // Item table is a read only table, we replicate this table on all nodes!
  int last_affinity = -1;
  void *large_buf = alloca(1024);
  logger->info("Item Loader starts");
  for (uint i = 1; i <= g_tpcc_config.nr_items; i++) {
    int affinity = (i - 1) * NodeConfiguration::g_nr_threads / g_tpcc_config.nr_items;
    if (affinity != last_affinity) {
      // Just trying to be load balanced
      SetAllocAffinity(affinity);
      last_affinity = affinity;
    }

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
  logger->info("Item Loader done.");
  RestoreAllocAffinity();
}

template <>
void Loader<LoaderType::Stock>::DoLoad()
{
  // Stock and StockData table
  void *large_buf = alloca(1024);
  std::string s_dist = RandomStr(24);
  logger->info("Stock Loader starts");
  for (uint w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
    logger->info("Stock Loader on warehouse {}", w);
    for(size_t i = 1; i <= g_tpcc_config.nr_items; i++) {
      const auto k = Stock::Key::New(w, i);
      const auto k_data =  StockData::Key::New(w, i);

      DoOnSlice<tpcc::Stock>(
          k,
          [=](auto slice_id, auto core_id) {
            Stock::Value v;
            v.s_quantity = RandomNumber(10, 100);
            v.s_ytd = 0;
            v.s_order_cnt = 0;
            v.s_remote_cnt = 0;

            Checker::SanityCheckStock(&k, &v);

            SetAllocAffinity(core_id);
            auto handle = relation(TableType::Stock).SearchOrCreate(k.EncodeFromAlloca(large_buf));

            OnNewRow(slice_id, TableType::Stock, k, handle);
            felis::InitVersion(handle, v.Encode());
          });

      DoOnSlice<tpcc::StockData>(
          k_data,
          [=](auto slice_id, auto core_id) {
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
            v_data.s_dist_01.assign(s_dist);
            v_data.s_dist_02.assign(s_dist);
            v_data.s_dist_03.assign(s_dist);
            v_data.s_dist_04.assign(s_dist);
            v_data.s_dist_05.assign(s_dist);
            v_data.s_dist_06.assign(s_dist);
            v_data.s_dist_07.assign(s_dist);
            v_data.s_dist_08.assign(s_dist);
            v_data.s_dist_09.assign(s_dist);
            v_data.s_dist_10.assign(s_dist);

            SetAllocAffinity(core_id);
            auto data_handle = relation(TableType::StockData).SearchOrCreate(k_data.EncodeFromAlloca(large_buf));

            OnNewRow(slice_id, TableType::StockData, k_data, data_handle);
            felis::InitVersion(data_handle, v_data.Encode());
          });
    }
  }
  relation(TableType::Stock).set_key_length(sizeof(Stock::Key));
  relation(TableType::StockData).set_key_length(sizeof(StockData::Key));
  logger->info("Stock Loader done.");
}

template <>
void Loader<LoaderType::District>::DoLoad()
{
  void *large_buf = alloca(1024);
  logger->info("District Loader starts");
  for (auto w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
    for (uint d = 1; d <= g_tpcc_config.districts_per_warehouse; d++) {
      const auto k = District::Key::New(w, d);

      DoOnSlice<tpcc::District>(
          k,
          [=](auto slice_id, auto core_id) {
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

            SetAllocAffinity(core_id);
            auto handle = relation(TableType::District).SearchOrCreate(k.EncodeFromAlloca(large_buf));

            OnNewRow(slice_id, TableType::District, k, handle);
            felis::InitVersion(handle, v.Encode());
          });

    }
  }
  relation(TableType::District).set_key_length(sizeof(District::Key));
  logger->info("District Loader done.");
}

template <>
void Loader<LoaderType::Customer>::DoLoad()
{
  void *large_buf = alloca(1024);
  logger->info("Customer Loader starts");
  for (auto w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
    for (auto d = 1; d <= g_tpcc_config.districts_per_warehouse; d++) {
      logger->info("Customer Loader on warehouse {}", w);
      for (auto cidx0 = 0; cidx0 < g_tpcc_config.customers_per_district; cidx0++) {
        const uint c = cidx0 + 1;
        auto k = Customer::Key::New(w, d, c);

        DoOnSlice<tpcc::Customer>(
            k,
            [=](auto slice_id, auto core_id) {
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

              SetAllocAffinity(core_id);
              auto handle = relation(TableType::Customer).SearchOrCreate(k.EncodeFromAlloca(large_buf));

              OnNewRow(slice_id, TableType::Customer, k, handle);
              felis::InitVersion(handle, v.Encode());
            });

        // I don't think we ever used this, I'll just disable this for now.
#if 0
        // customer name index
        auto k_idx = CustomerNameIdx::Key::New(k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));
        auto v_idx = CustomerNameIdx::Value::New(k.c_id);

        // index structure is:
        // (c_w_id, c_d_id, c_last, c_first) -> (c_id)

        handle = relation(TableType::CustomerNameIdx).SearchOrCreate(k_idx.EncodeFromAlloca(large_buf));
        slice_id = util::Instance<SliceLocator<tpcc::CustomerNameIdx>>().Locate(k_idx);
        OnNewRow(slice_id, TableType::CustomerNameIdx, k_idx, handle);
        felis::InitVersion(handle, v_idx.Encode());

        History::Key k_hist;

        k_hist.h_c_id = c;
        k_hist.h_c_d_id = d;
        k_hist.h_c_w_id = w;
        k_hist.h_d_id = d;
        k_hist.h_w_id = w;
        k_hist.h_date = GetCurrentTime();


        DoOnSlice<tpcc::History>(
            k_hist,
            [=](auto slice_id, auto core_id) {
              History::Value v_hist;
              v_hist.h_amount = 1000;
              v_hist.h_data.assign(RandomStr(RandomNumber(10, 24)));

              SetAllocAffinity(core_id);
              auto hist_handle = relation(TableType::History).SearchOrCreate(k_hist.EncodeFromAlloca(large_buf));

              OnNewRow(slice_id, TableType::History, k_hist, hist_handle);
              felis::InitVersion(hist_handle, v_hist.Encode());
            });
#endif
      }
    }
  }

  relation(TableType::Customer).set_key_length(sizeof(Customer::Key));
  relation(TableType::CustomerNameIdx).set_key_length(sizeof(CustomerNameIdx::Key));
  relation(TableType::History).set_key_length(sizeof(History::Key));
  logger->info("Customer Loader done.");
}

template <>
void Loader<LoaderType::Order>::DoLoad()
{
  void *large_buf = alloca(1024);
  // a random permutation of customer IDs
  auto c_ids = new uint32_t[g_tpcc_config.customers_per_district];

  logger->info("Order Loader starts.");
  for (auto w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
    logger->info("Order Loader on warehouse {}", w);
    for (auto d = 1; d <= g_tpcc_config.districts_per_warehouse; d++) {
      // Random shuffle
      for (auto c = 1; c <= g_tpcc_config.customers_per_district; c++) {
        auto j = RandomNumber(0, c - 1);
        if (j != c - 1)
          c_ids[c - 1] = c_ids[j];
        c_ids[j] = c;
      }

      for (auto c = 1; c <= g_tpcc_config.customers_per_district; c++) {
        auto ts_now = w * g_tpcc_config.customers_per_district * g_tpcc_config.districts_per_warehouse
                      + d * g_tpcc_config.customers_per_district + c;
        auto ol_cnt = ts_now % 11 + 5;
        auto order_id = c << 8;
        const auto k_oo = OOrder::Key::New(w, d, order_id);

        DoOnSlice<tpcc::OOrder>(
            k_oo,
            [=](auto slice_id, auto core_id) {
              OOrder::Value v_oo;

              v_oo.o_c_id = c_ids[c - 1];
              if ((k_oo.o_id >> 8) < 2101)
                v_oo.o_carrier_id = RandomNumber(1, 10);
              else
                v_oo.o_carrier_id = 0;

              // We cannot randomly generate this, because of consistency across partitions
              v_oo.o_ol_cnt = ol_cnt;
              v_oo.o_all_local = 1;
              v_oo.o_entry_d = ts_now;

              Checker::SanityCheckOOrder(&k_oo, &v_oo);

              SetAllocAffinity(core_id);
              auto oo_handle = relation(TableType::OOrder).SearchOrCreate(k_oo.EncodeFromAlloca(large_buf));
              OnNewRow(slice_id, TableType::OOrder, k_oo, oo_handle);
              felis::InitVersion(oo_handle, v_oo.Encode());
            });

        const auto k_oo_idx = OOrderCIdIdx::Key::New(k_oo.o_w_id, k_oo.o_d_id, c_ids[c - 1], k_oo.o_id);
        const auto v_oo_idx = OOrderCIdIdx::Value::New(0);

        DoOnSlice<tpcc::OOrderCIdIdx>(
            k_oo_idx,
            [=](auto slice_id, auto core_id) {
              SetAllocAffinity(core_id);
              auto oo_idx_handle = relation(TableType::OOrderCIdIdx).SearchOrCreate(k_oo_idx.EncodeFromAlloca(large_buf));
              OnNewRow(slice_id, TableType::OOrderCIdIdx, k_oo_idx, oo_idx_handle);
              felis::InitVersion(oo_idx_handle, v_oo_idx.Encode());
            });

        if (c >= 2101) {
          auto k_no = NewOrder::Key::New(w, d, k_oo.o_id, c_ids[c - 1]);
          NewOrder::Value v_no;
          Checker::SanityCheckNewOrder(&k_no, &v_no);
          DoOnSlice<tpcc::NewOrder>(
              k_no,
              [=](auto slice_id, auto core_id) {
                SetAllocAffinity(core_id);
                auto no_handle = relation(TableType::NewOrder).SearchOrCreate(k_no.EncodeFromAlloca(large_buf));
                OnNewRow(slice_id, TableType::NewOrder, k_no, no_handle);
                felis::InitVersion(no_handle, v_no.Encode());
              });
        }

        for (auto l = 1; l <= ol_cnt; l++) {
          auto k_ol = OrderLine::Key::New(w, d, k_oo.o_id, l);
          DoOnSlice<tpcc::OrderLine>(
              k_ol,
              [=](auto slice_id, auto core_id) {
                OrderLine::Value v_ol;

                v_ol.ol_i_id = RandomNumber(1, 100000);
                if (k_ol.ol_o_id < 2101) {
                  v_ol.ol_delivery_d = ts_now;
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

                SetAllocAffinity(core_id);
                auto ol_handle = relation(TableType::OrderLine).SearchOrCreate(k_ol.EncodeFromAlloca(large_buf));

                OnNewRow(slice_id, TableType::OrderLine, k_ol, ol_handle);
                felis::InitVersion(ol_handle, v_ol.Encode());
              });
        }
      }
      auto auto_inc_zone = w * 10 + d;
      relation(TableType::OOrder).ResetAutoIncrement(
          auto_inc_zone, g_tpcc_config.customers_per_district + 1);
    }
  }

  auto nr_all_districts = g_tpcc_config.nr_warehouses * g_tpcc_config.districts_per_warehouse;
  g_last_no_o_ids = new std::atomic_ulong[nr_all_districts];
  std::fill(g_last_no_o_ids, g_last_no_o_ids + nr_all_districts, 2101 << 8);

  delete [] c_ids;

  relation(TableType::OOrder).set_key_length(sizeof(OOrder::Key));
  relation(TableType::OOrderCIdIdx).set_key_length(sizeof(OOrderCIdIdx::Key));
  relation(TableType::NewOrder).set_key_length(sizeof(NewOrder::Key));
  relation(TableType::OrderLine).set_key_length(sizeof(OrderLine::Key));
  logger->info("Order Loader done.");
}

}

static constexpr int kTPCCTxnMix[] = {
  49, 47, 4,
};

static int kTPCCPriTxnBatchThres[] = {
  20, 100
}; // meaning mix = 20, 80

felis::BaseTxn *Client::CreateTxn(uint64_t serial_id)
{
  // TODO: generate standard TPC-C txn mix here. Currently, only NewOrder,
  // Delivery and Payment are available.
  auto x_pct = NodeConfiguration::g_priority_batch_mode_pct;
  int rd = r.next_u32() % (100 + x_pct);
  if (x_pct && rd >= 100) {
    rd = r.next_u32() % 100; // re-random

    if (rd < kTPCCPriTxnBatchThres[0])
      return TxnFactory::Create(int(TxnType::PriStock), this, serial_id);
    else
      return TxnFactory::Create(int(TxnType::PriNewOrderDelivery), this, serial_id);

  }
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

using namespace felis;

int TpccSliceRouter::SliceToNodeId(int16_t slice_id)
{
  auto &conf = util::Instance<NodeConfiguration>();
  return 1 + slice_id * conf.nr_nodes() / g_tpcc_config.max_slice_id(); // Because node starts from 1
}

int TpccSliceRouter::SliceToCoreId(int16_t slice_id)
{
  auto &conf = util::Instance<NodeConfiguration>();
  return (uint16_t) (slice_id * conf.nr_nodes() * conf.g_nr_threads / g_tpcc_config.max_slice_id()) % conf.g_nr_threads;
}

}
