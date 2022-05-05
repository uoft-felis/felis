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

#include "util/objects.h"
#include "util/random.h"
#include "node_config.h"
#include "console.h"
#include "tpcc.h"
#include "opts.h"

#include "felis_probes.h"

#include "new_order.h"
#include "payment.h"
#include "delivery.h"
#include "stock_level.h"
#include "order_status.h"

namespace tpcc {

using felis::Console;
using felis::VHandle;
using felis::IndexInfo;

Config::Config()
{
  uniform_item_distribution = false;
  nr_items = 100000;
  nr_warehouses = 256;
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

bool Config::IsWarehousePinnable()
{
  auto &conf = Instance<NodeConfiguration>();
  return nr_warehouses % (conf.nr_nodes() * conf.g_nr_threads) == 0;
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

  logger->info("Warehouses {}, Pin? {}",
               g_tpcc_config.nr_warehouses,
               g_tpcc_config.IsWarehousePinnable());

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

  auto &mgr = Instance<felis::TableManager>();
  mgr.Create<Customer, CustomerInfo, District, History, Item, NewOrder, OOrder, OOrderCIdIdx,
             OrderLine, Stock, Warehouse>();

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

uint32_t ClientBase::AcquireLastNewOrderId(int warehouse, int district)
{
  abort_if(!g_tpcc_config.shard_by_warehouse, "TODO: needs to implement this under random sharding.");
  auto node_id = util::Instance<NodeConfiguration>().node_id();
  auto idx = (warehouse - 1) * g_tpcc_config.districts_per_warehouse + district - 1;
  return (g_last_no_start[node_id][idx].fetch_add(1) << 8) | node_id;
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

bool ClientBase::is_warehouse_hotspot(uint wid)
{
  return (g_tpcc_config.hotspot_warehouse_bitmap & (1 << (wid - 1))) != 0;
}

std::atomic_ulong *ClientBase::g_last_no_start[32];
std::atomic_ulong *ClientBase::g_last_no_end[32];
uint64_t *ClientBase::g_pwv_stock_resources;

namespace loaders {

template <>
void Loader<LoaderType::Warehouse>::DoLoad()
{
  // shirley: recover vhandles if is recovery
  if (felis::Options::kRecovery) {
    void *large_buf = alloca(1024);
    int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    
    // uint64_t curr_ep_stale = util::Instance<EpochManager>().current_epoch_nr();
    uint64_t curr_ep = (mem::GetPmemPersistInfo()->largest_sid >> 32) + 1;
    // shirley: epoch manager haven't advanced yet.
    // printf("EpochManager ep = %lu, pmem ep = %lu\n", curr_ep_stale, curr_ep);
    
    mem::BrkWFree *vhandles_brk = felis::VHandle::inline_pool.get_pool(core_id);
    uint8_t *data = vhandles_brk->get_data();
    uint64_t *ring_buffer = vhandles_brk->get_ring_buffer(); 
    size_t data_offset = vhandles_brk->get_cached_offset();
    size_t initial_offset_freelist = vhandles_brk->get_cached_initial_offset_freelist();
    size_t initial_offset_pending_freelist = vhandles_brk->get_cached_initial_offset_pending_freelist();
    size_t data_block_size = vhandles_brk->get_cached_block_size();
    size_t lmt_rb = vhandles_brk->get_cached_limit_ring_buffer();

    // reset deleted vhandles
    // note: assuming we'll never delete too much into pending freelist that it loops over to freelist
    for (size_t i_off = initial_offset_freelist; i_off != initial_offset_pending_freelist; i_off++) {
      if (i_off == lmt_rb) {
        i_off = 0;
        if (i_off == initial_offset_pending_freelist) {
          // we've scanned tne entire freelist.
          break;
        }
      }
      std::memset((uint8_t *)(ring_buffer[i_off]), 0, 64);
      // shirley pmem shirley test
      _mm_clwb((uint64_t *)(ring_buffer[i_off]));
    }
    // shirley pmem shirley test
    _mm_sfence();

    // now read vhandles and rebuild index
    for (uint64_t i = 0; i < data_offset; i += data_block_size) {
      VHandle *vhdl_row = (VHandle *)(data + i);
      int table_id = vhdl_row->table_id;
      if (table_id == 0) continue; // shirley: this row was deleted and reseted during freelist
      int key0 = vhdl_row->key_0;
      int key1 = vhdl_row->key_1;
      int key2 = vhdl_row->key_2;
      int key3 = vhdl_row->key_3;
      switch (table_id) {
        case ((int)tpcc::TableType::Warehouse): {
          auto k = tpcc::Warehouse::Key::New(key0);
          auto handle = tables().Get<tpcc::Warehouse>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)tpcc::TableType::Item: {
          auto k = tpcc::Item::Key::New(key0);
          auto handle = tables().Get<tpcc::Item>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)tpcc::TableType::Stock: {
          auto k = tpcc::Stock::Key::New(key0, key1);
          auto handle = tables().Get<tpcc::Stock>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)tpcc::TableType::District: {
          auto k = tpcc::District::Key::New(key0, key1);
          auto handle = tables().Get<tpcc::District>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)tpcc::TableType::Customer: {
          auto k = tpcc::Customer::Key::New(key0, key1, key2);
          auto handle = tables().Get<tpcc::Customer>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)tpcc::TableType::CustomerInfo: {
          auto k = tpcc::CustomerInfo::Key::New(key0, key1, key2);
          auto handle = tables().Get<tpcc::CustomerInfo>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)tpcc::TableType::OOrder: {
          auto k = tpcc::OOrder::Key::New(key0, key1, key2);
          auto handle = tables().Get<tpcc::OOrder>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)tpcc::TableType::OOrderCIdIdx: {
          auto k = tpcc::OOrderCIdIdx::Key::New(key0, key1, key2, key3);
          auto handle = tables().Get<tpcc::OOrderCIdIdx>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)tpcc::TableType::NewOrder: {
          auto k = tpcc::NewOrder::Key::New(key0, key1, key2, key3);
          auto handle = tables().Get<tpcc::NewOrder>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)tpcc::TableType::OrderLine: {
          auto k = tpcc::OrderLine::Key::New(key0, key1, key2, key3);
          auto handle = tables().Get<tpcc::OrderLine>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        default: {
          printf("tpcc recovery loader unknown table_id = %d\n", table_id);
          std::abort();
          break;
        }
      }
      // shirley: if using non-deterministic order id for auto increment, then need to revert changes.
      uint64_t vhdl_sid2 = vhdl_row->GetInlineSid(felis::SortedArrayVHandle::SidType2);
      if (!vhdl_sid2) {
        continue;
      }
      // shirley: reset sid2 if was written
      if (vhdl_sid2 >> 32 == curr_ep) {
        // continue;
        vhdl_row->ResetSid2();
        //shirley pmem shirley test
        _mm_clwb(vhdl_row);
      }
      else {
        auto vhdl_sid1 = vhdl_row->GetInlineSid(felis::SortedArrayVHandle::SidType1);
        if (vhdl_sid1 != vhdl_sid2) {
          vhdl_row->add_majorGC_if_ext();
        }
        // shirley: don't do minGC here, just rebuild major GC list for simplicity.
        // // shirley: sid2 exists from previous epoch. Perform minGC if ptr1 is external
        // vhdl_row->FreePtr1();
        // vhdl_row->Copy2To1();
        // vhdl_row->ResetSid2();
        // // shirley pmem shirley test
        // // _mm_clwb(vhdl_row); // just flush the cacheline that contains the sid1/2 info that we modified.
      }
    }

    return;
  }

  void *large_buf = alloca(1024);
  // logger->info("Warehouse Loader starts");
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

          auto handle = tables().Get<tpcc::Warehouse>().SearchOrCreate(k.EncodeView(large_buf));
          // printf("warehouse %d coreid %d\n", k.w_id, handle->object_coreid());
          OnNewRow(slice_id, TableType::Warehouse, k, handle);
          // shirley: probe size of version value
          // felis::probes::VersionValueSizeArray{(int)v.EncodeSize()}();
          // shirley: DON'T bc init tables. probe transient vs persistent
          // felis::probes::TransientPersistentCount{true}();
          // shirley: initial database should be allocated from inline pmem.
          auto p = handle->vhandle_ptr()->AllocFromInline(v.EncodeSize());
          felis::InitVersion(handle, i, -1, -1, -1, (int)tpcc::TableType::Warehouse, v.EncodeToPtrOrDefault(p));
          // felis::InitVersion(handle, v.Encode());
        });
  }

  // logger->info("Warehouse Loader done.");
}

template <>
void Loader<LoaderType::Item>::DoLoad()
{
  // shirley: don't load / initialize tables if is recovery
  if (felis::Options::kRecovery) {
    return;
  }

  // Item table is a read only table, we replicate this table on all nodes!
  int last_affinity = -1;
  void *large_buf = alloca(1024);
  // logger->info("Item Loader starts");
  for (uint i = 1; i <= g_tpcc_config.nr_items; i++) {
    int core_id = (i - 1) * NodeConfiguration::g_nr_threads / g_tpcc_config.nr_items;
    if (go::Scheduler::CurrentThreadPoolId() - 1 != core_id) continue;

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
    auto handle = tables().Get<tpcc::Item>().SearchOrCreate(k.EncodeView(large_buf));
    // no OnNewRow() here, we don't ship Item table
    // shirley: probe size of version value
    // felis::probes::VersionValueSizeArray{(int)v.EncodeSize()}();
    // shirley: DON'T bc init tables. probe transient vs persistent
    // felis::probes::TransientPersistentCount{true}();
    auto p = handle->vhandle_ptr()->AllocFromInline(v.EncodeSize());
    felis::InitVersion(handle, i, -1, -1, -1, (int)tpcc::TableType::Item, v.EncodeToPtrOrDefault(p));
  }
  // logger->info("Item Loader done.");
}

template <>
void Loader<LoaderType::Stock>::DoLoad()
{
  // shirley: don't load / initialize tables if is recovery
  if (felis::Options::kRecovery) {
    return;
  }

  // Stock and StockData table
  void *large_buf = alloca(1024);
  std::string s_dist = RandomStr(24);
  // logger->info("Stock Loader starts");
  for (uint w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
    // logger->info("Stock Loader on warehouse {}", w);
    for(size_t i = 1; i <= g_tpcc_config.nr_items; i++) {
      const auto k = Stock::Key::New(w, i);
      // const auto k_data =  StockData::Key::New(w, i);

      DoOnSlice<tpcc::Stock>(
          k,
          [=](auto slice_id, auto core_id) {
            Stock::Value v;
            v.s_quantity = RandomNumber(10, 100);
            v.s_ytd = 0;
            v.s_order_cnt = 0;
            v.s_remote_cnt = 0;

            Checker::SanityCheckStock(&k, &v);

            auto handle = tables().Get<tpcc::Stock>().SearchOrCreate(k.EncodeView(large_buf));

            OnNewRow(slice_id, TableType::Stock, k, handle);
            // shirley: probe size of version value
            // felis::probes::VersionValueSizeArray{(int)v.EncodeSize()}();
            // shirley: DON'T bc init tables. probe transient vs persistent
            // felis::probes::TransientPersistentCount{true}();
            // shirley: initial database should be allocated from inline pmem.
            auto p = handle->vhandle_ptr()->AllocFromInline(v.EncodeSize());
            felis::InitVersion(handle, w, i, -1, -1, (int)tpcc::TableType::Stock, v.EncodeToPtrOrDefault(p));
            //felis::InitVersion(handle, v.Encode());
          });
#if 0
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
            auto data_handle = tables().Get<tpcc::StockData>().SearchOrCreate(k_data.EncodeView(large_buf));

            OnNewRow(slice_id, TableType::StockData, k_data, data_handle);
            felis::InitVersion(data_handle, v_data.Encode());
          });
#endif
    }
  }
  // logger->info("Stock Loader done.");
}

template <>
void Loader<LoaderType::District>::DoLoad()
{
  // shirley: don't load / initialize tables if is recovery
  if (felis::Options::kRecovery) {
    return;
  }

  void *large_buf = alloca(1024);
  // logger->info("District Loader starts");
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

            auto handle = tables().Get<tpcc::District>().SearchOrCreate(k.EncodeView(large_buf));

            OnNewRow(slice_id, TableType::District, k, handle);
            // shirley: probe size of version value
            // felis::probes::VersionValueSizeArray{(int)v.EncodeSize()}();
            // shirley: DON'T bc init tables. probe transient vs persistent
            // felis::probes::TransientPersistentCount{true}();
            // shirley: initial database should be allocated from inline pmem.
            auto p = handle->vhandle_ptr()->AllocFromInline(v.EncodeSize());
            felis::InitVersion(handle, w, d, -1, -1, (int)tpcc::TableType::District, v.EncodeToPtrOrDefault(p));
            // felis::InitVersion(handle, v.Encode());
          });

    }
  }
  if (Client::g_enable_pwv) {
    g_pwv_stock_resources = new uint64_t[g_tpcc_config.nr_warehouses];
    std::fill(g_pwv_stock_resources,
              g_pwv_stock_resources + g_tpcc_config.nr_warehouses,
              0);
  }
  // logger->info("District Loader done.");
}

template <>
void Loader<LoaderType::Customer>::DoLoad()
{
  // shirley: don't load / initialize tables if is recovery
  if (felis::Options::kRecovery) {
    return;
  }

  void *large_buf = alloca(1024);
  // logger->info("Customer Loader starts");
  for (auto w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
    for (auto d = 1; d <= g_tpcc_config.districts_per_warehouse; d++) {
      // logger->info("Customer Loader on warehouse {}", w);
      for (auto cidx0 = 0; cidx0 < g_tpcc_config.customers_per_district; cidx0++) {
        const uint c = cidx0 + 1;
        auto k = Customer::Key::New(w, d, c);

        DoOnSlice<tpcc::Customer>(
            k,
            [=](auto slice_id, auto core_id) {
              Customer::Value v;
              CustomerInfo::Value info_v;
              v.c_discount = RandomNumber(1, 5000) / 100;
              if (RandomNumber(1, 100) <= 10)
                v.c_credit.assign("BC");
              else
                v.c_credit.assign("GC");

              if (c <= 1000)
                info_v.c_last.assign(GetCustomerLastName(c - 1));
              else
                info_v.c_last.assign(GetNonUniformCustomerLastNameLoad());

              info_v.c_first.assign(RandomStr(RandomNumber(8, 16)));
              v.c_credit_lim = 50000;

              v.c_balance = -1000;
              v.c_ytd_payment = 1000;
              v.c_payment_cnt = 1;
              v.c_delivery_cnt = 0;

              info_v.c_street_1.assign(RandomStr(RandomNumber(10, 20)));
              info_v.c_street_2.assign(RandomStr(RandomNumber(10, 20)));
              info_v.c_city.assign(RandomStr(RandomNumber(10, 20)));
              info_v.c_state.assign(RandomStr(3));
              info_v.c_zip.assign(RandomNStr(4) + "11111");
              info_v.c_phone.assign(RandomNStr(16));
              info_v.c_since = GetCurrentTime();
              info_v.c_middle.assign("OE");
              info_v.c_data.assign(RandomStr(RandomNumber(300, 500)));

              Checker::SanityCheckCustomer(&k, &v);

              auto handle = tables().Get<tpcc::Customer>().SearchOrCreate(k.EncodeView(large_buf));
              OnNewRow(slice_id, TableType::Customer, k, handle);
              // shirley: probe size of version value
              // felis::probes::VersionValueSizeArray{(int)v.EncodeSize()}();
              // shirley: DON'T bc init tables. probe transient vs persistent
              // felis::probes::TransientPersistentCount{true}();
              // shirley: initial database should be allocated from inline pmem.
              auto p = handle->vhandle_ptr()->AllocFromInline(v.EncodeSize());
              felis::InitVersion(handle, w, d, c, -1, (int)tpcc::TableType::Customer, v.EncodeToPtrOrDefault(p));
              // felis::InitVersion(handle, v.Encode());

              auto info_handle = tables().Get<tpcc::CustomerInfo>().SearchOrCreate(k.EncodeView(large_buf));
              OnNewRow(slice_id, TableType::CustomerInfo, k, handle);
              // shirley: initial database should be allocated from inline pmem.
              auto info_p = info_handle->vhandle_ptr()->AllocFromInline(info_v.EncodeSize());
              felis::InitVersion(info_handle, w, d, c, -1, (int)tpcc::TableType::CustomerInfo, info_v.EncodeToPtrOrDefault(info_p));
              // felis::InitVersion(info_handle, info_v.Encode());
            });

        // I don't think we ever used this, I'll just disable this for now.
#if 0
        // customer name index
        auto k_idx = CustomerNameIdx::Key::New(k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));
        auto v_idx = CustomerNameIdx::Value::New(k.c_id);

        // index structure is:
        // (c_w_id, c_d_id, c_last, c_first) -> (c_id)

        handle = tables().Get<tpcc::CustomerNameIdx>().SearchOrCreate(k_idx.EncodeView(large_buf));
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
              auto hist_handle = tables().Get<tpcc::History>().SearchOrCreate(k_hist.EncodeView(large_buf));

              OnNewRow(slice_id, TableType::History, k_hist, hist_handle);
              felis::InitVersion(hist_handle, v_hist.Encode());
            });
#endif
      }
    }
  }
  // logger->info("Customer Loader done.");
}

template <>
void Loader<LoaderType::Order>::DoLoad()
{
  // shirley: don't load / initialize tables if is recovery
  if (felis::Options::kRecovery) {
    if (go::Scheduler::CurrentThreadPoolId() != 1) {
      // shirley: only let 1 thread do the recovery
      return;
    }
    uint64_t largest_sid = mem::GetPmemPersistInfo()->largest_sid;
    uint64_t last_epoch_nr = largest_sid >> 32;
    bool first_slot = !(last_epoch_nr % 2);
    
    uint64_t *auto_inc_addr;
    if (first_slot) {
      auto_inc_addr = mem::GetPmemPersistInfo()->auto_inc_cnt;
    }
    else {
      auto_inc_addr = mem::GetPmemPersistInfo()->auto_inc_cnt_2;
    }
    util::Instance<TableManager>().Get<tpcc::OOrder>().RecoverAutoIncrement(auto_inc_addr);
    return;
  }

  auto nr_all_districts = g_tpcc_config.nr_warehouses * g_tpcc_config.districts_per_warehouse;
  for (int i = 1; i <= util::Instance<NodeConfiguration>().nr_nodes(); i++) {
    g_last_no_start[i] = new std::atomic_ulong[nr_all_districts];
    g_last_no_end[i] = new std::atomic_ulong[nr_all_districts];

    std::fill(g_last_no_start[i], g_last_no_start[i] + nr_all_districts, 2101);
    std::fill(g_last_no_end[i], g_last_no_end[i] + nr_all_districts, 3000);
  }

  void *large_buf = alloca(1024);
  // a random permutation of customer IDs
  auto c_ids = new uint32_t[g_tpcc_config.customers_per_district];

  // logger->info("Order Loader starts.");
  for (auto w = 1; w <= g_tpcc_config.nr_warehouses; w++) {
    // logger->info("Order Loader on warehouse {}", w);
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

              auto oo_handle = tables().Get<tpcc::OOrder>().SearchOrCreate(k_oo.EncodeView(large_buf));
              OnNewRow(slice_id, TableType::OOrder, k_oo, oo_handle);
              // shirley: probe size of version value
              // felis::probes::VersionValueSizeArray{(int)v_oo.EncodeSize()}();
              // shirley: DON'T bc init tables. probe transient vs persistent
              // felis::probes::TransientPersistentCount{true}();
              auto p = oo_handle->vhandle_ptr()->AllocFromInline(v_oo.EncodeSize());
              felis::InitVersion(oo_handle, w, d, order_id, -1, (int)tpcc::TableType::OOrder, v_oo.EncodeToPtrOrDefault(p));
            });

        const auto k_oo_idx = OOrderCIdIdx::Key::New(k_oo.o_w_id, k_oo.o_d_id, c_ids[c - 1], k_oo.o_id);
        const auto v_oo_idx = OOrderCIdIdx::Value::New(0);

        DoOnSlice<tpcc::OOrderCIdIdx>(
            k_oo_idx,
            [=](auto slice_id, auto core_id) {
              auto oo_idx_handle = tables().Get<tpcc::OOrderCIdIdx>().SearchOrCreate(k_oo_idx.EncodeView(large_buf));
              OnNewRow(slice_id, TableType::OOrderCIdIdx, k_oo_idx, oo_idx_handle);
              // shirley: probe size of version value
              // felis::probes::VersionValueSizeArray{(int)v_oo_idx.EncodeSize()}();
              // shirley: DON'T bc init tables. probe transient vs persistent
              // felis::probes::TransientPersistentCount{true}();
              auto p = oo_idx_handle->vhandle_ptr()->AllocFromInline(v_oo_idx.EncodeSize());
              felis::InitVersion(oo_idx_handle, k_oo.o_w_id, k_oo.o_d_id, c_ids[c - 1], k_oo.o_id, (int)tpcc::TableType::OOrderCIdIdx, v_oo_idx.EncodeToPtrOrDefault(p));
            });

        if (c >= 2101) {
          auto k_no = NewOrder::Key::New(w, d, k_oo.o_id, c_ids[c - 1]);
          NewOrder::Value v_no;
          Checker::SanityCheckNewOrder(&k_no, &v_no);
          DoOnSlice<tpcc::NewOrder>(
              k_no,
              [=](auto slice_id, auto core_id) {
                auto no_handle = tables().Get<tpcc::NewOrder>().SearchOrCreate(k_no.EncodeView(large_buf));
                OnNewRow(slice_id, TableType::NewOrder, k_no, no_handle);
                // shirley: probe size of version value
                // felis::probes::VersionValueSizeArray{(int)v_no.EncodeSize()}();
                // shirley: DON'T bc init tables. probe transient vs persistent
                // felis::probes::TransientPersistentCount{true}();
                auto p = no_handle->vhandle_ptr()->AllocFromInline(v_no.EncodeSize());
                felis::InitVersion(no_handle, w, d, k_oo.o_id, c_ids[c - 1], (int)tpcc::TableType::NewOrder, v_no.EncodeToPtrOrDefault(p));
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

                auto ol_handle = tables().Get<tpcc::OrderLine>().SearchOrCreate(k_ol.EncodeView(large_buf));

                OnNewRow(slice_id, TableType::OrderLine, k_ol, ol_handle);
                // shirley: probe size of version value
                // felis::probes::VersionValueSizeArray{(int)v_ol.EncodeSize()}();
                // shirley: DON'T bc init tables. probe transient vs persistent
                // felis::probes::TransientPersistentCount{true}();
                auto p = ol_handle->vhandle_ptr()->AllocFromInline(v_ol.EncodeSize());
                felis::InitVersion(ol_handle, w, d, k_oo.o_id, l, (int)tpcc::TableType::OrderLine, v_ol.EncodeToPtrOrDefault(p));
              });
        }
      }
      auto auto_inc_zone = w * 10 + d;
      tables().Get<tpcc::OOrder>().ResetAutoIncrement(
          auto_inc_zone, g_tpcc_config.customers_per_district + 1);
    }
  }

  delete [] c_ids;
  // logger->info("Order Loader done.");
}

}

//45, 43, 4, 4, 4 //50, 50, 0, 0, 0
static constexpr int kTPCCTxnMix[] = {
  45, 43, 4, 4, 4
};

felis::BaseTxn *Client::CreateTxn(uint64_t serial_id, void *txntype_id, void *txn_struct_buffer)
{
  int rd = r.next_u32() % 100;
  int txn_type_id = 0;
  while (true) {
    int threshold = kTPCCTxnMix[txn_type_id];
    if (rd < threshold)
      break;
    rd -= threshold;
    txn_type_id++;
  }
  felis::BaseTxn *base_txn = TxnFactory::Create(TxnType(txn_type_id), this, serial_id);
  base_txn->txn_typeid = txn_type_id;

  return base_txn;
}

felis::BaseTxn *Client::CreateTxnRecovery(uint64_t serial_id, int txntype_id, void *txn_struct_buffer) {
  felis::BaseTxn *base_txn = nullptr;
  switch (txntype_id) {
    case (int)(tpcc::TxnType::NewOrder): {
      base_txn = new tpcc::NewOrderTxn(this, serial_id, (NewOrderStruct *)txn_struct_buffer);
      break;
    }
    case (int)(tpcc::TxnType::Payment): {
      base_txn = new tpcc::PaymentTxn(this, serial_id, (PaymentStruct *)txn_struct_buffer);
      break;
    }
    case (int)(tpcc::TxnType::Delivery): {
      base_txn = new tpcc::DeliveryTxn(this, serial_id, (DeliveryStruct *)txn_struct_buffer);
      break;
    }
    case (int)(tpcc::TxnType::OrderStatus): {
      base_txn = new tpcc::OrderStatusTxn(this, serial_id, (OrderStatusStruct *)txn_struct_buffer);
      break;
    }
    case (int)(tpcc::TxnType::StockLevel): {
      base_txn = new tpcc::StockLevelTxn(this, serial_id, (StockLevelStruct *)txn_struct_buffer);
      break;
    }
    default: {
      printf("tpcc CreateTxnRecovery unknown txn_id = %d\n", txntype_id);
      std::abort();
    }
  }
  base_txn->txn_typeid = txntype_id;
  return base_txn;
}

size_t Client::TxnInputSize(int txn_id) {
  size_t input_size;
  switch (txn_id) {
    case (int)(tpcc::TxnType::NewOrder): {
      input_size = util::Align(sizeof(NewOrderStruct), 8);
      break;
    }
    case (int)(tpcc::TxnType::Payment): {
      input_size = util::Align(sizeof(PaymentStruct), 8);
      break;
    }
    case (int)(tpcc::TxnType::Delivery): {
      input_size = util::Align(sizeof(DeliveryStruct), 8);
      break;
    }
    case (int)(tpcc::TxnType::OrderStatus): {
      input_size = util::Align(sizeof(OrderStatusStruct), 8);
      break;
    }
    case (int)(tpcc::TxnType::StockLevel): {
      input_size = util::Align(sizeof(StockLevelStruct), 8);
      break;
    }
    default: {
      printf("tpcc TxnInputSize unknown txn_id = %d\n", txn_id);
      std::abort();
    }
  }
  return input_size;
}

void Client::PersistTxnStruct(int txn_id, void *base_txn, void *txn_struct_buffer) {
  switch (txn_id) {
    case (int)(tpcc::TxnType::NewOrder): {
      NewOrderStruct txn_struct = *(NewOrderTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(NewOrderStruct));
      break;
    }
    case (int)(tpcc::TxnType::Payment): {
      PaymentStruct txn_struct = *(PaymentTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(PaymentStruct));
      break;
    }
    case (int)(tpcc::TxnType::Delivery): {
      DeliveryStruct txn_struct = *(DeliveryTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(DeliveryStruct));
      break;
    }
    case (int)(tpcc::TxnType::OrderStatus): {
      OrderStatusStruct txn_struct = *(OrderStatusTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(OrderStatusStruct));
      break;
    }
    case (int)(tpcc::TxnType::StockLevel): {
      StockLevelStruct txn_struct = *(StockLevelTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(StockLevelStruct));
      break;
    }
    default: {
      printf("tpcc PersistTxnStruct unknown txn_id = %d\n", txn_id);
      std::abort();
    }
  }
  return;
}

void Client::PersistAutoInc() {
  uint64_t curr_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  bool first_slot = !(curr_epoch_nr % 2);
  uint64_t *auto_inc_addr;
  if (first_slot) {
    auto_inc_addr = mem::GetPmemPersistInfo()->auto_inc_cnt;
  }
  else {
    auto_inc_addr = mem::GetPmemPersistInfo()->auto_inc_cnt_2;
  }
  util::Instance<TableManager>().Get<tpcc::OOrder>().PersistAutoIncrement(auto_inc_addr);
  mem::FlushPmemPersistInfo();
  return;
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
