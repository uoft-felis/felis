#include "table_decl.h"

#include <sys/time.h>
#include <string>
#include <sys/types.h>
#include <cstdlib>

#include <cstdlib>
#include <unistd.h>
// #include <getopt.h>

#include <set>
#include <vector>
#include <memory>
#include <string>
#include <mutex>
#include <atomic>

#include "util.h"
#include "node_config.h"
#include "tpcc.h"

using felis::Relation;
using felis::RelationManager;
using felis::NodeConfiguration;
using util::Instance;

namespace tpcc {

struct Config {
  bool uniform_item_distribution = false;

  size_t nr_items = 100000;
  size_t nr_warehouses = 8;
  size_t districts_per_warehouse = 10;
  size_t customers_per_district = 3000;
} kTPCCConfig;

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

// maps a wid => core id
static inline unsigned int CoreId(unsigned int wid)
{
  assert(wid >= 1 && wid <= kTPCCConfig.nr_warehouses);
  int nthreads = NodeConfiguration::kNrThreads;
  wid -= 1; // 0-idx
  if (kTPCCConfig.nr_warehouses <= nthreads)
    return wid;
  const unsigned nwhse_per_partition = kTPCCConfig.nr_warehouses / nthreads;
  const unsigned partid = wid / nwhse_per_partition;
  return partid;
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
    auto &conf = Instance<NodeConfiguration>();
    uint min = kTPCCConfig.nr_warehouses * (conf.node_id() - 1) / conf.nr_nodes() + 1;
    uint max = kTPCCConfig.nr_warehouses * conf.node_id() / conf.nr_nodes();
    return RandomNumber(min, max);
  }
  return r.next() % kTPCCConfig.nr_warehouses + 1;
}

uint ClientBase::PickDistrict()
{
  return RandomNumber(1, kTPCCConfig.districts_per_warehouse);
}

uint ClientBase::GetCurrentTime()
{
  static __thread uint tl_hack = 0;
  return tl_hack;
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

TableHandles::TableHandles()
{
  assert(kTPCCConfig.nr_warehouses >= 1);
  for (int table_id = 0; table_id < static_cast<int>(TableType::NRTable); table_id++) {
    InitiateTable(static_cast<TableType>(table_id));
  }
}

void TableHandles::InitiateTable(TableType table)
{
  RelationManager &mgr = Instance<RelationManager>();
  std::string name = kTPCCTableNames[static_cast<int>(table)];

  logger->info("Initialize TPCC Table {} id {}", name, (int) table);

  int base_idx = static_cast<int>(table) * kTPCCConfig.nr_warehouses;
  int thandle = (int) table;

  for (size_t i = 0; i < kTPCCConfig.nr_warehouses; i++)
    table_handles[base_idx + i] = thandle;
}

felis::Relation &Util::relation(TableType table, unsigned int wid)
{
  assert(wid > 0); // wid starting from 1
  int idx = static_cast<int>(table) * kTPCCConfig.nr_warehouses + wid - 1;
  int fid = Instance<TableHandles>().table_handle(idx);
  return Instance<RelationManager>().GetRelationOrCreate(fid);
}

int Util::partition(unsigned int wid)
{
  // partition id also starts from 1 because 0 means local shard. See node_config.cc
  return (wid - 1) * Instance<NodeConfiguration>().nr_nodes() / kTPCCConfig.nr_warehouses + 1;
}

namespace loaders {

template <>
void Loader<LoaderType::Warehouse>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint i = 1; i <= kTPCCConfig.nr_warehouses; i++) {
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

    relation(TableType::Warehouse, i).InitValue(k.EncodeFromAlloca(large_buf), v.Encode());
  }

  for (uint i = 1; i <= kTPCCConfig.nr_warehouses; i++) {
    relation(TableType::Warehouse, i).set_key_length(sizeof(Warehouse::Key));
  }
  logger->info("Warehouse Table loading done.");
}

template <>
void Loader<LoaderType::Item>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint i = 1; i <= kTPCCConfig.nr_items; i++) {
    // items don't "belong" to a certain warehouse, so no pinning
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
    relation(TableType::Item, 1)
        .InitValue(k.EncodeFromAlloca(large_buf), v.Encode());
  }
  relation(TableType::Item, 1).set_key_length(sizeof(Item::Key));
  logger->info("Item Table loading done.");
}

template <>
void Loader<LoaderType::Stock>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint w = 1; w <= kTPCCConfig.nr_warehouses; w++) {
    util::PinToCPU(CoreId(w));
    mem::SetThreadLocalAllocAffinity(CoreId(w));

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

      relation(TableType::Stock, w).InitValue(k.EncodeFromAlloca(large_buf), v.Encode());
      relation(TableType::StockData, w).InitValue(k_data.EncodeFromAlloca(large_buf), v_data.Encode());
    }
    relation(TableType::Stock, w).set_key_length(sizeof(Stock::Key));
    relation(TableType::StockData, w).set_key_length(sizeof(StockData::Key));
  }
  mem::SetThreadLocalAllocAffinity(-1);
  logger->info("Stock Table loading done.");
}

template <>
void Loader<LoaderType::District>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint w = 1; w <= kTPCCConfig.nr_warehouses; w++) {
    util::PinToCPU(CoreId(w));
    mem::SetThreadLocalAllocAffinity(CoreId(w));

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

      relation(TableType::District, w).InitValue(k.EncodeFromAlloca(large_buf), v.Encode());
    }
    relation(TableType::District, w).set_key_length(sizeof(District::Key));
  }
  mem::SetThreadLocalAllocAffinity(-1);
  logger->info("District Table loading done.");
}

template <>
void Loader<LoaderType::Customer>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint w = 1; w <= kTPCCConfig.nr_warehouses; w++) {
    util::PinToCPU(CoreId(w));
    mem::SetThreadLocalAllocAffinity(CoreId(w));

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
	relation(TableType::Customer, w).InitValue(k.EncodeFromAlloca(large_buf), v.Encode());

	// customer name index
	auto k_idx = CustomerNameIdx::Key::New(k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));
	auto v_idx = CustomerNameIdx::Value::New(k.c_id);

	// index structure is:
	// (c_w_id, c_d_id, c_last, c_first) -> (c_id)

	relation(TableType::CustomerNameIdx, w).InitValue(k_idx.EncodeFromAlloca(large_buf),
							    v_idx.Encode());

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

	relation(TableType::History, w).InitValue(k_hist.EncodeFromAlloca(large_buf), v_hist.Encode());
      }
    }

    relation(TableType::Customer, w).set_key_length(sizeof(Customer::Key));
    relation(TableType::CustomerNameIdx, w).set_key_length(sizeof(CustomerNameIdx::Key));
    relation(TableType::History, w).set_key_length(sizeof(History::Key));
  }
  mem::SetThreadLocalAllocAffinity(-1);
}

template <>
void Loader<LoaderType::Order>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint w = 1; w <= kTPCCConfig.nr_warehouses; w++) {
    util::PinToCPU(CoreId(w));
    mem::SetThreadLocalAllocAffinity(CoreId(w) % NodeConfiguration::kNrThreads);;
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

      for (uint c = 1; c <= kTPCCConfig.customers_per_district; c++) {
	const auto k_oo = OOrder::Key::New(w, d, c);
	OOrder::Value v_oo;

	v_oo.o_c_id = c_ids[c - 1];
	if (k_oo.o_id < 2101)
	  v_oo.o_carrier_id = RandomNumber(1, 10);
	else
	  v_oo.o_carrier_id = 0;
	v_oo.o_ol_cnt = GetOrderLinesPerCustomer();
	v_oo.o_all_local = 1;
	v_oo.o_entry_d = GetCurrentTime();

	Checker::SanityCheckOOrder(&k_oo, &v_oo);

	relation(TableType::OOrder, w).InitValue(k_oo.EncodeFromAlloca(large_buf), v_oo.Encode());

	const auto k_oo_idx = OOrderCIdIdx::Key::New(k_oo.o_w_id, k_oo.o_d_id, v_oo.o_c_id, k_oo.o_id);
	const auto v_oo_idx = OOrderCIdIdx::Value::New(0);

	relation(TableType::OOrderCIdIdx, w).InitValue(k_oo_idx.EncodeFromAlloca(large_buf), v_oo_idx.Encode());

	if (c >= 2101) {
	  auto k_no = NewOrder::Key::New(w, d, c);
	  NewOrder::Value v_no;

	  Checker::SanityCheckNewOrder(&k_no, &v_no);

	  relation(TableType::NewOrder, w).InitValue(k_no.EncodeFromAlloca(large_buf), v_no.Encode());
	}

	for (uint l = 1; l <= uint(v_oo.o_ol_cnt); l++) {
	  auto k_ol = OrderLine::Key::New(w, d, c, l);
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
	  relation(TableType::OrderLine, w).InitValue(k_ol.EncodeFromAlloca(large_buf), v_ol.Encode());
	}
      }
    }
    relation(TableType::OOrder, w).set_key_length(sizeof(OOrder::Key));
    relation(TableType::OOrderCIdIdx, w).set_key_length(sizeof(OOrderCIdIdx::Key));
    relation(TableType::NewOrder, w).set_key_length(sizeof(NewOrder::Key));
    relation(TableType::OrderLine, w).set_key_length(sizeof(OrderLine::Key));
  }
  mem::SetThreadLocalAllocAffinity(-1);
}

}

felis::BaseTxn *Client::RunCreateTxn()
{
  // TODO: generate standard TPC-C txn mix here
  // currently, only NewOrder is available
  return TxnFactory::Create(static_cast<int>(TxnType::NewOrder), this);
}

}
