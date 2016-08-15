/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */

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

#include <mutex>
#include <atomic>

#include "util.h"

#include "tpcc.h"

using dolly::Relation;
using dolly::RelationManager;
using dolly::Epoch;
using util::MixIn;
using util::Instance;

namespace tpcc {

// configuration flags
int g_disable_xpartition_txn = 0;
int g_disable_read_only_scans = 0;
int g_enable_partition_locks = 0;
int g_enable_separate_tree_per_partition = 0;
int g_new_order_remote_item_pct = 1;
int g_uniform_item_dist = 0;
int g_order_status_scan_hack = 0;
int g_microbench_static = 0;
int g_microbench_simple = 0;
int g_microbench_random = 0;
int g_wh_temperature = 0;
uint g_microbench_rows = 100000;  // this many rows

// can't have both ratio and rows at the same time
double g_microbench_wr_ratio = 0; // this % of writes
uint g_microbench_wr_rows = 0; // this number of rows to write

// how much % of time a worker should use a random home wh
// 0 - always use home wh
// 50 - 50% of time use random wh
// 100 - always use a random wh
double g_wh_spread = 0;

size_t g_tpcc_scale_factor = dolly::Epoch::kNrThreads; // scale factor that's running on the client!

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

util::CacheAligned<std::mutex> *g_partition_locks = nullptr;

// T must implement lock()/unlock(). Both must *not* throw exceptions
template <typename T>
class ScopedMultilock {
public:
  inline ScopedMultilock()
    : did_lock(false) {}

  inline ~ScopedMultilock() {
    if (did_lock)
      for (auto &t : locks)
	t->unlock();
  }

  inline void enq(T &t) {
    assert(!did_lock);
    locks.emplace_back(&t);
  }

  inline void multilock() {
    assert(!did_lock);
    if (locks.size() > 1)
      sort(locks.begin(), locks.end());
#ifdef CHECK_INVARIANTS
    if (set<T *>(locks.begin(), locks.end()).size() != locks.size()) {
      for (auto &t : locks)
	cerr << "lock: " << hexify(t) << endl;
      INVARIANT(false && "duplicate locks found");
    }
#endif
    for (auto &t : locks)
      t->lock();
    did_lock = true;
  }

private:
  bool did_lock;
  typename std::vector<T*> locks;
};

// like a lock_guard, but has the option of not acquiring
template <typename T>
class ScopedLockGuard {
public:
  ScopedLockGuard(T &l) : l(&l) {
    this->l->lock();
  }

  ScopedLockGuard(T *l) : l(l) {
    if (this->l)
      this->l->lock();
  }

  ~ScopedLockGuard() {
    if (l)
      l->unlock();
  }

private:
  T *l;
};

static inline size_t NumWarehouses()
{
  return (size_t) g_tpcc_scale_factor;
}

// config constants
static constexpr inline size_t NumItems()
{
  return 100000;
}

static constexpr inline size_t NumDistrictsPerWarehouse()
{
  return 10;
}

static constexpr inline size_t NumCustomersPerDistrict()
{
  return 3000;
}

static int NMaxCustomerIdxScanElems = 512;

// maps a wid => partition id
static inline unsigned int PartitionId(unsigned int wid)
{
  assert(wid >= 1 && wid <= NumWarehouses());
  int nthreads = Epoch::kNrThreads;
  wid -= 1; // 0-idx
  if (NumWarehouses() <= nthreads)
    // more workers than partitions, so its easy
    return wid;
  const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
  const unsigned partid = wid / nwhse_per_partition;
  if (partid >= nthreads)
    return nthreads - 1;
  return partid;
}

static inline std::mutex &LockForPartition(unsigned int wid)
{
  return g_partition_locks[PartitionId(wid)].elem;
}

// utils for generating random #s and strings
static inline int CheckBetweenInclusive(int v, int lower, int upper)
{
  assert(v >= lower);
  assert(v <= upper);
  return v;
}

static inline int RandomNumber(util::FastRandom &r, int min, int max)
{
  return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
}

static inline int NonUniformRandom(util::FastRandom &r, int A, int C, int min, int max)
{
  return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
}

static inline int GetItemId(util::FastRandom &r)
{
  return CheckBetweenInclusive(
    g_uniform_item_dist ?
    RandomNumber(r, 1, NumItems()) :
    NonUniformRandom(r, 8191, 7911, 1, NumItems()),
    1, NumItems());
}

static inline int GetCustomerId(util::FastRandom &r)
{
  return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict()), 1, NumCustomersPerDistrict());
}

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

// all tokens are at most 5 chars long
static const size_t CustomerLastNameMaxSize = 5 * 3;

static inline size_t
GetCustomerLastName(uint8_t *buf, util::FastRandom &r, int num)
{
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

static inline size_t GetCustomerLastName(char *buf, util::FastRandom &r, int num)
{
  return GetCustomerLastName((uint8_t *) buf, r, num);
}

static inline std::string GetCustomerLastName(util::FastRandom &r, int num)
{
  std::string ret;
  ret.resize(CustomerLastNameMaxSize);
  ret.resize(GetCustomerLastName((uint8_t *) &ret[0], r, num));
  return ret;
}

static inline std::string GetNonUniformCustomerLastNameLoad(util::FastRandom &r)
{
  return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
}

static inline size_t GetNonUniformCustomerLastNameRun(uint8_t *buf, util::FastRandom &r)
{
  return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
}

static inline size_t GetNonUniformCustomerLastNameRun(char *buf, util::FastRandom &r)
{
  return GetNonUniformCustomerLastNameRun((uint8_t *) buf, r);
}

static inline std::string
GetNonUniformCustomerLastNameRun(util::FastRandom &r)
{
  return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
}

// following oltpbench, we really generate strings of len - 1...
static inline std::string RandomStr(util::FastRandom &r, uint len)
{
  // this is a property of the oltpbench implementation...
  if (len == 0)
    return "";

  uint i = 0;
  std::string buf(len - 1, 0);
  while (i < (len - 1)) {
    const char c = (char) r.next_char();
    // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
    // is a less restrictive filter than isalnum()
    if (!isalnum(c))
      continue;
    buf[i++] = c;
  }
  return buf;
}

// RandomNStr() actually produces a string of length len
static inline std::string RandomNStr(util::FastRandom &r, uint len)
{
  const char base = '0';
  std::string buf(len, 0);
  for (uint i = 0; i < len; i++)
    buf[i] = (char)(base + (r.next() % 10));
  return buf;
}

struct Checker {
  // these sanity checks are just a few simple checks to make sure
  // the data is not entirely corrupted

  static inline  void
  SanityCheckCustomer(const Customer::Key *k, const Customer::Value *v) {
    assert(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= NumWarehouses());
    assert(k->c_d_id >= 1 && static_cast<size_t>(k->c_d_id) <= NumDistrictsPerWarehouse());
    assert(k->c_id >= 1 && static_cast<size_t>(k->c_id) <= NumCustomersPerDistrict());
    assert(v->c_credit == "BC" || v->c_credit == "GC");
    assert(v->c_middle == "OE");
  }

  static inline  void
  SanityCheckWarehouse(const Warehouse::Key *k, const Warehouse::Value *v) {
    assert(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= NumWarehouses());
    assert(v->w_state.size() == 2);
    assert(v->w_zip == "123456789");
  }

  static inline  void
  SanityCheckDistrict(const District::Key *k, const District::Value *v) {
    assert(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= NumWarehouses());
    assert(k->d_id >= 1 && static_cast<size_t>(k->d_id) <= NumDistrictsPerWarehouse());
    assert(v->d_next_o_id >= 3001);
    assert(v->d_state.size() == 2);
    assert(v->d_zip == "123456789");
  }

  static inline  void
  SanityCheckItem(const Item::Key *k, const Item::Value *v) {
    assert(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= NumItems());
    assert(v->i_price >= 1.0 && v->i_price <= 100.0);
  }

  static inline  void
  SanityCheckStock(const Stock::Key *k, const Stock::Value *v) {
    assert(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= NumWarehouses());
    assert(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= NumItems());
  }

  static inline  void
  SanityCheckNewOrder(const NewOrder::Key *k, const NewOrder::Value *v) {
    assert(k->no_w_id >= 1 && static_cast<size_t>(k->no_w_id) <= NumWarehouses());
    assert(k->no_d_id >= 1 && static_cast<size_t>(k->no_d_id) <= NumDistrictsPerWarehouse());
  }

  static inline  void
   SanityCheckOOrder(const OOrder::Key *k, const OOrder::Value *v) {
    assert(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= NumWarehouses());
    assert(k->o_d_id >= 1 && static_cast<size_t>(k->o_d_id) <= NumDistrictsPerWarehouse());
    assert(v->o_c_id >= 1 && static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
    assert(v->o_carrier_id >= 0 && static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
    assert(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
  }

  static inline void
  SanityCheckOrderLine(const OrderLine::Key *k, const OrderLine::Value *v) {
    assert(k->ol_w_id >= 1 && static_cast<size_t>(k->ol_w_id) <= NumWarehouses());
    assert(k->ol_d_id >= 1 && static_cast<size_t>(k->ol_d_id) <= NumDistrictsPerWarehouse());
    assert(k->ol_number >= 1 && k->ol_number <= 15);
    assert(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems());
  }

};

TPCCTableHandles::TPCCTableHandles()
{
  assert(NumWarehouses() >= 1);
    for (int table_id = 0; table_id < static_cast<int>(TPCCTable::NRTable); table_id++) {
      InitiateTable(static_cast<TPCCTable>(table_id));
    }
}

void TPCCTableHandles::InitiateTable(TPCCTable table)
{
  logger->info("Initialize TPCC Table {}", (int) table);
  int nthreads = Epoch::kNrThreads;
  RelationManager &mgr = Instance<RelationManager>();
  std::string name = kTPCCTableNames[static_cast<int>(table)];

  int base_idx = static_cast<int>(table) * NumWarehouses();

  if (g_enable_separate_tree_per_partition) {
    if (NumWarehouses() <= nthreads) {
      for (size_t i = 0; i < NumWarehouses(); i++) {
	table_handles[base_idx + i] = mgr.LookupRelationId(name + "_" + std::to_string(i));
      }
    } else {
      const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
      for (size_t partid = 0; partid < nthreads; partid++) {
	const unsigned wstart = partid * nwhse_per_partition;
	const unsigned wend   = (partid + 1 == nthreads) ?
	  NumWarehouses() : (partid + 1) * nwhse_per_partition;
	int thandle = mgr.LookupRelationId(name + "_" + std::to_string(partid));
	for (size_t i = wstart; i < wend; i++)
	  table_handles[base_idx + i] = thandle;
      }
    }
  } else {
    int thandle = mgr.LookupRelationId(name);
    for (size_t i = 0; i < NumWarehouses(); i++)
      table_handles[base_idx + i] = thandle;
  }
}

static TPCCTableHandles *global_table_handles;

dolly::Relation &TPCCMixIn::relation(TPCCTable table, unsigned int wid)
{
  assert(wid > 0); // wid starting from 1
  int idx = static_cast<int>(table) * NumWarehouses() + wid - 1;
  int fid = global_table_handles->table_handle(idx);
  return Instance<RelationManager>().GetRelationOrCreate(fid);
}

namespace loaders {

template <>
void Loader<TPCCLoader::Warehouse>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint i = 1; i <= NumWarehouses(); i++) {
    auto k = Warehouse::Key(i);
    auto v = Warehouse::Value();

    auto w_name = RandomStr(r, RandomNumber(r, 6, 10));
    auto w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
    auto w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
    auto w_city = RandomStr(r, RandomNumber(r, 10, 20));
    auto w_state = RandomStr(r, 3);
    auto w_zip = std::string("123456789");

    v.w_ytd = 300000;
    v.w_tax = (float) RandomNumber(r, 0, 2000) / 10000.0;
    v.w_name.assign(w_name);
    v.w_street_1.assign(w_street_1);
    v.w_street_2.assign(w_street_2);
    v.w_city.assign(w_city);
    v.w_state.assign(w_state);
    v.w_zip.assign(w_zip);

    Checker::SanityCheckWarehouse(&k, &v);

    relation(TPCCTable::Warehouse, i).SetupReExec(k.EncodeFromAlloca(large_buf), 0, v.Encode());
  }
  logger->info("Warehouse Table loading done.");
}

template <>
void Loader<TPCCLoader::Item>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint i = 1; i <= NumItems(); i++) {
    // items don't "belong" to a certain warehouse, so no pinning
    const Item::Key k(i);
    Item::Value v;

    auto i_name = RandomStr(r, RandomNumber(r, 14, 24));
    v.i_name.assign(i_name);
    v.i_price = (float) RandomNumber(r, 100, 10000) / 100.0;

    const int len = RandomNumber(r, 26, 50);
    if (RandomNumber(r, 1, 100) > 10) {
      const std::string i_data = RandomStr(r, len);
      v.i_data.assign(i_data);
    } else {
      const int startOriginal = RandomNumber(r, 2, (len - 8));
      const std::string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL"
	+ RandomStr(r, len - startOriginal - 7);
      v.i_data.assign(i_data);
    }
    v.i_im_id = RandomNumber(r, 1, 10000);

    Checker::SanityCheckItem(&k, &v);
    relation(TPCCTable::Item, 1)
      .SetupReExec(k.EncodeFromAlloca(large_buf), 0, v.Encode());
  }
  logger->info("Item Table loading done.");
}

template <>
void Loader<TPCCLoader::Stock>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint w = 1; w <= NumWarehouses(); w++) {
    util::PinToCPU(PartitionId(w));
    mem::SetThreadLocalAllocAffinity(PartitionId(w));

    for(size_t i = 1; i <= NumItems(); i++) {
      const Stock::Key k(w, i);
      const StockData::Key k_data(w, i);

      Stock::Value v;
      v.s_quantity = RandomNumber(r, 10, 100);
      v.s_ytd = 0;
      v.s_order_cnt = 0;
      v.s_remote_cnt = 0;

      StockData::Value v_data;
      const int len = RandomNumber(r, 26, 50);
      if (RandomNumber(r, 1, 100) > 10) {
	const std::string s_data = RandomStr(r, len);
	v_data.s_data.assign(s_data);
      } else {
	const int startOriginal = RandomNumber(r, 2, (len - 8));
	const std::string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL"
	  + RandomStr(r, len - startOriginal - 7);
	v_data.s_data.assign(s_data);
      }
      v_data.s_dist_01.assign(RandomStr(r, 24));
      v_data.s_dist_02.assign(RandomStr(r, 24));
      v_data.s_dist_03.assign(RandomStr(r, 24));
      v_data.s_dist_04.assign(RandomStr(r, 24));
      v_data.s_dist_05.assign(RandomStr(r, 24));
      v_data.s_dist_06.assign(RandomStr(r, 24));
      v_data.s_dist_07.assign(RandomStr(r, 24));
      v_data.s_dist_08.assign(RandomStr(r, 24));
      v_data.s_dist_09.assign(RandomStr(r, 24));
      v_data.s_dist_10.assign(RandomStr(r, 24));

      Checker::SanityCheckStock(&k, &v);

      relation(TPCCTable::Stock, w).SetupReExec(k.EncodeFromAlloca(large_buf), 0, v.Encode());
      relation(TPCCTable::StockData, w).SetupReExec(k_data.EncodeFromAlloca(large_buf), 0, v_data.Encode());
    }
  }
  mem::SetThreadLocalAllocAffinity(-1);
  logger->info("Stock Table loading done.");
}

template <>
void Loader<TPCCLoader::District>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint w = 1; w <= NumWarehouses(); w++) {
    util::PinToCPU(PartitionId(w));
    mem::SetThreadLocalAllocAffinity(PartitionId(w));

    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      const District::Key k(w, d);
      District::Value v;
      v.d_ytd = 30000;
      v.d_tax = (float) (RandomNumber(r, 0, 2000) / 10000.0);
      v.d_next_o_id = 3001;
      v.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
      v.d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
      v.d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
      v.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
      v.d_state.assign(RandomStr(r, 3));
      v.d_zip.assign("123456789");

      Checker::SanityCheckDistrict(&k, &v);

      relation(TPCCTable::District, w).SetupReExec(k.EncodeFromAlloca(large_buf), 0, v.Encode());
    }
  }
  mem::SetThreadLocalAllocAffinity(-1);
  logger->info("District Table loading done.");
}


static uint32_t GetCurrentTimeMillis()
{
  //struct timeval tv;
  //ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
  //return tv.tv_sec * 1000;

  // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
  // for now, we just give each core an increasing number

  static __thread uint32_t tl_hack = 0;
  return tl_hack++;
}

template <>
void Loader<TPCCLoader::Customer>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint w = 1; w <= NumWarehouses(); w++) {
    util::PinToCPU(PartitionId(w));
    mem::SetThreadLocalAllocAffinity(PartitionId(w));

    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      for (uint cidx0 = 0; cidx0 < NumCustomersPerDistrict(); cidx0++) {
	const uint c = cidx0 + 1;
	Customer::Key k(w, d, c);
	Customer::Value v;

	v.c_discount = (float) (RandomNumber(r, 1, 5000) / 10000.0);
	if (RandomNumber(r, 1, 100) <= 10)
	  v.c_credit.assign("BC");
	else
	  v.c_credit.assign("GC");

	if (c <= 1000)
	  v.c_last.assign(GetCustomerLastName(r, c - 1));
	else
	  v.c_last.assign(GetNonUniformCustomerLastNameLoad(r));

	v.c_first.assign(RandomStr(r, RandomNumber(r, 8, 16)));
	v.c_credit_lim = 50000;

	v.c_balance = -10;
	v.c_ytd_payment = 10;
	v.c_payment_cnt = 1;
	v.c_delivery_cnt = 0;

	v.c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
	v.c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
	v.c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
	v.c_state.assign(RandomStr(r, 3));
	v.c_zip.assign(RandomNStr(r, 4) + "11111");
	v.c_phone.assign(RandomNStr(r, 16));
	v.c_since = GetCurrentTimeMillis();
	v.c_middle.assign("OE");
	v.c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

	Checker::SanityCheckCustomer(&k, &v);
	relation(TPCCTable::Customer, w).SetupReExec(k.EncodeFromAlloca(large_buf), 0, v.Encode());

	// customer name index
	CustomerNameIdx::Key k_idx(k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));
	CustomerNameIdx::Value v_idx{k.c_id};

	// index structure is:
	// (c_w_id, c_d_id, c_last, c_first) -> (c_id)

	relation(TPCCTable::CustomerNameIdx, w).SetupReExec(k_idx.EncodeFromAlloca(large_buf), 0,
							    v_idx.Encode());

	History::Key k_hist;

	k_hist.h_c_id = c;
	k_hist.h_c_d_id = d;
	k_hist.h_c_w_id = w;
	k_hist.h_d_id = d;
	k_hist.h_w_id = w;
	k_hist.h_date = GetCurrentTimeMillis();

	History::Value v_hist;
	v_hist.h_amount = 10;
	v_hist.h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

	relation(TPCCTable::History, w).SetupReExec(k_hist.EncodeFromAlloca(large_buf), 0, v_hist.Encode());
      }
    }
  }
  mem::SetThreadLocalAllocAffinity(-1);
}

static size_t NumOrderLinesPerCustomer(util::FastRandom &r)
{
  return RandomNumber(r, 5, 15);
}

template <>
void Loader<TPCCLoader::Order>::DoLoad()
{
  void *large_buf = alloca(1024);
  for (uint w = 1; w <= NumWarehouses(); w++) {
    util::PinToCPU(PartitionId(w));
    mem::SetThreadLocalAllocAffinity(PartitionId(w) % dolly::Epoch::kNrThreads);;
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      std::set<uint> c_ids_s;
      std::vector<uint> c_ids;

      while (c_ids.size() != NumCustomersPerDistrict()) {
	const auto x = (r.next() % NumCustomersPerDistrict()) + 1;
	if (c_ids_s.count(x))
	  continue;
	c_ids_s.insert(x);
	c_ids.emplace_back(x);
      }

      for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
	const OOrder::Key k_oo(w, d, c);

	OOrder::Value v_oo;

	v_oo.o_c_id = c_ids[c - 1];
	if (k_oo.o_id < 2101)
	  v_oo.o_carrier_id = RandomNumber(r, 1, 10);
	else
	  v_oo.o_carrier_id = 0;
	v_oo.o_ol_cnt = NumOrderLinesPerCustomer(r);
	if (g_microbench_simple)
	  v_oo.o_ol_cnt--;  // make room for the insert
	v_oo.o_all_local = 1;
	v_oo.o_entry_d = GetCurrentTimeMillis();

	Checker::SanityCheckOOrder(&k_oo, &v_oo);

	relation(TPCCTable::OOrder, w).SetupReExec(k_oo.EncodeFromAlloca(large_buf), 0, v_oo.Encode());

	const OOrderCIdIdx::Key k_oo_idx(k_oo.o_w_id, k_oo.o_d_id, v_oo.o_c_id, k_oo.o_id);
	OOrderCIdIdx::Value v_oo_idx{0};

	relation(TPCCTable::OOrderCIdIdx, w).SetupReExec(k_oo_idx.EncodeFromAlloca(large_buf), 0, v_oo_idx.Encode());

	if (c >= 2101) {
	  NewOrder::Key k_no(w, d, c);
	  NewOrder::Value v_no;

	  Checker::SanityCheckNewOrder(&k_no, &v_no);

	  relation(TPCCTable::NewOrder, w).SetupReExec(k_no.EncodeFromAlloca(large_buf), 0, v_no.Encode());
	}

	for (uint l = 1; l <= uint(v_oo.o_ol_cnt); l++) {
	  const OrderLine::Key k_ol(w, d, c, l);

	  OrderLine::Value v_ol;
	  v_ol.ol_i_id = RandomNumber(r, 1, 100000);
	  if (k_ol.ol_o_id < 2101) {
	    v_ol.ol_delivery_d = v_oo.o_entry_d;
	    v_ol.ol_amount = 0;
	  } else {
	    v_ol.ol_delivery_d = 0;
	    // random within [0.01 .. 9,999.99]
	    v_ol.ol_amount = (float) (RandomNumber(r, 1, 999999) / 100.0);
	  }

	  v_ol.ol_supply_w_id = k_ol.ol_w_id;
	  v_ol.ol_quantity = 5;
	  // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
	  //v_ol.ol_dist_info = RandomStr(r, 24);

	  Checker::SanityCheckOrderLine(&k_ol, &v_ol);
	  relation(TPCCTable::OrderLine, w).SetupReExec(k_ol.EncodeFromAlloca(large_buf), 0, v_ol.Encode());
	}
      }
    }
  }
  mem::SetThreadLocalAllocAffinity(-1);
}

}
}

namespace dolly {

using namespace tpcc;

// we specialize the following template classess
//
// Request<util::MixIn<tpcc::NewOrderStruct, TPCCMixIn>>
// Request<util::MixIn<tpcc::DeliveryStruct, TPCCMixIn>>
// Request<util::MixIn<tpcc::CreditCheckStruct, TPCCMixIn>>
// Request<util::MixIn<tpcc::PaymentStruct, TPCCMixIn>>
//
// We only implement the RunTxn() method. ParseFromBuffer() method are implemented
// under tpcc_workload.cc, which is the main entrance of this workload support
// module.

template<>
void Request<MixIn<tpcc::NewOrderStruct, tpcc::TPCCMixIn>>::RunTxn()
{
  bool all_local = true;

  for (uint i = 0; i < nr_items; i++) {
    if (supplier_warehouse_id[i] != warehouse_id) all_local = false;
  }

  assert(!g_disable_xpartition_txn || all_local);

  ScopedMultilock<std::mutex> mlock;
  if (g_enable_partition_locks) {
    if (all_local) {
      mlock.enq(LockForPartition(warehouse_id));
    } else {
      std::set<unsigned int> lockset;
      mlock.enq(LockForPartition(warehouse_id));
      lockset.insert(PartitionId(warehouse_id));

      for (uint i = 0; i < nr_items; i++) {
        if (lockset.find(PartitionId(supplier_warehouse_id[i])) == lockset.end()) {
          mlock.enq(LockForPartition(supplier_warehouse_id[i]));
          lockset.insert(PartitionId(supplier_warehouse_id[i]));
        }
      }
    }
    mlock.multilock();
  }

  // large_buf for the key
  void *large_buf = alloca(1024);
  TxnValidator validator;
  CommitBuffer buffer(this);

  const Customer::Key k_c(warehouse_id, district_id, customer_id);
  auto v_c = relation(TPCCTable::Customer, warehouse_id)
    .Get<Customer::Value>(k_c.EncodeFromAlloca(large_buf),
			  serializable_id(),
			  buffer);
  Checker::SanityCheckCustomer(&k_c, &v_c);

  const Warehouse::Key k_w(warehouse_id);
  auto v_w = relation(TPCCTable::Warehouse, warehouse_id)
    .Get<Warehouse::Value>(k_w.EncodeFromAlloca(large_buf),
			   serializable_id(),
			   buffer);

  Checker::SanityCheckWarehouse(&k_w, &v_w);

  const District::Key k_d(warehouse_id, district_id);
  auto v_d = relation(TPCCTable::District, warehouse_id)
    .Get<District::Value>(k_d.EncodeFromAlloca(large_buf),
			  serializable_id(),
			  buffer);

  Checker::SanityCheckDistrict(&k_d, &v_d);

  const uint64_t my_next_o_id = v_d.d_next_o_id;

  const NewOrder::Key k_no(warehouse_id, district_id, my_next_o_id);
  const NewOrder::Value v_no{sql::inline_str_fixed<12>()}; // this is a dummy

  logger->debug("NewOrder sid {} Put New Order {} {} {}", serializable_id(),
		k_no.no_w_id, k_no.no_d_id, k_no.no_o_id);
  relation(TPCCTable::NewOrder, warehouse_id)
    .Put(k_no.Encode(), serializable_id(), v_no.Encode(), buffer);

  District::Value v_d_new(v_d);
  v_d_new.d_next_o_id++;
  relation(TPCCTable::District, warehouse_id)
    .Put(k_d.Encode(), serializable_id(), v_d_new.Encode(), buffer);

  const OOrder::Key k_oo(warehouse_id, district_id, k_no.no_o_id);
  const OOrder::Value v_oo {customer_id, 0, (uint8_t) nr_items, all_local, ts_now};

  relation(TPCCTable::OOrder, warehouse_id)
    .Put(k_oo.Encode(), serializable_id(), v_oo.Encode(), buffer);

  const OOrderCIdIdx::Key k_oo_idx(warehouse_id, district_id, customer_id, k_no.no_o_id);
  const OOrderCIdIdx::Value v_oo_idx{0};
  relation(TPCCTable::OOrderCIdIdx, warehouse_id)
    .Put(k_oo_idx.Encode(), serializable_id(), v_oo_idx.Encode(), buffer);

  for (uint ol_number = 1; ol_number <= nr_items; ol_number++) {
    const uint ol_supply_w_id = supplier_warehouse_id[ol_number - 1];
    const uint ol_i_id = item_id[ol_number - 1];
    const uint ol_quantity = order_quantities[ol_number - 1];

    const Item::Key k_i(ol_i_id);
    auto v_i = relation(TPCCTable::Item, 1)
      .Get<Item::Value>(k_i.EncodeFromAlloca(large_buf),
			serializable_id(),
			buffer);
    Checker::SanityCheckItem(&k_i, &v_i);

    const Stock::Key k_s(ol_supply_w_id, ol_i_id);
    auto v_s = relation(TPCCTable::Stock, ol_supply_w_id)
      .Get<Stock::Value>(k_s.EncodeFromAlloca(large_buf),
			 serializable_id(),
			 buffer);
    Checker::SanityCheckStock(&k_s, &v_s);

    Stock::Value v_s_new(v_s);
    if (v_s_new.s_quantity - ol_quantity >= 10)
      v_s_new.s_quantity -= ol_quantity;
    else
      v_s_new.s_quantity += -int32_t(ol_quantity) + 91;
    v_s_new.s_ytd += ol_quantity;
    v_s_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

    logger->debug("NewOrder sid {} Put Stock {} {}", serializable_id(), ol_supply_w_id, ol_i_id);
    relation(TPCCTable::Stock, ol_supply_w_id)
      .Put(k_s.Encode(), serializable_id(), v_s_new.Encode(), buffer);

    const OrderLine::Key k_ol(warehouse_id, district_id, k_no.no_o_id, ol_number);
    const OrderLine::Value v_ol (ol_i_id, 0,
				 (float) ol_quantity * v_i.i_price,
				 ol_supply_w_id, (uint8_t) ol_quantity);

    logger->debug("NewOrder sid {} Put OrderLine {} {} {} {}", serializable_id(),
		  k_ol.ol_w_id, k_ol.ol_d_id, k_ol.ol_o_id, k_ol.ol_number);

    relation(TPCCTable::OrderLine, warehouse_id)
      .Put(k_ol.Encode(), serializable_id(), v_ol.Encode(), buffer);
  }

  buffer.Commit(serializable_id(), &validator);
}

template<>
void Request<MixIn<tpcc::DeliveryStruct, tpcc::TPCCMixIn>>::RunTxn()
{
  // worst case txn profile:
  //   10 times:
  //     1 new_order scan node
  //     1 oorder get
  //     2 order_line scan nodes
  //     15 order_line puts
  //     1 new_order remove
  //     1 oorder put
  //     1 customer get
  //     1 customer put
  //
  // output from counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 21
  //   max_read_set_size : 133
  //   max_write_set_size : 133
  //   num_txn_contexts : 4
  ScopedLockGuard<std::mutex> slock(
    g_enable_partition_locks ? &LockForPartition(warehouse_id) : nullptr);
  void *large_buf = alloca(1024);
  TxnValidator validator;
  CommitBuffer buffer(this);

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    logger->debug("Delivery sid {} process district {} last oid {}", serializable_id(), d, last_no_o_ids[d - 1]);
    const NewOrder::Key k_no_0(warehouse_id, d, last_no_o_ids[d - 1]);
    const NewOrder::Key k_no_1(warehouse_id, d, std::numeric_limits<int32_t>::max());
    NewOrder::Key k_no;
    bool k_no_found = false;

    // scan on the index
    relation(TPCCTable::NewOrder, warehouse_id)
      .Scan(k_no_0.EncodeFromAlloca(large_buf),
	    k_no_1.EncodeFromAlloca((uint8_t *) large_buf + k_no_0.EncodeSize() + sizeof(VarStr) + 1),
	    serializable_id(),
	    buffer,
	    [&k_no, &k_no_found] (const VarStr *k, const VarStr *v) -> bool {
	      k_no.DecodeFrom(k);
	      k_no_found = true;
	      NewOrder::Value v_no;
	      v_no.DecodeFrom(v);

	      Checker::SanityCheckNewOrder(&k_no, &v_no);
	      return false;
	    });

    if (unlikely(!k_no_found))
      continue;

    // last_no_o_ids[d - 1] = k_no.no_o_id + 1; // XXX: update last seen

    const OOrder::Key k_oo(warehouse_id, d, k_no.no_o_id);
    // even if we read the new order entry, there's no guarantee
    // we will read the oorder entry: in this case the txn will abort,
    // but we're simply bailing out early
    auto v_oo = relation(TPCCTable::OOrder, warehouse_id)
      .Get<OOrder::Value>(k_oo.EncodeFromAlloca(large_buf),
			  serializable_id(), buffer);
    Checker::SanityCheckOOrder(&k_oo, &v_oo);

    // static_limit_callback<15> c(s_arena.get(), false); // never more than 15 order_lines per order
    const OrderLine::Key k_oo_0(warehouse_id, d, k_no.no_o_id, 0);
    const OrderLine::Key k_oo_1(warehouse_id, d, k_no.no_o_id, std::numeric_limits<int32_t>::max());

    float sum = 0.0;
    auto it = relation(TPCCTable::OrderLine, warehouse_id)
      .SearchIterator(k_oo_0.EncodeFromAlloca(large_buf),
		      k_oo_1.EncodeFromAlloca((uint8_t *) large_buf + k_oo_0.EncodeSize()
					      + sizeof(VarStr) + 1),
		      serializable_id(), buffer);

    for (size_t i = 0; it.IsValid() && i < 15; i++, it.Next(serializable_id(), buffer)) {
      const auto v_ol = it.object()->ToType<OrderLine::Value>();

// #ifdef CHECK_INVARIANTS
      OrderLine::Key k_ol;
      k_ol.DecodeFrom(&it.key());

      Checker::SanityCheckOrderLine(&k_ol, &v_ol);
// #endif

      sum += v_ol.ol_amount;
      OrderLine::Value v_ol_new(v_ol);
      v_ol_new.ol_delivery_d = ts;

      logger->debug("Delivery sid {} Put OrderLine {} {} {} {}", serializable_id(),
		    k_ol.ol_w_id, k_ol.ol_d_id, k_ol.ol_o_id, k_ol.ol_number);
      relation(TPCCTable::OrderLine, warehouse_id)
	.Put(k_ol.Encode(), serializable_id(), v_ol_new.Encode(), buffer);
    }

    // delete new order on k_no
    // we just insert a nullptr?
    // TODO: double check if someone is reading/scanning this item!
    // -Mike
    logger->debug("Delivery sid {} Delete NewOrder {} {} {}", serializable_id(), k_no.no_w_id, k_no.no_d_id, k_no.no_o_id);
    relation(TPCCTable::NewOrder, warehouse_id)
      .Put(k_no.Encode(), serializable_id(), nullptr, buffer);

    logger->debug("Delivery sid {} Put OOrder {} {} {}", serializable_id(), k_oo.o_w_id, k_oo.o_d_id, k_oo.o_id);
    // update oorder
    OOrder::Value v_oo_new(v_oo);
    v_oo_new.o_carrier_id = o_carrier_id;
    relation(TPCCTable::OOrder, warehouse_id)
      .Put(k_oo.Encode(), serializable_id(), v_oo_new.Encode(), buffer);

    const uint c_id = v_oo.o_c_id;
    const float ol_total = sum;

    // update customer
    const Customer::Key k_c(warehouse_id, d, c_id);
    const auto v_c = relation(TPCCTable::Customer, warehouse_id)
      .Get<Customer::Value>(k_c.EncodeFromAlloca(large_buf),
			    serializable_id(),
			    buffer);

    logger->debug("Delivery sid {} Put Customer {} {} {}", serializable_id(), warehouse_id, d, c_id);
    Customer::Value v_c_new(v_c);
    v_c_new.c_balance += ol_total;
    relation(TPCCTable::Customer, warehouse_id)
      .Put(k_c.Encode(), serializable_id(), v_c_new.Encode(), buffer);
  }
  buffer.Commit(serializable_id(), &validator);
}

template<>
void Request<MixIn<tpcc::CreditCheckStruct, tpcc::TPCCMixIn>>::RunTxn()
{
  /*
    Note: Cahill's credit check transaction to introduce SI's anomaly.

    SELECT c_balance, c_credit_lim
    INTO :c_balance, :c_credit_lim
    FROM Customer
    WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id

    SELECT SUM(ol_amount) INTO :neworder_balance
    FROM OrderLine, Orders, NewOrder
    WHERE ol_o_id = o_id AND ol_d_id = :d_id
    AND ol_w_id = :w_id AND o_d_id = :d_id
    AND o_w_id = :w_id AND o_c_id = :c_id
    AND no_o_id = o_id AND no_d_id = :d_id
    AND no_w_id = :w_id

    if (c_balance + neworder_balance > c_credit_lim)
    c_credit = "BC";
    else
    c_credit = "GC";

    SQL UPDATE Customer SET c_credit = :c_credit
    WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id
  */
  assert(!g_disable_xpartition_txn || customer_warehouse_id == warehouse_id);

  ScopedMultilock<std::mutex> mlock;
  if (g_enable_partition_locks) {
    mlock.enq(LockForPartition(warehouse_id));
    if (PartitionId(customer_warehouse_id) != PartitionId(warehouse_id))
      mlock.enq(LockForPartition(customer_warehouse_id));
    mlock.multilock();
  }

  TxnValidator validator;
  CommitBuffer buffer(this);

  uint8_t *large_buf = (uint8_t*) alloca(2048);

  logger->debug("CC txn, sid {}, whid {} did {} cwid {} cdid {} cid {}", serializable_id(),
		warehouse_id, district_id, customer_warehouse_id,
		customer_district_id, customer_id);

  // select * from customer with random C_ID
  Customer::Key k_c(customer_warehouse_id, customer_district_id, customer_id);
  const auto v_c = relation(TPCCTable::Customer, customer_warehouse_id)
    .Get<Customer::Value>(k_c.EncodeFromAlloca(large_buf),
			  serializable_id(),
			  buffer);
  Checker::SanityCheckCustomer(&k_c, &v_c);

  // scan order
  //		c_w_id = :w_id;
  //		c_d_id = :d_id;
  //		c_id = :c_id;
  NewOrder::Key k_no_0(warehouse_id, district_id, 0);
  NewOrder::Key k_no_1(warehouse_id, district_id, std::numeric_limits<int32_t>::max());

  auto it = relation(TPCCTable::NewOrder, warehouse_id)
    .SearchIterator(
      k_no_0.EncodeFromAlloca(large_buf),
      k_no_1.EncodeFromAlloca(large_buf + k_no_0.EncodeSize() + sizeof(VarStr) + 1),
      serializable_id(), buffer);

  large_buf += k_no_0.EncodeSize() + k_no_1.EncodeSize() + sizeof(VarStr) * 2 + 2;

  double sum = 0;
  for (; it.IsValid(); it.Next(serializable_id(), buffer))
  {
    NewOrder::Key k_no;
    k_no.DecodeFrom(&it.key());

    OOrder::Key k_oo(warehouse_id, district_id, k_no.no_o_id);
    auto v_oo = relation(TPCCTable::OOrder, warehouse_id)
      .Get<OOrder::Value>(k_oo.EncodeFromAlloca(large_buf),
			  serializable_id(),
			  buffer);

    // Order line scan
    //		ol_d_id = :d_id
    //		ol_w_id = :w_id
    //		ol_o_id = o_id
    //		ol_number = 1-15
    const OrderLine::Key k_ol_0(warehouse_id, district_id, k_no.no_o_id, 1);
    const OrderLine::Key k_ol_1(warehouse_id, district_id, k_no.no_o_id, 15);

    relation(TPCCTable::OrderLine, warehouse_id)
      .Scan(
	k_ol_0.EncodeFromAlloca(large_buf),
	k_ol_1.EncodeFromAlloca(large_buf + k_ol_0.EncodeSize() + sizeof(VarStr) + 1),
	serializable_id(),
	buffer,
	[&sum] (const VarStr *k, const VarStr *v) -> bool {
	  OrderLine::Value v_ol;
	  v_ol.DecodeFrom(v);
	  sum += v_ol.ol_amount;
	  return true;
	});
  }

  // c_credit update
  Customer::Value v_c_new(v_c);
  if (v_c_new.c_balance + sum >= 5000) // Threshold = 5K
    v_c_new.c_credit.assign("BC");
  else
    v_c_new.c_credit.assign("GC");

  logger->debug("CC sid {} Put Customer {} {} {}", serializable_id(),
		k_c.c_w_id, k_c.c_d_id, k_c.c_id);
  relation(TPCCTable::Customer, customer_warehouse_id)
    .Put(k_c.Encode(), serializable_id(), v_c_new.Encode(), buffer);

  buffer.Commit(serializable_id(), &validator);
}

template<>
void Request<MixIn<tpcc::PaymentStruct, tpcc::TPCCMixIn>>::RunTxn()
{
  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 10
  //   max_read_set_size : 71
  //   max_write_set_size : 1
  //   num_txn_contexts : 5
  ScopedMultilock<std::mutex> mlock;
  if (g_enable_partition_locks) {
    mlock.enq(LockForPartition(warehouse_id));
    if (PartitionId(customer_warehouse_id) != PartitionId(warehouse_id))
      mlock.enq(LockForPartition(customer_warehouse_id));
    mlock.multilock();
  }

  TxnValidator validator;
  CommitBuffer buffer(this);

  logger->debug("Pay txn sid {} whid {} did {} cwhid {} cdid {}", serializable_id(),
		warehouse_id, district_id, customer_warehouse_id, customer_district_id);

  void *large_buf = alloca(1024);

  const Warehouse::Key k_w(warehouse_id);
  auto v_w = relation(TPCCTable::Warehouse, warehouse_id)
    .Get<Warehouse::Value>(k_w.EncodeFromAlloca(large_buf),
			   serializable_id(),
			   buffer);
  Checker::SanityCheckWarehouse(&k_w, &v_w);

  Warehouse::Value v_w_new(v_w);
  v_w_new.w_ytd += payment_amount;
  relation(TPCCTable::Warehouse, warehouse_id)
    .Put(k_w.Encode(), serializable_id(), v_w_new.Encode(), buffer);

  District::Key k_d(warehouse_id, district_id);
  auto v_d = relation(TPCCTable::District, warehouse_id)
    .Get<District::Value>(k_d.EncodeFromAlloca(large_buf),
			  serializable_id(),
			  buffer);
  Checker::SanityCheckDistrict(&k_d, &v_d);

  District::Value v_d_new(v_d);
  v_d_new.d_ytd += payment_amount;
  relation(TPCCTable::District, warehouse_id)
    .Put(k_d.Encode(), serializable_id(), v_d_new.Encode(), buffer);

  Customer::Key k_c;
  k_c.c_w_id = customer_warehouse_id;
  k_c.c_d_id = customer_district_id;
  Customer::Value v_c;

  if (is_by_name) {
    // cust by name
    uint8_t *lastname_buf = by.lastname_buf;

    static const std::string zeros(16, 0);
    static const std::string ones(16, 255);

    CustomerNameIdx::Key k_c_idx_0;
    k_c_idx_0.c_w_id = customer_warehouse_id;
    k_c_idx_0.c_d_id = customer_district_id;
    k_c_idx_0.c_last.assign((const char *) lastname_buf, 16);
    k_c_idx_0.c_first.assign(zeros);

    CustomerNameIdx::Key k_c_idx_1;
    k_c_idx_1.c_w_id = customer_warehouse_id;
    k_c_idx_1.c_d_id = customer_district_id;
    k_c_idx_1.c_last.assign((const char *) lastname_buf, 16);
    k_c_idx_1.c_first.assign(ones);

    std::vector<uint32_t> customer_ids;
    relation(TPCCTable::CustomerNameIdx, customer_warehouse_id)
      .Scan(
	k_c_idx_0.EncodeFromAlloca(large_buf),
	k_c_idx_1.EncodeFromAlloca((uint8_t *) large_buf + k_c_idx_0.EncodeSize() + sizeof(VarStr) + 1),
	serializable_id(),
	buffer,
	[&customer_ids] (const VarStr *k, const VarStr *v) -> bool {
	  CustomerNameIdx::Value name_idx;
	  name_idx.DecodeFrom(v);
	  customer_ids.push_back(name_idx.c_id);
	  return customer_ids.size() < NMaxCustomerIdxScanElems;
	});

    assert(customer_ids.size() > 0);

    k_c.c_id = customer_ids[(customer_ids.size() - 1) / 2];
    v_c = relation(TPCCTable::Customer, customer_warehouse_id)
      .Get<Customer::Value>(k_c.EncodeFromAlloca(large_buf),
			    serializable_id(),
			    buffer);
  } else {
    // cust by ID
    k_c.c_id = by.customer_id;
    v_c = relation(TPCCTable::Customer, customer_warehouse_id)
      .Get<Customer::Value>(k_c.EncodeFromAlloca(large_buf),
			    serializable_id(),
			    buffer);
  }
  Checker::SanityCheckCustomer(&k_c, &v_c);
  Customer::Value v_c_new(v_c);

  v_c_new.c_balance -= payment_amount;
  v_c_new.c_ytd_payment += payment_amount;
  v_c_new.c_payment_cnt++;

  if (strncmp(v_c.c_credit.data(), "BC", 2) == 0) {
    char buf[501];
    int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s",
		     k_c.c_id,
		     k_c.c_d_id,
		     k_c.c_w_id,
		     district_id,
		     warehouse_id,
		     payment_amount,
		     v_c.c_data.c_str());
    v_c_new.c_data.resize_junk(
      std::min(static_cast<size_t>(n), v_c_new.c_data.max_size()));
    memcpy((void *) v_c_new.c_data.data(), &buf[0], v_c_new.c_data.size());
  }
  relation(TPCCTable::Customer, customer_warehouse_id)
    .Put(k_c.Encode(), serializable_id(), v_c_new.Encode(), buffer);

  History::Key k_h(k_c.c_d_id, k_c.c_w_id, k_c.c_id, district_id, warehouse_id, ts);
  History::Value v_h;
  v_h.h_amount = payment_amount;
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *) v_h.h_data.data(), v_h.h_data.max_size() + 1,
		   "%.10s    %.10s",
		   v_w.w_name.c_str(),
		   v_d.d_name.c_str());
  v_h.h_data.resize_junk(std::min(static_cast<size_t>(n), v_h.h_data.max_size()));
  relation(TPCCTable::History, warehouse_id)
    .Put(k_h.Encode(), serializable_id(), v_h.Encode(), buffer);

  buffer.Commit(serializable_id(), &validator);
}

}

namespace util {

template <>
tpcc::TPCCTableHandles &Instance()
{
  if (tpcc::global_table_handles == nullptr) {
    tpcc::global_table_handles = new tpcc::TPCCTableHandles();
  }
  return *tpcc::global_table_handles;
}

}
