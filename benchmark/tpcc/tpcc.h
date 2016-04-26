// -*- c++ -*-

#ifndef TPCC_H
#define TPCC_H

#include <map>
#include <array>
#include <string>
#include <vector>
#include <cassert>

#include "worker.h"
#include "index.h"
#include "util.h"
#include "sqltypes.h"

namespace tpcc {

// TODO: I wonder if we can use boost::spirit to create a DSL?

// table and schemas definition
struct Customer {

  struct KeyStruct {
    uint32_t c_w_id;
    uint32_t c_d_id;
    uint32_t c_id;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t, uint32_t, uint32_t> Key;

  struct ValueStruct {
    float c_discount;
    sql::inline_str_fixed< 2> c_credit;
    sql::inline_str_8<16> c_last;
    sql::inline_str_8<16> c_first;
    float c_credit_lim;
    float c_balance;
    float c_ytd_payment;
    int32_t c_payment_cnt;
    int32_t c_delivery_cnt;
    sql::inline_str_8<20> c_street_1;
    sql::inline_str_8<20> c_street_2;
    sql::inline_str_8<20> c_city;
    sql::inline_str_fixed<2> c_state;
    sql::inline_str_fixed<9> c_zip;
    sql::inline_str_fixed<16> c_phone;
    uint32_t c_since;
    sql::inline_str_fixed<2> c_middle;
    sql::inline_str_16<500> c_data;
  } __attribute__((packed));

  typedef sql::Schemas<ValueStruct,
		       float,
		       sql::inline_str_fixed<2>,
		       sql::inline_str_base<uint8_t, 16>,
		       sql::inline_str_base<uint8_t, 16>,
		       float, float, float,
		       sql::hint32, sql::hint32,
		       sql::inline_str_base<uint8_t, 20>,
		       sql::inline_str_base<uint8_t, 20>,
		       sql::inline_str_base<uint8_t, 20>,
		       sql::inline_str_fixed<2>,
		       sql::inline_str_fixed<9>,
		       sql::inline_str_fixed<16>,
		       sql::hint32,
		       sql::inline_str_fixed<2>,
		       sql::inline_str_base<sql::hint16, 500>> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");
};


struct CustomerNameIdx {

  struct KeyStruct {
    uint32_t c_w_id;
    uint32_t c_d_id;
    sql::inline_str_fixed<16> c_last;
    sql::inline_str_fixed<16> c_first;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct,
		       uint32_t,
		       uint32_t,
		       sql::inline_str_fixed<16>,
		       sql::inline_str_fixed<16>> Key;

  struct ValueStruct {
    uint32_t c_id;
  } __attribute__((packed));

  typedef sql::Schemas<ValueStruct,
		       sql::hint32> Value;

};

struct District {

  struct KeyStruct {
    uint32_t d_w_id;
    uint32_t d_id;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t, uint32_t> Key;

  struct ValueStruct {
    float d_ytd;
    float d_tax;
    uint32_t d_next_o_id;
    sql::inline_str_8<10> d_name;
    sql::inline_str_8<20> d_street_1;
    sql::inline_str_8<20> d_street_2;
    sql::inline_str_8<20> d_city;
    sql::inline_str_fixed<2> d_state;
    sql::inline_str_fixed<9> d_zip;
  } __attribute__((packed));

  typedef sql::Schemas<ValueStruct,
		       float, float,
		       sql::hint32,
		       sql::inline_str_base<uint8_t, 10>,
		       sql::inline_str_base<uint8_t, 20>,
		       sql::inline_str_base<uint8_t, 20>,
		       sql::inline_str_base<uint8_t, 20>,
		       sql::inline_str_fixed<2>,
		       sql::inline_str_fixed<9>> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

struct History {

  struct KeyStruct {
    uint32_t h_c_id;
    uint32_t h_c_d_id;
    uint32_t h_c_w_id;
    uint32_t h_d_id;
    uint32_t h_w_id;
    uint32_t h_date;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct,
		       uint32_t,
		       uint32_t,
		       uint32_t,
		       uint32_t,
		       uint32_t,
		       uint32_t> Key;

  struct ValueStruct {
    float h_amount;
    sql::inline_str_8<24> h_data;
  } __attribute__((packed));

  typedef sql::Schemas<ValueStruct,
		       float, sql::inline_str_base<uint8_t, 24>> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

struct Item {

  struct KeyStruct {
    uint32_t i_id;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t> Key;

  struct ValueStruct {
    sql::inline_str_8<24> i_name;
    float i_price;
    sql::inline_str_8<50> i_data;
    int32_t i_im_id;
  } __attribute__((packed));

  typedef sql::Schemas<ValueStruct,
		       sql::inline_str_base<uint8_t, 24>,
		       float,
		       sql::inline_str_base<uint8_t, 50>,
		       sql::hint32> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

struct NewOrder {

  struct KeyStruct {
    uint32_t no_w_id;
    uint32_t no_d_id;
    uint32_t no_o_id;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t, uint32_t, uint32_t> Key;

  struct ValueStruct {
    sql::inline_str_fixed<12> no_dummy;
  } __attribute__((packed));

  typedef sql::Schemas<ValueStruct, sql::inline_str_fixed<12>> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

struct OOrder {

  struct KeyStruct {
    uint32_t o_w_id;
    uint32_t o_d_id;
    uint32_t o_id;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t, uint32_t, uint32_t> Key;

  struct ValueStruct {
    uint32_t o_c_id;
    uint32_t o_carrier_id;
    uint8_t o_ol_cnt;
    bool o_all_local;
    uint32_t o_entry_d;
  } __attribute__((packed)); // 8 + 1 + 4 + 4 = 17

  typedef sql::Schemas<ValueStruct, sql::hint32, sql::hint32, uint8_t, bool, sql::hint32> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

struct OOrderCIdIdx {

  struct KeyStruct {
    uint32_t o_w_id;
    uint32_t o_d_id;
    uint32_t o_c_id;
    uint32_t o_o_id;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t, uint32_t, uint32_t, uint32_t> Key;

  struct ValueStruct {
    uint8_t o_dummy;
  } __attribute__((packed));

  typedef sql::Schemas<ValueStruct, uint8_t> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

struct OrderLine {

  struct KeyStruct {
    uint32_t ol_w_id;
    uint32_t ol_d_id;
    uint32_t ol_o_id;
    uint32_t ol_number;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t, uint32_t, uint32_t, uint32_t> Key;

  struct ValueStruct {
    int32_t ol_i_id;
    uint32_t ol_delivery_d;
    float ol_amount;
    int32_t ol_supply_w_id;
    uint8_t ol_quantity;
  } __attribute__((packed)); // 8 + 4 + 4 + 1 = 17

  typedef sql::Schemas<ValueStruct,
		       sql::hint32,
		       sql::hint32,
		       float,
		       sql::hint32,
		       uint8_t> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

struct Stock {

  struct KeyStruct {
    uint32_t s_w_id;
    uint32_t s_i_id;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t, uint32_t> Key;

  struct ValueStruct {
    int16_t s_quantity;
    float s_ytd;
    int32_t s_order_cnt;
    int32_t s_remote_cnt;
  } __attribute__((packed)); // 2 + 4 + 8 = 14

  typedef sql::Schemas<ValueStruct,
		       sql::hint16,
		       float,
		       sql::hint32,
		       sql::hint32> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

struct StockData {

  struct KeyStruct {
    uint32_t s_w_id;
    uint32_t s_i_id;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t, uint32_t> Key;

  struct ValueStruct {
    sql::inline_str_8<50> s_data;
    sql::inline_str_fixed<24> s_dist_01;
    sql::inline_str_fixed<24> s_dist_02;
    sql::inline_str_fixed<24> s_dist_03;
    sql::inline_str_fixed<24> s_dist_04;
    sql::inline_str_fixed<24> s_dist_05;
    sql::inline_str_fixed<24> s_dist_06;
    sql::inline_str_fixed<24> s_dist_07;
    sql::inline_str_fixed<24> s_dist_08;
    sql::inline_str_fixed<24> s_dist_09;
    sql::inline_str_fixed<24> s_dist_10;
  } __attribute__((packed));

  typedef sql::Schemas<ValueStruct,
		       sql::inline_str_base<uint8_t, 50>,
		       sql::inline_str_fixed<24>,
		       sql::inline_str_fixed<24>,
		       sql::inline_str_fixed<24>,
		       sql::inline_str_fixed<24>,
		       sql::inline_str_fixed<24>,
		       sql::inline_str_fixed<24>,
		       sql::inline_str_fixed<24>,
		       sql::inline_str_fixed<24>,
		       sql::inline_str_fixed<24>,
		       sql::inline_str_fixed<24>> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

struct Warehouse {

  struct KeyStruct {
    uint32_t w_id;
  } __attribute__((packed));

  typedef sql::Schemas<KeyStruct, uint32_t> Key;

  struct ValueStruct {
    float w_ytd;
    float w_tax;
    sql::inline_str_8<10> w_name;
    sql::inline_str_8<20> w_street_1;
    sql::inline_str_8<20> w_street_2;
    sql::inline_str_8<20> w_city;
    sql::inline_str_fixed<2> w_state;
    sql::inline_str_fixed<9> w_zip;
  } __attribute__((packed)); // 78+8+11 = 97

  typedef sql::Schemas<ValueStruct,
		       float, float,
		       sql::inline_str_base<uint8_t, 10>,
		       sql::inline_str_base<uint8_t, 20>,
		       sql::inline_str_base<uint8_t, 20>,
		       sql::inline_str_base<uint8_t, 20>,
		       sql::inline_str_fixed<2>,
		       sql::inline_str_fixed<9>> Value;

  static_assert(Key::DecodeSize() == sizeof(KeyStruct), "SQL Schemas inconsistent");
  static_assert(Value::DecodeSize() == sizeof(ValueStruct), "SQL Schemas inconsistent");

};

enum struct TPCCTable : int { Customer, CustomerNameIdx, District, History, Item,
    NewOrder, OOrder, OOrderCIdIdx, OrderLine, Stock, StockData, Warehouse,
    NRTable };

static const char *kTPCCTableNames[] = {
  "customer",
  "customer_name_idx",
  "district",
  "history",
  "item",
  "new_order",
  "oorder",
  "oorder_c_id_idx",
  "order_line",
  "stock",
  "stock_data",
  "warehouse",
};

class TPCCMixIn {
protected:
  dolly::Relation &relation(TPCCTable table, unsigned int wid);
};


class TPCCTableHandles {
  int table_handles[dolly::RelationManager::kMaxNrRelations];
public:
  TPCCTableHandles();
  int table_handle(int idx) const {
    assert(idx < dolly::RelationManager::kMaxNrRelations);
    return table_handles[idx];
  }

  void InitiateTable(TPCCTable table);
};


// loaders for each table
namespace loaders {

enum TPCCLoader {
  Warehouse, Item, Stock, District, Customer, Order
};

template <enum TPCCLoader TLN>
class Loader : public tpcc::TPCCMixIn {
  util::FastRandom r;
public:
  Loader(unsigned long seed) : r(seed) {}
  void operator()();
};

}

// input parameters
struct NewOrderStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_id;
  uint nr_items;
  uint ts_now;

  uint item_id[15];
  uint supplier_warehouse_id[15];
  uint order_quantities[15];
};

struct DeliveryStruct {
  uint warehouse_id;
  uint o_carrier_id;
  uint32_t ts;

  int32_t last_no_o_ids[10]; // XXX(Mike): array of 10 integers, unhack!
};

struct CreditCheckStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_warehouse_id;
  uint customer_district_id;
  uint customer_id;
};

struct PaymentStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_warehouse_id;
  uint customer_district_id;
  float payment_amount;
  uint32_t ts;
  bool is_by_name; // notice, there is an internal branch
  union {
    uint8_t lastname_buf[16];
    uint customer_id;
  } by;
};

////////////////////////////////////////////////////////////////////////////////

// configuration flags
extern int g_disable_xpartition_txn;
extern int g_disable_read_only_scans;
extern int g_enable_partition_locks;
extern int g_enable_separate_tree_per_partition;
extern int g_new_order_remote_item_pct;
// extern int g_new_order_fast_id_gen;
extern int g_uniform_item_dist;
extern int g_order_status_scan_hack;
extern int g_microbench_static;
extern int g_microbench_simple;
extern int g_microbench_random;
extern int g_wh_temperature;
extern uint g_microbench_rows;

extern double g_microbench_wr_ratio;
extern uint g_microbench_wr_rows;

extern double g_wh_spread;

extern unsigned g_txn_workload_mix[];
extern size_t g_tpcc_scale_factor;

}

#endif /* TPCC_H */
