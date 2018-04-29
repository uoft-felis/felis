#include "tpcc.h"

namespace tpcc {

struct PaymentStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_warehouse_id;
  uint customer_district_id;
  int payment_amount;
  uint32_t ts;
  bool is_by_name;

  union {
    uint8_t lastname_buf[16];
    uint customer_id;
  } by;
};

template <>
PaymentStruct TPCCClient::GenerateTransactionInput<PaymentStruct>()
{
  PaymentStruct s;
  s.warehouse_id = PickWarehouse();
  s.district_id = PickDistrict();
  if (nr_warehouses() == 1
      || RandomNumber(1, 100) > int(kPaymentRemoteCustomer * 100)) {
    s.customer_warehouse_id = s.warehouse_id;
    s.customer_district_id = s.district_id;
  } else {
    s.customer_warehouse_id = RandomNumberExcept(1, nr_warehouses(), s.warehouse_id);
    s.customer_district_id = PickDistrict();
  }
  s.payment_amount = RandomNumber(100, 500000);
  s.ts = GetCurrentTime();
  s.is_by_name = (RandomNumber(1, 100) <= int(kPaymentByName * 100));
  if (s.is_by_name) {
    memset(s.by.lastname_buf, 0, 16);
    GetNonUniformCustomerLastNameRun(s.by.lastname_buf);
  } else {
    s.by.customer_id = GetCustomerId();
  }
  return s;
}

}
