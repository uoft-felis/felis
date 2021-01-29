#include "tpcc.h"

namespace tpcc {

struct CreditCheckStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_warehouse_id;
  uint customer_district_id;
  uint customer_id;
};

template <>
CreditCheckStruct ClientBase::GenerateTransactionInput<CreditCheckStruct>()
{
  CreditCheckStruct s;
  s.warehouse_id = PickWarehouse();
  s.district_id = PickDistrict();
  if (nr_warehouses() == 1
      || RandomNumber(1, 100) > int(kCreditCheckRemoteCustomer * 100)) {
    s.customer_warehouse_id = s.warehouse_id;
    s.customer_district_id = s.district_id;
  } else {
    s.customer_warehouse_id = RandomNumberExcept(1, nr_warehouses(), s.warehouse_id);
    s.customer_district_id = PickDistrict();
  }
  s.customer_id = GetCustomerId();
  return s;
}


}
