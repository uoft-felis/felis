#include "tpcc.h"

namespace tpcc {

struct DeliveryStruct {
  uint warehouse_id;
  uint o_carrier_id;
  uint32_t ts;

  int32_t last_no_o_ids[10]; // XXX(Mike): array of 10 integers, unhack!
};


template <>
DeliveryStruct TPCCClient::GenerateTransactionInput<DeliveryStruct>()
{
  DeliveryStruct s;
  s.warehouse_id = PickWarehouse();
  s.o_carrier_id = PickDistrict();
  s.ts = GetCurrentTime();

  // XXX: hack
  memcpy(s.last_no_o_ids, last_no_o_ids, sizeof(int) * 10);
  return s;
}

}
