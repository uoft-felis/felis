#include "request.h"
#include "log.h"
#include <cassert>

namespace db_backup {

template<>
void Request<tpcc::NewOrderStruct>::ParseFromBuffer(ParseBuffer &buffer)
{
  buffer.Read(&warehouse_id, 4);
  buffer.Read(&district_id, 4);
  buffer.Read(&customer_id, 4);
  buffer.Read(&nr_items, 4);
  buffer.Read(&ts_now, 4);

  for (uint i = 0; i < nr_items; i++) {
    buffer.Read(&item_id[i], sizeof(uint));
    buffer.Read(&supplier_warehouse_id[i], sizeof(uint));
    buffer.Read(&order_quantities[i], sizeof(uint));
  }
}

template<>
void Request<tpcc::DeliveryStruct>::ParseFromBuffer(ParseBuffer &buffer)
{
  buffer.Read(&warehouse_id, 4);
  buffer.Read(&o_carrier_id, 4);
  buffer.Read(&ts, 4);

  for (int i = 0; i < 10; i++) {
    buffer.Read(&last_no_o_ids[i], sizeof(int32_t));
  }
}

template<>
void Request<tpcc::CreditCheckStruct>::ParseFromBuffer(db_backup::ParseBuffer &buffer)
{
  buffer.Read(&warehouse_id, 4);
  buffer.Read(&district_id, 4);
  buffer.Read(&customer_warehouse_id, 4);
  buffer.Read(&customer_district_id, 4);
  buffer.Read(&customer_id, 4);
}

template<>
void Request<tpcc::PaymentStruct>::ParseFromBuffer(ParseBuffer &buffer)
{
  buffer.Read(&warehouse_id, 4);
  buffer.Read(&district_id, 4);
  buffer.Read(&customer_warehouse_id, 4);
  buffer.Read(&customer_district_id, 4);
  buffer.Read(&payment_amount, 4);
  buffer.Read(&ts, 4);
  buffer.Read(&is_by_name, 1);
  buffer.Read(&by, 16);
}

template<>
void Request<tpcc::NewOrderStruct>::Process()
{

}

template<>
void Request<tpcc::DeliveryStruct>::Process()
{

}

template<>
void Request<tpcc::CreditCheckStruct>::Process()
{

}

template<>
void Request<tpcc::PaymentStruct>::Process()
{

}

const uint8_t BaseRequest::kTPCCNewOrderTx = 1;
const uint8_t BaseRequest::kTPCCDeliveryTx = 2;
const uint8_t BaseRequest::kTPCCCreditCheckTx = 3;
const uint8_t BaseRequest::kTPCCPaymentTx = 4;
const uint8_t BaseRequest::kMaxType = 5;

const BaseRequest::FactoryMap BaseRequest::kFactoryMap = {
  {kTPCCNewOrderTx, [] (uint8_t type) {return new Request<tpcc::NewOrderStruct>;}},
  {kTPCCDeliveryTx, [] (uint8_t type) {return new Request<tpcc::DeliveryStruct>;}},
  {kTPCCCreditCheckTx, [] (uint8_t type) {return new Request<tpcc::CreditCheckStruct>;}},
  {kTPCCPaymentTx, [] (uint8_t type) {return new Request<tpcc::PaymentStruct>;}},
};

BaseRequest *BaseRequest::CreateRequestFromBuffer(ParseBuffer &buffer)
{
  uint8_t type = 0;
  buffer.Read(&type, 1);
  assert(type != 0);
  assert(type < kMaxType);
  logger->debug("txn req type {0:d}", type);
  auto req = kFactoryMap.at(type)(type);
  req->ParseFromBuffer(buffer);
  return req;
}

}
