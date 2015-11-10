// -*- c++ -*-

#ifndef REQUEST_H
#define REQUEST_H

#include "net-io.h"
#include <map>
#include <functional>

namespace db_backup {

class BaseRequest {
  static const uint8_t kTPCCNewOrderTx;
  static const uint8_t kTPCCDeliveryTx;
  static const uint8_t kTPCCCreditCheckTx;
  static const uint8_t kTPCCPaymentTx;
  static const uint8_t kMaxType;

  typedef std::map<uint8_t, std::function<BaseRequest* (uint8_t)> > FactoryMap;
  static const FactoryMap kFactoryMap;

public:
  static BaseRequest *CreateRequestFromBuffer(ParseBuffer &buffer);

  virtual ~BaseRequest() {}
  virtual void ParseFromBuffer(ParseBuffer &buffer) = 0;
  virtual void Process() = 0;
};

template <class T>
class Request : public BaseRequest, public T {
  virtual void ParseFromBuffer(ParseBuffer &buffer);
  virtual void Process();
};

namespace tpcc {

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

}

}

#endif /* REQUEST_H */
