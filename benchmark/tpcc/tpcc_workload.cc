#include "table_decl.h"

#include "tpcc.h"
#include "epoch.h"
#include "log.h"
#include "util.h"
#include "index.h"
#include "module.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

using util::MixIn;
using util::Instance;

namespace dolly {

template<>
void Request<MixIn<tpcc::NewOrderStruct, tpcc::TPCCMixIn>>::ParseFromChannel(go::TcpInputChannel *channel)
{
  struct {
    uint warehouse_id;
    uint district_id;
    uint customer_id;
    uint nr_items;
    uint ts_now;
  } __attribute__((packed)) header;

  channel->Read(&header, sizeof(header));
  warehouse_id = header.warehouse_id;
  district_id = header.district_id;
  customer_id = header.customer_id;
  nr_items = header.nr_items;
  ts_now = header.ts_now;

  struct {
    uint item_id;
    uint sup_warehouse_id;
    uint order_quantity;
  } __attribute__((packed)) items[nr_items];

  channel->Read(&items, sizeof(uint) * 3 * nr_items);

  for (uint i = 0; i < nr_items; i++) {
    item_id[i] = items[i].item_id;
    supplier_warehouse_id[i] = items[i].sup_warehouse_id;
    order_quantities[i] = items[i].order_quantity;
  }
}

template<>
int Request<MixIn<tpcc::NewOrderStruct, tpcc::TPCCMixIn>>::CoreAffinity() const
{
  return warehouse_id;
}

template<>
void Request<MixIn<tpcc::DeliveryStruct, tpcc::TPCCMixIn>>::ParseFromChannel(go::TcpInputChannel *channel)
{
  struct {
    uint warehouse_id;
    uint o_carrier_id;
    uint32_t ts;
  } __attribute__((packed)) header;
  channel->Read(&header, sizeof(header));
  warehouse_id = header.warehouse_id;
  o_carrier_id = header.o_carrier_id;
  ts = header.ts;
  channel->Read(last_no_o_ids, sizeof(uint32_t) * 10);
}

template<>
int Request<MixIn<tpcc::DeliveryStruct, tpcc::TPCCMixIn>>::CoreAffinity() const
{
  return warehouse_id;
}

template<>
void Request<MixIn<tpcc::CreditCheckStruct, tpcc::TPCCMixIn>>::ParseFromChannel(go::TcpInputChannel *channel)
{
  channel->Read(&warehouse_id, 4);
  channel->Read(&district_id, 4);
  channel->Read(&customer_warehouse_id, 4);
  channel->Read(&customer_district_id, 4);
  channel->Read(&customer_id, 4);
}

template<>
int Request<MixIn<tpcc::CreditCheckStruct, tpcc::TPCCMixIn>>::CoreAffinity() const
{
  return warehouse_id;
}

template<>
void Request<MixIn<tpcc::PaymentStruct, tpcc::TPCCMixIn>>::ParseFromChannel(go::TcpInputChannel *channel)
{
  struct {
    uint warehouse_id;
    uint district_id;
    uint customer_warehouse_id;
    uint customer_district_id;
    int payment_amount;
    uint32_t ts;
    uint8_t is_by_name;
    uint8_t by_buf[16];
  } __attribute__((packed)) header;
  channel->Read(&header, sizeof(header));

  warehouse_id = header.warehouse_id;
  district_id = header.district_id;
  customer_warehouse_id = header.customer_warehouse_id;
  customer_district_id = header.customer_district_id;
  payment_amount = header.payment_amount;
  ts = header.ts;

  is_by_name = header.is_by_name;
  memcpy(&by, header.by_buf, 16);
}

template<>
int Request<MixIn<tpcc::PaymentStruct, tpcc::TPCCMixIn>>::CoreAffinity() const
{
  return warehouse_id;
}


static const uint8_t kTPCCNewOrderTx = 1;
static const uint8_t kTPCCDeliveryTx = 2;
static const uint8_t kTPCCCreditCheckTx = 3;
static const uint8_t kTPCCPaymentTx = 4;
static const uint8_t kMaxType = 5;

static void *AllocBaseRequest(Epoch *e, size_t size)
{
  return e->AllocFromBrk(go::Scheduler::CurrentThreadPoolId() - 1, size);
}

static const BaseRequest::FactoryMap kTPCCFactoryMap = {
  {kTPCCNewOrderTx,
   [] (Epoch *e) {
      return new
      (AllocBaseRequest(e, sizeof(Request<MixIn<tpcc::NewOrderStruct, tpcc::TPCCMixIn>>)))
      Request<MixIn<tpcc::NewOrderStruct, tpcc::TPCCMixIn>>();
    }
  },

  {kTPCCDeliveryTx,
   [] (Epoch *e) {
      return new
      (AllocBaseRequest(e, sizeof(Request<MixIn<tpcc::DeliveryStruct, tpcc::TPCCMixIn>>)))
      Request<MixIn<tpcc::DeliveryStruct, tpcc::TPCCMixIn>>();
    }
  },

  {kTPCCCreditCheckTx,
   [] (Epoch *e) {
      return new
      (AllocBaseRequest(e, sizeof(Request<MixIn<tpcc::CreditCheckStruct, tpcc::TPCCMixIn>>)))
      Request<MixIn<tpcc::CreditCheckStruct, tpcc::TPCCMixIn>>();
    }
  },

  {kTPCCPaymentTx,
   [] (Epoch *e) {
      return new
      (AllocBaseRequest(e, sizeof(Request<MixIn<tpcc::PaymentStruct, tpcc::TPCCMixIn>>)))
      Request<MixIn<tpcc::PaymentStruct, tpcc::TPCCMixIn>>();
    }
  },
};

template <enum tpcc::loaders::TPCCLoader TLN>
static tpcc::loaders::Loader<TLN> *CreateLoader(unsigned long seed, std::mutex *m,
						std::atomic_int *count_down, int cpu)
{
  return new tpcc::loaders::Loader<TLN>(seed, m, count_down, cpu);
}

static void LoadTPCCDataSet()
{
  std::mutex m;
  std::atomic_int count_down(6);
  m.lock(); // use as a semaphore

  go::GetSchedulerFromPool(1)->WakeUp(CreateLoader<tpcc::loaders::Warehouse>(9324, &m, &count_down, 0));
  go::GetSchedulerFromPool(2)->WakeUp(CreateLoader<tpcc::loaders::Item>(235443, &m, &count_down, 1));
  go::GetSchedulerFromPool(3)->WakeUp(CreateLoader<tpcc::loaders::Stock>(89785943, &m, &count_down, 2));
  go::GetSchedulerFromPool(4)->WakeUp(CreateLoader<tpcc::loaders::District>(129856349, &m, &count_down, 3));
  go::GetSchedulerFromPool(5)->WakeUp(CreateLoader<tpcc::loaders::Customer>(923587856425, &m, &count_down, 4));
  go::GetSchedulerFromPool(6)->WakeUp(CreateLoader<tpcc::loaders::Order>(2343352, &m, &count_down, 5));

  m.lock(); // waits
  auto &mgr = Instance<dolly::RelationManager>();
  mgr.GetRelationOrCreate(mgr.LookupRelationId("customer_name_idx")).set_read_only(true);
  mgr.GetRelationOrCreate(mgr.LookupRelationId("item")).set_read_only(true);
}

class TPCCModule : public Module<WorkloadModule> {
 public:
  void Init() override {
    BaseRequest::MergeFactoryMap(kTPCCFactoryMap);
    // just to initialize this
    Instance<tpcc::TPCCTableHandles>();
    LoadTPCCDataSet();
  }
  std::string name() const override {
    return "TPC-C";
  }
};

static TPCCModule tpcc_module;

}
