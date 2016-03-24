#include "tpcc.h"
#include "epoch.h"
#include "log.h"
#include "util.h"

using util::MixIn;
using util::Instance;

namespace db_backup {

template<>
void Request<MixIn<tpcc::NewOrderStruct, tpcc::TPCCMixIn>>::ParseFromBuffer(ParseBuffer &buffer)
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
void Request<MixIn<tpcc::DeliveryStruct, tpcc::TPCCMixIn>>::ParseFromBuffer(ParseBuffer &buffer)
{
  buffer.Read(&warehouse_id, 4);
  buffer.Read(&o_carrier_id, 4);
  buffer.Read(&ts, 4);

  for (int i = 0; i < 10; i++) {
    buffer.Read(&last_no_o_ids[i], sizeof(int32_t));
  }
}

template<>
void Request<MixIn<tpcc::CreditCheckStruct, tpcc::TPCCMixIn>>::ParseFromBuffer(db_backup::ParseBuffer &buffer)
{
  buffer.Read(&warehouse_id, 4);
  buffer.Read(&district_id, 4);
  buffer.Read(&customer_warehouse_id, 4);
  buffer.Read(&customer_district_id, 4);
  buffer.Read(&customer_id, 4);
}

template<>
void Request<MixIn<tpcc::PaymentStruct, tpcc::TPCCMixIn>>::ParseFromBuffer(ParseBuffer &buffer)
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

static const uint8_t kTPCCNewOrderTx = 1;
static const uint8_t kTPCCDeliveryTx = 2;
static const uint8_t kTPCCCreditCheckTx = 3;
static const uint8_t kTPCCPaymentTx = 4;
static const uint8_t kMaxType = 5;

static const BaseRequest::FactoryMap kTPCCFactoryMap = {
  {kTPCCNewOrderTx,
   [] () {
      return new Request<MixIn<tpcc::NewOrderStruct, tpcc::TPCCMixIn>>;
    }
  },

  {kTPCCDeliveryTx,
   [] () {
      return new Request<MixIn<tpcc::DeliveryStruct, tpcc::TPCCMixIn>>;
    }
  },

  {kTPCCCreditCheckTx,
   [] () {
      return new Request<MixIn<tpcc::CreditCheckStruct, tpcc::TPCCMixIn>>;
    }
  },

  {kTPCCPaymentTx,
   [] () {
      return new Request<MixIn<tpcc::PaymentStruct, tpcc::TPCCMixIn>>;
    }
  },
};

template <enum tpcc::loaders::TPCCLoader TLN>
static tpcc::loaders::Loader<TLN> CreateLoader(unsigned long seed)
{
  return tpcc::loaders::Loader<TLN>(seed);
}

static void LoadTPCCDataSet()
{
  auto &mgr = Instance<db_backup::WorkerManager>();
  std::future<void> tasks[] = {
    mgr.SelectWorker().AddTask(CreateLoader<tpcc::loaders::Warehouse>(9324)),
    mgr.SelectWorker().AddTask(CreateLoader<tpcc::loaders::Item>(235443)),
    mgr.SelectWorker().AddTask(CreateLoader<tpcc::loaders::Stock>(89785943)),
    mgr.SelectWorker().AddTask(CreateLoader<tpcc::loaders::District>(129856349)),
    mgr.SelectWorker().AddTask(CreateLoader<tpcc::loaders::Customer>(923587856425)),
    mgr.SelectWorker().AddTask(CreateLoader<tpcc::loaders::Order>(2343352)),
  };
  for (auto &t: tasks) {
    t.wait();
  }
}

extern "C" void InitializeWorkload()
{
  logger->info("Loading TPCC Workload Support");
  BaseRequest::MergeFactoryMap(kTPCCFactoryMap);
  logger->info("loading dataset...");

  // just to initialize this
  Instance<tpcc::TPCCTableHandles>();
  LoadTPCCDataSet();
  logger->info("done.");
}

}
