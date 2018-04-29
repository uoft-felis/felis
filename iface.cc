#include "util.h"
#include "promise.h"
#include "node_config.h"
#include "txn.h"
#include "benchmark/tpcc/tpcc.h"

namespace util {

using namespace dolly;

template <>
PromiseRoutineTransportService &Impl()
{
  return Instance<NodeConfiguration>();
}

template <>
EpochClient &Impl()
{
  return Instance<tpcc::Client>();
}

// template <>
// BaseFactory<BaseTxn, tpcc::Client *>::Table BaseFactory<BaseTxn, tpcc::Client *>::table;

}

namespace dolly {

void InitializeInterfaces()
{
  tpcc::TxnFactory::Initialize();
}

}
