#include "util.h"
#include "promise.h"
#include "node_config.h"
#include "txn.h"
#include "benchmark/tpcc/tpcc.h"

namespace util {

using namespace felis;

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

}

namespace felis {

void InitializeInterfaces()
{
  tpcc::TxnFactory::Initialize();
}

}
