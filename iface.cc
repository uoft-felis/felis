#include "util.h"
#include "promise.h"
#include "node_config.h"
#include "txn.h"
#include "epoch.h"

namespace util {

using namespace felis;

template <>
PromiseRoutineTransportService &Impl()
{
  return Instance<NodeConfiguration>();
}

template <>
PromiseRoutineLookupService &Impl()
{
  return Instance<EpochPromiseRoutineLookupService>();
}

}
