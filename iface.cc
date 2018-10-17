#include "util.h"
#include "promise.h"
#include "node_config.h"
#include "txn.h"

namespace util {

using namespace felis;

template <>
PromiseRoutineTransportService &Impl()
{
  return Instance<NodeConfiguration>();
}

}
