#include "util/objects.h"
#include "tcp_node.h"
#include "epoch.h"
#include "routine_sched.h"
#include "vhandle_sync.h"

namespace util {

using namespace felis;

IMPL(PromiseRoutineTransportService, TcpNodeTransport);
IMPL(PromiseRoutineDispatchService, EpochExecutionDispatchService);
IMPL(VHandleSyncService, SpinnerSlot);
// IMPL(VHandleSyncService, SimpleSync);
IMPL(PromiseAllocationService, EpochPromiseAllocationService);

}
