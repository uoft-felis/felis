// I really hope this could be a .cc file. However, Clang has problems with
// devirtualizing those virtual calls, even with -fvisibility=hidden.
//
// Only include this file from a .cc file!!!

#include "util.h"
#include "node_config.h"
#include "epoch.h"
#include "vhandle_sync.h"

namespace util {

using namespace felis;

IMPL(PromiseRoutineTransportService, NodeConfiguration);
IMPL(PromiseRoutineDispatchService, EpochExecutionDispatchService);
IMPL(VHandleSyncService, SpinnerSlot);
IMPL(PromiseAllocationService, EpochPromiseAllocationService);

}
