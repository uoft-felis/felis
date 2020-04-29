#include "index.h"
#include "felis_probes.h"

using util::Instance;

namespace felis {

std::map<std::string, Checkpoint *> Checkpoint::impl;

void InitVersion(felis::VHandle *handle, VarStr *obj = (VarStr *) kPendingValue)
{
  handle->AppendNewVersion(0, 0);
  if (obj != (void *) kPendingValue) {
    abort_if(!handle->WriteWithVersion(0, obj, 0),
              "Diverging outcomes during setup setup");
  }
}

VHandle *Table::NewRow()
{
  if (enable_inline)
    return (VHandle *) VHandle::NewInline();
  else
    return (VHandle *) VHandle::New();
}

}
