#include "index.h"
#include "felis_probes.h"

using util::Instance;

namespace felis {

std::map<std::string, Checkpoint *> Checkpoint::impl;

void InitVersion(felis::VHandle *handle, VarStr *obj = (VarStr *) kPendingValue) {
  while (!handle->AppendNewVersion(0, 0)) {
      asm("pause" : : :"memory");
    }
  if (obj != (void *) kPendingValue) {
    abort_if(!handle->WriteWithVersion(0, obj, 0),
              "Diverging outcomes during setup setup");
  }
}

}
