#include "index.h"
#include "felis_probes.h"

using util::Instance;

namespace felis {

std::map<std::string, Checkpoint *> Checkpoint::impl;

//shirley: InitVersion should write to sid1, ptr1, similar to a row insert. 
void InitVersion(felis::VHandle *handle, VarStr *obj = (VarStr *) kPendingValue)
{
  // handle-> sid1 = 0; //bc initial version of database, sid is 0.
  handle->SetInlineSid(felis::SortedArrayVHandle::SidType1,0);
  // handle -> ptr1 = obj; // shirley: don't need to check pendingValue. tpcc always creates initial value 
  handle->SetInlinePtr(felis::SortedArrayVHandle::SidType1,(uint8_t *)obj);

  // shirley: don't need these things below. we're only creating sid1, ptr1, 
  // not using version array (should be nullptr).
  
  // handle->AppendNewVersion(0, 0);
  // if (obj != (void *) kPendingValue) {
  //   abort_if(!handle->WriteWithVersion(0, obj, 0),
  //             "Diverging outcomes during setup setup");
  // }

  //shirley pmem: flush cache after initial row insert
  // _mm_clwb((char *)handle);
  // _mm_clwb((char *)handle + 64);
  // _mm_clwb((char *)handle + 128);
  // _mm_clwb((char *)handle + 192);
  //shirley: don't need flush obj. first insert always inlined in miniheap (max varstr is 83 bytes?)

}

// shirley: modify this to allocate new IndexInfo that contains ptr to new vhandle
// shirley: maybe allocate new vhandle in IndexInfo constructor instead.
// shirley: Maybe create a similar slabpool for the IndexInfo (similar to vhandle slabpool)
VHandle *Table::NewRow()
{
  if (enable_inline)
    return (VHandle *) VHandle::NewInline();
  else
    return (VHandle *) VHandle::New();
}

}
