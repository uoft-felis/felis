#include "index.h"
#include "felis_probes.h"
#include "opts.h"

using util::Instance;

namespace felis {

std::map<std::string, Checkpoint *> Checkpoint::impl;

//shirley: InitVersion should write to sid1, ptr1, similar to a row insert. 
void InitVersion(felis::IndexInfo *handle, int key_0, int key_1, int key_2, int key_3, int table_id, VarStr *obj = (VarStr *) kPendingValue)
{
  VHandle *p_vhandle = handle->vhandle_ptr();
  // handle-> sid1 = 0; //bc initial version of database, sid is 0.
  p_vhandle->SetInlineSid(felis::SortedArrayVHandle::SidType1, 0);
  // handle -> ptr1 = obj; // shirley: don't need to check pendingValue. tpcc always creates initial value 
  p_vhandle->SetInlinePtr(felis::SortedArrayVHandle::SidType1,(uint8_t *)obj);

  // shirley: setting keys in vhandle
  p_vhandle->set_table_keys(key_0, key_1, key_2, key_3, table_id);

  // // shirley: also init dram cache
  if (!felis::Options::kDisableDramCache) {
    auto temp_dram_version = (DramVersion*) mem::GetDataRegion().Alloc(sizeof(DramVersion));
    temp_dram_version->val = (VarStr*) mem::GetDataRegion().Alloc(VarStr::NewSize(obj->length()));
    std::memcpy(temp_dram_version->val, obj, VarStr::NewSize(obj->length()));
    int curAffinity = mem::ParallelPool::CurrentAffinity();
    ((VarStr*)(temp_dram_version->val))->set_region_id(curAffinity);
    temp_dram_version->ep_num = 0;
    temp_dram_version->this_coreid = curAffinity;
    handle->dram_version = temp_dram_version;
    util::Instance<GC_Dram>().AddRow(handle, 0);
  }

  // shirley: don't need these things below. we're only creating sid1, ptr1, 
  // not using version array (should be nullptr).
  
  // handle->AppendNewVersion(0, 0);
  // if (obj != (void *) kPendingValue) {
  //   abort_if(!handle->WriteWithVersion(0, obj, 0),
  //             "Diverging outcomes during setup setup");
  // }

  //shirley pmem: flush cache after initial row insert
  // _mm_clwb((char *)p_vhandle);
  // _mm_clwb(((char *)p_vhandle) + 64);
  // _mm_clwb(((char *)p_vhandle) + 128);
  // _mm_clwb(((char *)p_vhandle) + 192);
  //shirley: don't need flush obj. first insert always inlined in miniheap (max varstr is 83 bytes?)

}

// shirley: modify this to allocate new IndexInfo that contains ptr to new vhandle
// shirley: new vhandle is allocated in IndexInfo constructor.
IndexInfo *Table::NewRow(void *vhandle)
{
  IndexInfo *index_info = IndexInfo::New(vhandle);
  // if (enable_inline)
  //   return (VHandle *) VHandle::NewInline();
  // else
  //   return (VHandle *) VHandle::New();
  return index_info;
}

}
