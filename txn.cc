#include "txn.h"
#include "index.h"
#include "util/objects.h"
#include "literals.h"
#include "gc.h"
#include "gc_dram.h"
#include "commit_buffer.h"

namespace felis {

BaseTxn::BrkType BaseTxn::g_brk;
int BaseTxn::g_cur_numa_node = 0;

void BaseTxn::InitBrk(long nr_epochs)
{
  auto nr_numa_nodes = (NodeConfiguration::g_nr_threads - 1) / mem::kNrCorePerNode + 1;
  auto lmt = 128_M * nr_epochs / nr_numa_nodes;
  for (auto n = 0; n < nr_numa_nodes; n++) {
    auto numa_node = n;
    g_brk[n] = mem::Brk::New(mem::AllocMemory(mem::Txn, lmt, numa_node), lmt);
  }
}

// #define READ_OWN_WRITE

void BaseTxn::BaseTxnRow::AppendNewVersion(int ondemand_split_weight)
{
  if (!EpochClient::g_enable_granola && !EpochClient::g_enable_pwv) {
    auto commit_buffer = EpochClient::g_workload_client->commit_buffer;
    auto is_dup = false;

#ifdef READ_OWN_WRITE
    if (!VHandleSyncService::g_lock_elision)
      is_dup = commit_buffer->AddRef(go::Scheduler::CurrentThreadPoolId() - 1, vhandle, sid);
#endif

    if (!is_dup) {
      index_info->AppendNewVersion(sid, epoch_nr, ondemand_split_weight);
    } else {
      // This should be rare. Let's warn the user.
      logger->warn("Duplicate write detected in sid {} on row {}", sid, (void *) index_info);
    }
  } else {
    if (index_info->nr_versions() == 0) {
      index_info->AppendNewVersion(sid, epoch_nr);
      index_info->WriteExactVersion(0, nullptr, epoch_nr);
    }
  }
}

VarStr *BaseTxn::BaseTxnRow::ReadVarStr(bool is_insert)
{
  auto commit_buffer = EpochClient::g_workload_client->commit_buffer;
  CommitBuffer::Entry *ent = nullptr;
#ifdef READ_OWN_WRITE
  ent = commit_buffer->LookupDuplicate(index_info, sid);
#endif
  if (ent && ent->u.value != (VarStr *) kPendingValue)
    return ent->u.value;
  else
    return index_info->ReadWithVersion(sid, is_insert);
}

bool BaseTxn::BaseTxnRow::WriteVarStr(VarStr *obj)
{
  if (!EpochClient::g_enable_granola && !EpochClient::g_enable_pwv) {
    auto commit_buffer = EpochClient::g_workload_client->commit_buffer;
    CommitBuffer::Entry *ent = nullptr;
#ifdef READ_OWN_WRITE
    ent = commit_buffer->LookupDuplicate(index_info, sid);
#endif
    if (ent) {
      ent->u.value = obj;
      if (ent->wcnt.fetch_sub(1) - 1 > 0)
        return true;
    }
    return index_info->WriteWithVersion(sid, obj, epoch_nr);
  } else {
    auto p = index_info->ReadExactVersion(0);
    size_t nr_bytes = 0;
    // SHIRLEY: this is for granola so don't touch this GC (?)
    util::Instance<GC>().FreeIfGarbage(index_info->vhandle_ptr(), p, obj);
    return index_info->WriteExactVersion(0, obj, epoch_nr);
  }
}

bool BaseTxn::BaseTxnRow::WriteAbort() {
  bool result = WriteVarStr((VarStr *) kIgnoreValue);
  if ((index_info->last_version()) == this->sid) {
    // shirley: now search for the latest non-ignore value
    // if found in version array (and not the first element), then write that to vhandle (with minGC) and dram cache
    // if found in first element of version array, then only write to dram cache and optional minGC (just don't minGC).
  
    // copy the found value to dram cache
    VarStr *found_val = index_info->ReadWithVersion(sid, false);
    int val_size = sizeof(VarStr) + found_val->length();
    VarStr *val_dram = nullptr;
    
    if (!felis::Options::kDisableDramCache) {
      val_dram = (VarStr *) mem::GetDataRegion().Alloc(val_size);
      std::memcpy(val_dram, found_val, val_size);
      val_dram->set_region_id(mem::ParallelPool::CurrentAffinity());
      index_info->dram_version->val = val_dram;//(VarStr*) mem::GetDataRegion().Alloc(val_sz);
      index_info->dram_version->ep_num = sid; // util::Instance<EpochManager>().current_epoch_nr();
    }

    // now write the found value to vhandle if not already there
    if (found_val != index_info->first_version_ptr()) {
      // shirley: assume by now, prefetching vhandle has completed
      VHandle *vhandle = index_info->vhandle_ptr();
      // shirley: minor GC
      auto ptr2 = vhandle->GetInlinePtr(felis::SortedArrayVHandle::SidType2);
      // shirley: don't do minGC if is recovery bc dont have major GC list
      if (!felis::Options::kRecovery && ptr2){
        // vhandle->remove_majorGC_if_ext();
        // vhandle->FreePtr1(); 
        vhandle->Copy2To1();
      }

      auto sid2 = vhandle->GetInlineSid(felis::SortedArrayVHandle::SidType2);
      if ((felis::Options::kRecovery) && (sid2 >> 32 == this->sid >> 32)) {
        //i.e. before crash, we already wrote to this row.
        if (vhandle->is_inline_ptr(ptr2)) {
          // can directly copy to ptr2 if inlined bc of determinism
          if (!felis::Options::kDisableDramCache) {
            std::memcpy(ptr2, val_dram, val_size);
          }
          else {
            std::memcpy(ptr2, found_val, val_size);
          }
        }
        else {
          // should re-allocate if is external
          VarStr *val_ext = (VarStr *) (mem::GetExternalPmemPool().Alloc(true));
          if (!felis::Options::kDisableDramCache) {
            std::memcpy(val_ext, val_dram, val_size);
          }
          else {
            std::memcpy(val_ext, found_val, val_size);
            val_ext->set_region_id(mem::ParallelPool::CurrentAffinity());
          }
          vhandle->SetInlinePtr(felis::SortedArrayVHandle::SidType2,(uint8_t *)val_ext); 
        }
        vhandle->SetInlineSid(felis::SortedArrayVHandle::SidType2, this->sid); 
        vhandle->add_majorGC_if_ext();
        return result;
      }

      // alloc inline val and copy data
      VarStr *val = (VarStr *) (vhandle->AllocFromInline(val_size, felis::SortedArrayVHandle::SidType2));
      if (!val){
        val = (VarStr *) (mem::GetExternalPmemPool().Alloc(true));
        // val = (VarStr *) (mem::GetPersistentPool().Alloc(val_sz));
      }
      if (!felis::Options::kDisableDramCache) {
        std::memcpy(val, val_dram, val_size);
      }
      else {
        std::memcpy(val, found_val, val_size);
        val->set_region_id(mem::ParallelPool::CurrentAffinity());
      }

      // sid2 = sid;
      vhandle->SetInlineSid(felis::SortedArrayVHandle::SidType2, this->sid); 
      // ptr2 = val;
      vhandle->SetInlinePtr(felis::SortedArrayVHandle::SidType2,(uint8_t *)val); 
      // shirley: add to major GC if ptr1 inlined
      vhandle->add_majorGC_if_ext();
      
      // flush external varstr
      if (!(vhandle->is_inline_ptr((uint8_t *)val))) {
        for (int val_i = 0; val_i < val_size; val_i += 64) {
          //shirley pmem shirley test
          _mm_clwb((char *)val + val_i);
        }
      }

      //shirley pmem shirley test: flush cache after last version write
      _mm_clwb((char *)vhandle); 
      _mm_clwb((char *)vhandle + 64);
      _mm_clwb((char *)vhandle + 128);
      _mm_clwb((char *)vhandle + 192);
      //shirley: flush val in case it's external? need to check size, might be larger than 64 bytes
      
      return result;
    }
  }
  return result;
}

int64_t BaseTxn::UpdateForKeyAffinity(int node, IndexInfo *row)
{
  if (Options::kOnDemandSplitting) {
    // shirley todo: if need contention manager, modify this code (use index_info not vhandle)
    // auto &conf = util::Instance<NodeConfiguration>();
    // if (row->contention_affinity() == -1 || (node != 0 && conf.node_id() != node))
    //   goto nosplit;

    // auto client = EpochClient::g_workload_client;
    // auto commit_buffer = client->commit_buffer;

    // if (commit_buffer->LookupDuplicate(row, sid))
    //   goto nosplit;

    // auto &mgr = client->get_contention_locality_manager();
    // auto aff = mgr.GetScheduleCore(row->contention_affinity());
    // return aff;
  }

nosplit:
  return -1;
}

BaseTxn::BaseTxnIndexOpContext::BaseTxnIndexOpContext(
    BaseTxnHandle handle, EpochObject state,
    uint16_t keys_bitmap, VarStr **keys,
    uint16_t slices_bitmap, int16_t *slices,
    uint16_t rels_bitmap, int16_t *rels)
    : handle(handle), state(state), keys_bitmap(keys_bitmap),
      slices_bitmap(slices_bitmap), rels_bitmap(rels_bitmap)
{
  ForEachWithBitmap(
      keys_bitmap,
      [this, keys](int to, int from) {
        key_len[to] = keys[from]->length();
        key_data[to] = keys[from]->data();
      });
  ForEachWithBitmap(
      slices_bitmap,
      [this, slices](int to, int from) {
        slice_ids[to] = slices[from];
      });
  ForEachWithBitmap(
      rels_bitmap,
      [this, rels](int to, int from) {
        relation_ids[to] = rels[from];
      });
}

size_t BaseTxn::BaseTxnIndexOpContext::EncodeSize() const
{
    size_t sum = 0;
    int nr_keys = __builtin_popcount(keys_bitmap);
    for (auto i = 0; i < nr_keys; i++) {
      sum += 2 + key_len[i];
    }
    sum += __builtin_popcount(slices_bitmap) * sizeof(uint16_t);
    sum += __builtin_popcount(rels_bitmap) * sizeof(int16_t);
    return kHeaderSize + sum;
}

uint8_t *BaseTxn::BaseTxnIndexOpContext::EncodeTo(uint8_t *buf) const
{
  memcpy(buf, this, kHeaderSize);
  uint8_t *p = buf + kHeaderSize;
  int nr_keys = __builtin_popcount(keys_bitmap);
  int nr_slices = __builtin_popcount(slices_bitmap);
  int nr_rels = __builtin_popcount(rels_bitmap);

  for (auto i = 0; i < nr_keys; i++) {
    memcpy(p, &key_len[i], 2);
    memcpy(p + 2, key_data[i], key_len[i]);
    p += 2 + key_len[i];
  }

  memcpy(p, slice_ids, nr_slices * sizeof(int16_t));
  p += nr_slices * sizeof(int16_t);

  memcpy(p, relation_ids, nr_rels * sizeof(int16_t));
  p += nr_rels * sizeof(int16_t);

  return p;
}

const uint8_t *BaseTxn::BaseTxnIndexOpContext::DecodeFrom(const uint8_t *buf)
{
  memcpy(this, buf, kHeaderSize);

  const uint8_t *p = buf + kHeaderSize;
  int nr_keys = __builtin_popcount(keys_bitmap);
  int nr_slices = __builtin_popcount(slices_bitmap);
  int nr_rels = __builtin_popcount(rels_bitmap);

  for (auto i = 0; i < nr_keys; i++) {
    memcpy(&key_len[i], p, 2);
    key_data[i] = (uint8_t *) p + 2;
    p += 2 + key_len[i];
  }

  memcpy(slice_ids, p, nr_slices * sizeof(int16_t));
  p += nr_slices * sizeof(int16_t);

  memcpy(relation_ids, p, nr_rels * sizeof(int16_t));
  p += nr_rels * sizeof(int16_t);

  return p;
}

BaseTxn::LookupRowResult BaseTxn::BaseTxnIndexOpLookup(const BaseTxnIndexOpContext &ctx, int idx)
{
  auto tbl = util::Instance<TableManager>().GetTable(ctx.relation_ids[idx]);
  BaseTxn::LookupRowResult result;
  result.fill(nullptr);

  if (ctx.slice_ids[idx] >= 0 || ctx.slice_ids[idx] == kReadOnlySliceId) {
    VarStrView key(ctx.key_len[idx], ctx.key_data[idx]);
    auto handle = tbl->Search(key);
    result[0] = handle;
  } else if (ctx.slice_ids[idx] == -1) {
    // printf("BaseTxnIndexOpLookup: reached range lookup, range_start = %lu, range_end = %lu\n",
    //         *(uint64_t *)(ctx.key_data[idx]), *(uint64_t *)(ctx.key_data[idx + 1]));
    VarStrView range_start(ctx.key_len[idx], ctx.key_data[idx]);
    VarStrView range_end(ctx.key_len[idx + 1], ctx.key_data[idx + 1]);
    // auto &table = mgr[ctx.relation_ids[idx]];
    int i = 0;

    // We need the scoped data for the iterators. Since we don't store the
    // iterators, we can allocate on the stack.

    auto r = go::Scheduler::Current()->current_routine();
    auto *olddata = r->userdata();
    if (olddata == nullptr) {
      void *buf = alloca(512);
      r->set_userdata(mem::Brk::New(buf, 512));
    }

    auto it = tbl->IndexSearchIterator(range_start, range_end);

    if (it) {
      for (; it->IsValid(); it->Next(), i++) {
        // printf("BaseTxnIndexOpLookup loop %d, result row %p\n", i, it->row());
        result[i] = it->row();
      }
    }
    else { // index search iterator not supported. use scan
      std::vector<IndexInfo *> res = tbl->SearchRange(range_start, range_end);
      for (int j = 0; j < res.size(); j++) {
        if (res[j] == nullptr) break;
        result[j] = res[j];
      }
    }
    
    r->set_userdata(olddata);
  }
  return result;
}

IndexInfo *BaseTxn::BaseTxnIndexOpInsert(const BaseTxnIndexOpContext &ctx, int idx)
{
  auto tbl = util::Instance<TableManager>().GetTable(ctx.relation_ids[idx]);

  abort_if(ctx.keys_bitmap != ctx.slices_bitmap,
           "InsertOp should have same number of keys and values. bitmap {} != {}",
           ctx.keys_bitmap, ctx.slices_bitmap);
  VarStrView key(ctx.key_len[idx], ctx.key_data[idx]);
  bool created = false;
  IndexInfo *result = tbl->SearchOrCreate(key, &created);

  if (created && NodeConfiguration::g_data_migration) {
    VarStr *kstr = VarStr::New(ctx.key_len[idx]);
    memcpy((void *) kstr->data(), ctx.key_data[idx], ctx.key_len[idx]);

    // shirley: not used in new design.
    // util::Instance<felis::SliceManager>().OnNewRow(
    //     ctx.slice_ids[idx], ctx.relation_ids[idx], kstr, result);
  }
  return result;
}

}
