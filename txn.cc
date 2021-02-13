#include "txn.h"
#include "index.h"
#include "util/objects.h"
#include "literals.h"
#include "gc.h"
#include "commit_buffer.h"

namespace felis {

BaseTxn::BrkType BaseTxn::g_brk;
int BaseTxn::g_cur_numa_node = 0;

void BaseTxn::InitBrk(long nr_epochs)
{
  auto nr_numa_nodes = (NodeConfiguration::g_nr_threads - 1) / mem::kNrCorePerNode + 1;
  auto lmt = 24_M * nr_epochs / nr_numa_nodes;
  for (auto n = 0; n < nr_numa_nodes; n++) {
    auto numa_node = n;
    g_brk[n] = mem::Brk::New(mem::AllocMemory(mem::Txn, lmt, numa_node), lmt);
  }
}

void BaseTxn::BaseTxnRow::AppendNewVersion(int ondemand_split_weight)
{
  if (!EpochClient::g_enable_granola) {
    auto commit_buffer = EpochClient::g_workload_client->commit_buffer;
    auto is_dup = commit_buffer->AddRef(go::Scheduler::CurrentThreadPoolId() - 1, vhandle, sid);
    if (!is_dup) {
      vhandle->AppendNewVersion(sid, epoch_nr, ondemand_split_weight);
    } else {
      // This should be rare. Let's warn the user.
      logger->warn("Duplicate write detected in sid {} on row {}", sid, (void *) vhandle);
    }
  } else {
    if (vhandle->nr_versions() == 0) {
      vhandle->AppendNewVersion(sid, epoch_nr);
      vhandle->WriteExactVersion(0, nullptr, epoch_nr);
    }
  }
}

VarStr *BaseTxn::BaseTxnRow::ReadVarStr()
{
  auto commit_buffer = EpochClient::g_workload_client->commit_buffer;
  auto ent = commit_buffer->LookupDuplicate(vhandle, sid);
  if (ent && ent->u.value != (VarStr *) kPendingValue)
    return ent->u.value;
  else
    return vhandle->ReadWithVersion(sid);
}

bool BaseTxn::BaseTxnRow::WriteVarStr(VarStr *obj)
{
  if (!EpochClient::g_enable_granola) {
    auto commit_buffer = EpochClient::g_workload_client->commit_buffer;
    auto ent = commit_buffer->LookupDuplicate(vhandle, sid);
    if (ent) {
      ent->u.value = obj;
      if (ent->wcnt.fetch_sub(1) - 1 > 0)
        return true;
    }
    return vhandle->WriteWithVersion(sid, obj, epoch_nr);
  } else {
    auto p = vhandle->ReadExactVersion(0);
    size_t nr_bytes = 0;
    // SHIRLEY: this is for granola so don't touch this GC (?)
    util::Instance<GC>().FreeIfGarbage(vhandle, p, obj);
    return vhandle->WriteExactVersion(0, obj, epoch_nr);
  }
}

int64_t BaseTxn::UpdateForKeyAffinity(int node, VHandle *row)
{
  if (Options::kOnDemandSplitting) {
    auto &conf = util::Instance<NodeConfiguration>();
    if (row->contention_affinity() == -1 || (node != 0 && conf.node_id() != node))
      goto nosplit;

    auto client = EpochClient::g_workload_client;
    auto commit_buffer = client->commit_buffer;

    if (commit_buffer->LookupDuplicate(row, sid))
      goto nosplit;

    auto &mgr = client->get_contention_locality_manager();
    auto aff = mgr.GetScheduleCore(row->contention_affinity());
    return aff;
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
        key_len[to] = keys[from]->len;
        key_data[to] = keys[from]->data;
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
    VarStr key(ctx.key_len[idx], 0, ctx.key_data[idx]);
    auto handle = tbl->Search(&key);
    result[0] = handle;
  } else if (ctx.slice_ids[idx] == -1) {
    VarStr range_start(ctx.key_len[idx], 0, ctx.key_data[idx]);
    VarStr range_end(ctx.key_len[idx + 1], 0, ctx.key_data[idx + 1]);
    // auto &table = mgr[ctx.relation_ids[idx]];
    int i = 0;
    for (auto it = tbl->IndexSearchIterator(&range_start, &range_end);
         it->IsValid(); it->Next(), i++) {
      result[i] = it->row();
    }
  }
  return result;
}

VHandle *BaseTxn::BaseTxnIndexOpInsert(const BaseTxnIndexOpContext &ctx, int idx)
{
  auto tbl = util::Instance<TableManager>().GetTable(ctx.relation_ids[idx]);

  abort_if(ctx.keys_bitmap != ctx.slices_bitmap,
           "InsertOp should have same number of keys and values. bitmap {} != {}",
           ctx.keys_bitmap, ctx.slices_bitmap);
  VarStr key(ctx.key_len[idx], 0, ctx.key_data[idx]);
  bool created = false;
  VHandle *result = tbl->SearchOrCreate(
      &key, &created);

  if (created) {
    VarStr *kstr = VarStr::New(ctx.key_len[idx]);
    memcpy((void *) kstr->data, ctx.key_data[idx], ctx.key_len[idx]);

    util::Instance<felis::SliceManager>().OnNewRow(
        ctx.slice_ids[idx], ctx.relation_ids[idx], kstr, result);
  }
  return result;
}

}
