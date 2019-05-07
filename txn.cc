#include "txn.h"
#include "index.h"
#include "util.h"
#include "literals.h"

namespace felis {

mem::Brk BaseTxn::g_brk;

void BaseTxn::InitBrk(long nr_epochs)
{
  auto lmt = 32_M * nr_epochs;
  g_brk = mem::Brk(mem::MemMapAlloc(mem::Txn, lmt), lmt);
}

void BaseTxn::TxnVHandle::AppendNewVersion()
{
  api->AppendNewVersion(sid, epoch_nr);
}

VarStr *BaseTxn::TxnVHandle::ReadVarStr()
{
  return api->ReadWithVersion(sid);
}

bool BaseTxn::TxnVHandle::WriteVarStr(VarStr *obj)
{
  return api->WriteWithVersion(sid, obj, epoch_nr);
}

BaseTxn::TxnIndexOpContext::TxnIndexOpContext(
    TxnHandle handle, EpochObject state,
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

size_t BaseTxn::TxnIndexOpContext::EncodeSize() const
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

uint8_t *BaseTxn::TxnIndexOpContext::EncodeTo(uint8_t *buf) const
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

const uint8_t *BaseTxn::TxnIndexOpContext::DecodeFrom(const uint8_t *buf)
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

BaseTxn::TxnIndexLookupOpImpl::TxnIndexLookupOpImpl(const TxnIndexOpContext &ctx, int idx)
{
  auto &rel = util::Instance<RelationManager>()[ctx.relation_ids[idx]];
  result.fill(nullptr);

  if (ctx.slice_ids[idx] >= 0 || ctx.slice_ids[idx] == kReadOnlySliceId) {
    VarStr key(ctx.key_len[idx], 0, ctx.key_data[idx]);
    auto handle = rel.Search(&key);
    result[0] = handle;
  } else if (ctx.slice_ids[idx] == -1) {
    VarStr range_start(ctx.key_len[idx], 0, ctx.key_data[idx]);
    VarStr range_end(ctx.key_len[idx + 1], 0, ctx.key_data[idx + 1]);
    auto &mgr = util::Instance<RelationManager>();
    auto &table = mgr[ctx.relation_ids[idx]];
    int i = 0;
    for (auto it = table.IndexSearchIterator(&range_start, &range_end);
         it.IsValid(); it.Next(), i++) {
      result[i] = it.row();
    }
  }
}

BaseTxn::TxnIndexInsertOpImpl::TxnIndexInsertOpImpl(const TxnIndexOpContext &ctx, int idx)
{
  auto &rel = util::Instance<RelationManager>()[ctx.relation_ids[idx]];

  abort_if(ctx.keys_bitmap != ctx.slices_bitmap,
           "InsertOp should have same number of keys and values. bitmap {} != {}",
           ctx.keys_bitmap, ctx.slices_bitmap);
  VarStr key(ctx.key_len[idx], 0, ctx.key_data[idx]);
  result = rel.SearchOrDefault(
      &key,
      [&ctx, idx]() {
        VarStr *kstr = VarStr::New(ctx.key_len[idx]);
        memcpy((void *) kstr->data, ctx.key_data[idx], ctx.key_len[idx]);
        auto row = new VHandle();
        util::Instance<felis::SliceManager>().OnNewRow(
            ctx.slice_ids[idx], ctx.relation_ids[idx], kstr, row);
        return row;
      });
}

}
