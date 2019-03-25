#include "txn.h"
#include "index.h"
#include "util.h"

namespace felis {

bool BaseTxn::TxnVHandle::AppendNewVersion()
{
  return api->AppendNewVersion(sid, epoch_nr);
}

VarStr *BaseTxn::TxnVHandle::ReadVarStr()
{
  return api->ReadWithVersion(sid);
}

bool BaseTxn::TxnVHandle::WriteVarStr(VarStr *obj)
{
  return api->WriteWithVersion(sid, obj, epoch_nr);
}

VHandle *BaseTxn::TxnIndexLookupOpImpl(int i, const TxnIndexOpContext &ctx)
{
  auto &rel = util::Instance<RelationManager>()[ctx.rel_id];
  VarStr key(ctx.key_len[i], 0, ctx.key_data[i]);
  auto handle = rel.Search(&key);
  return handle;
}

void BaseTxn::TxnIndexInsertOpImpl(const TxnIndexOpContext &ctx)
{
  auto &rel = util::Instance<RelationManager>()[ctx.rel_id];

  abort_if(ctx.keys_bitmap != ctx.values_bitmap,
           "InsertOp should have same number of keys and values. bitmap {} != {}",
           ctx.keys_bitmap, ctx.values_bitmap);

  for (auto i = 0; i < ctx.kMaxPackedKeys; i++) {
    if ((ctx.keys_bitmap & (1 << i)) == 0) continue;
    VarStr key(ctx.key_len[i], 0, ctx.key_data[i]);
    auto value = (VHandle *) ctx.value_data[i];

    rel.InsertOrDefault(&key, [value]() { return value; });
  }
}

}
