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

Optional<Tuple<VHandle *, int>> BaseTxn::TxnIndexLookupOpImpl(int i, const TxnIndexOpContext &ctx)
{
  auto &rel = util::Instance<RelationManager>()[ctx.rel_id];
  VarStr key(ctx.key_len[i], 0, ctx.key_data[i]);
  auto handle = rel.Search(&key);
  if (handle == nullptr)
    std::abort();
  return Tuple<VHandle *, int>(handle, i);
}

void BaseTxn::TxnIndexInsertOpImpl(const TxnIndexOpContext &ctx)
{
  auto &rel = util::Instance<RelationManager>()[ctx.rel_id];

  abort_if(ctx.nr_keys != ctx.nr_values,
           "InsertOp should have same number of keys and values. {} != {}",
           ctx.nr_keys, ctx.nr_values);

  for (int i = 0; i < ctx.nr_keys; i++) {
    VarStr key(ctx.key_len[i], 0, ctx.key_data[i]);
    auto value = (VHandle *) ctx.value_data[i];

    rel.InsertOrDefault(&key, [value]() { return value; });
  }
}

}
