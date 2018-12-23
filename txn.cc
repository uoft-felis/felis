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

Optional<Tuple<VHandle *>> BaseTxn::TxnIndexLookupOpImpl(const TxnIndexOpContext &ctx)
{
  auto &rel = util::Instance<RelationManager>().GetRelationOrCreate(ctx.rel_id);
  VarStr key((unsigned short) ctx.key_len, 0, ctx.key_data);
  auto handle = rel.Search(&key);
  return Tuple<VHandle *>(handle);
}

}
