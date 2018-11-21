#include "txn.h"
#include "index.h"
#include "util.h"

namespace felis {

bool BaseTxn::TxnVHandle::AppendNewVersion()
{
  return api->AppendNewVersion(sid, epoch_nr);
}

Optional<Tuple<VHandle *>> BaseTxn::TxnIndexLookupOpImpl(const TxnIndexOpContext &ctx)
{
  auto &rel = util::Instance<RelationManager>().GetRelationOrCreate(ctx.rel_id);
  VarStr key((unsigned short) ctx.key_len, 0, ctx.key_data);
  auto handle = rel.Search(&key);
  return Tuple<VHandle *>(handle);
}

void BaseTxn::TxnFuncRef()
{
  completion.Increment(1);
}

void BaseTxn::TxnFuncUnref(BaseTxn *txn, int origin_node_id)
{
  auto cur_node_id = util::Instance<NodeConfiguration>().node_id();
  if (cur_node_id == origin_node_id) {
    txn->completion.Complete();
  }
}

}
