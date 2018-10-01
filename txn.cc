#include "txn.h"
#include "index.h"

namespace felis {

VHandle *BaseTxn::TxnIndex::Lookup(const VarStr *k)
{
  return api->SetupReExec(k, sid, epoch_nr);
}

bool BaseTxn::TxnVHandle::AppendNewVersion()
{
  return api->AppendNewVersion(sid, epoch_nr);
}

}
