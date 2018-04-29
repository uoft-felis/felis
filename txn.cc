#include "txn.h"
#include "index.h"

namespace dolly {

VHandle *BaseTxn::TxnIndex::Lookup(const VarStr *k)
{
  return rel->SetupReExec(k, sid, epoch_nr);
}

}
