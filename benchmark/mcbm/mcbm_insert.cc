#include <numeric>
#include "mcbm_insert.h"
#include "pwv_graph.h"

namespace mcbm {

template <>
InsertStruct ClientBase::GenerateTransactionInput<InsertStruct>()
{
  InsertStruct s;
  s.row_id = PickRowNoDup();
  return s;
}

InsertTxn::InsertTxn(Client *client, uint64_t serial_id)
    : Txn<InsertState>(serial_id),
      InsertStruct(client->GenerateTransactionInput<InsertStruct>()),
      client(client)
{}

InsertTxn::InsertTxn(Client *client, uint64_t serial_id, InsertStruct *input)
    : Txn<InsertState>(serial_id),
      client(client)
{
  RecoverInputStruct(input);
}

void InsertTxn::PrepareInsert()
{
  auto &mgr = util::Instance<TableManager>();
  
  auto insert_key = MBTable::Key::New(row_id);

  INIT_ROUTINE_BRK(8192);
  void *buf = mem::AllocFromRoutine(16);

  auto handle = index_handle();

  // uint64_t args0 = row_id;

  auto args0 = Tuple<uint64_t>(row_id);

  if (VHandleSyncService::g_lock_elision) {
      // txn_indexop_affinity = g_tpcc_config.WarehouseToCoreId(warehouse_id);
      txn_indexop_affinity = std::numeric_limits<uint32_t>::max(); // magic to flatten out indexops
  }

  TxnIndexInsert<DummySliceRouter, InsertState::InsertCompletion, Tuple<uint64_t>>(
      &args0,
      KeyParam<MBTable>(insert_key));
}

void InsertTxn::Prepare()
{
    return;
}

void InsertTxn::Run()
{
    return;
}

}
