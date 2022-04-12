#include "mcbm_delete.h"

namespace mcbm {

template <>
DeleteStruct ClientBase::GenerateTransactionInput<DeleteStruct>() {
  DeleteStruct s;
  s.row_id = PickRowNoDup();
  return s;
}

DeleteTxn::DeleteTxn(Client *client, uint64_t serial_id)
    : Txn<DeleteState>(serial_id),
      DeleteStruct(client->GenerateTransactionInput<DeleteStruct>()),
      client(client) {}

DeleteTxn::DeleteTxn(Client *client, uint64_t serial_id, DeleteStruct *input)
    : Txn<DeleteState>(serial_id),
      client(client) 
{
  RecoverInputStruct(input);
}

void DeleteTxn::Prepare() {
  auto &mgr = util::Instance<TableManager>();
  void *buf = alloca(512);
  auto key = MBTable::Key::New(row_id);
  state->row = mgr.Get<MBTable>().Search(key.EncodeView(buf));
}

void DeleteTxn::Dummy(TxnRow vhandle) {
    return;
}

void DeleteTxn::Run() {
  auto aff = std::numeric_limits<uint64_t>::max();

  root->AttachRoutine(
    MakeContext(), 1,
    [](const auto &ctx) {
      auto &[state, index_handle] = ctx;
      Dummy(index_handle(state->row));
    },
    aff
  );

  // shirley zen: add sfence after txn run
  if (felis::Options::kEnableZen) {
    // _mm_sfence();
  }
}

} // namespace mcbm
