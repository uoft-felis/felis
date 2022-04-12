#include "mcbm_update.h"

namespace mcbm {

template <>
UpdateStruct ClientBase::GenerateTransactionInput<UpdateStruct>() {
  UpdateStruct s;
  s.row_id = PickRow();
  return s;
}

UpdateTxn::UpdateTxn(Client *client, uint64_t serial_id)
    : Txn<UpdateState>(serial_id),
      UpdateStruct(client->GenerateTransactionInput<UpdateStruct>()),
      client(client) {}

UpdateTxn::UpdateTxn(Client *client, uint64_t serial_id, UpdateStruct *input)
    : Txn<UpdateState>(serial_id),
      client(client) 
{
  RecoverInputStruct(input);
}

void UpdateTxn::Prepare() {
  auto &mgr = util::Instance<TableManager>();
  void *buf = alloca(512);
  auto key = MBTable::Key::New(row_id);
  state->row = mgr.Get<MBTable>().Search(key.EncodeView(buf));
}

void UpdateTxn::Dummy(TxnRow vhandle) {
    return;
}

void UpdateTxn::Run() {
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
