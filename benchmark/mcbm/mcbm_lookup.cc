#include "mcbm_lookup.h"

namespace mcbm {

template <>
LookupStruct ClientBase::GenerateTransactionInput<LookupStruct>() {
  LookupStruct s;
  s.row_id = PickRowNoDup();
  // s.row_id = PickRow();
  return s;
}

LookupTxn::LookupTxn(Client *client, uint64_t serial_id)
    : Txn<LookupState>(serial_id),
      LookupStruct(client->GenerateTransactionInput<LookupStruct>()),
      client(client) {}

LookupTxn::LookupTxn(Client *client, uint64_t serial_id, LookupStruct *input)
    : Txn<LookupState>(serial_id),
      client(client) 
{
  RecoverInputStruct(input);
}

void LookupTxn::Prepare() {
  auto &mgr = util::Instance<TableManager>();
  void *buf = alloca(512);
  auto key = MBTable::Key::New(row_id);
  state->row = mgr.Get<MBTable>().Search(key.EncodeView(buf));
}

void LookupTxn::ReadRowLookup(TxnRow vhandle)
{
    uint64_t rval = vhandle.Read<MBTable::Value>().v;
    // printf("read: value %lu\n", rval);
}

void LookupTxn::Run() {
  auto aff = std::numeric_limits<uint64_t>::max();

  root->AttachRoutine(
    MakeContext(), 1,
    [](const auto &ctx) {
      auto &[state, index_handle] = ctx;
      ReadRowLookup(index_handle(state->row));
    },
    aff
  );

  // shirley zen: add sfence after txn run
  if (felis::Options::kEnableZen) {
    // _mm_sfence();
  }
}

} // namespace mcbm
