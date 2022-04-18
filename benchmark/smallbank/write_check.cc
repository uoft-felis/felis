#include "write_check.h"

namespace smallbank {

template <>
WriteCheckStruct ClientBase::GenerateTransactionInput<WriteCheckStruct>() {
  WriteCheckStruct s;
  s.account_id = PickAccount();
  s.check_v = RandomNumber(1, 100);
  return s;
}

WriteCheckTxn::WriteCheckTxn(Client *client, uint64_t serial_id)
    : Txn<WriteCheckState>(serial_id),
      WriteCheckStruct(client->GenerateTransactionInput<WriteCheckStruct>()),
      client(client) {}

WriteCheckTxn::WriteCheckTxn(Client *client, uint64_t serial_id, WriteCheckStruct *input)
    : Txn<WriteCheckState>(serial_id),
      client(client) 
{
  RecoverInputStruct(input);
}

void WriteCheckTxn::Prepare() {
  INIT_ROUTINE_BRK(8192);
  auto &mgr = util::Instance<TableManager>();
  void *buf = alloca(512);

  auto account_key = Account::Key::New(account_id);
  auto account_ptr = mgr.Get<Account>().Search(account_key.EncodeView(buf));
  // shirley note: this is a hack bc account table never changes so we can directly read from index_info using sid = 1
  uint64_t cid = account_ptr->ReadWithVersion(1)->template ToType<Account::Value>().CustomerID;
  
  auto saving_key = Saving::Key::New(cid);
  auto checking_key = Checking::Key::New(cid);
  TxnIndexLookup<DummySliceRouter, WriteCheckState::Completion, void>(
      nullptr, 
      KeyParam<Saving>(saving_key),
      KeyParam<Checking>(checking_key));
}

void WriteCheckTxn::Run() {
  auto aff = std::numeric_limits<uint64_t>::max();

  root->AttachRoutine(
    MakeContext(check_v), 1,
    [](const auto &ctx) {
      auto &[state, index_handle, check_v] = ctx;
      TxnRow vhandle_sv = index_handle(state->saving);
      TxnRow vhandle_ck = index_handle(state->checking);
      auto sv = vhandle_sv.Read<Saving::Value>();
      auto ck = vhandle_ck.Read<Checking::Value>();
      if (sv.BalanceSv + ck.BalanceCk < check_v) {
        ck.BalanceCk -= (check_v + 1);
      }
      else {
        ck.BalanceCk -= (check_v);
      }
      vhandle_ck.Write(ck);
    },
    aff
  );

  // shirley zen: add sfence after txn run
  if (felis::Options::kEnableZen) {
    _mm_sfence();
  }
}

} // namespace smallbank
