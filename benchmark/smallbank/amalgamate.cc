#include "amalgamate.h"

namespace smallbank {

template <>
AmalgamateStruct ClientBase::GenerateTransactionInput<AmalgamateStruct>() {
  AmalgamateStruct s;
  s.account_id_1 = PickAccount();
  s.account_id_2 = PickAccount();
  while (s.account_id_2 == s.account_id_1) {
    s.account_id_2 = PickAccount();
  }
  return s;
}

AmalgamateTxn::AmalgamateTxn(Client *client, uint64_t serial_id)
    : Txn<AmalgamateState>(serial_id),
      AmalgamateStruct(client->GenerateTransactionInput<AmalgamateStruct>()),
      client(client) {}

AmalgamateTxn::AmalgamateTxn(Client *client, uint64_t serial_id, AmalgamateStruct *input)
    : Txn<AmalgamateState>(serial_id),
      client(client) 
{
  RecoverInputStruct(input);
}

void AmalgamateTxn::Prepare() {
  INIT_ROUTINE_BRK(8192);
  auto &mgr = util::Instance<TableManager>();
  void *buf = alloca(512);

  auto account_key_1 = Account::Key::New(account_id_1);
  auto account_ptr_1 = mgr.Get<Account>().Search(account_key_1.EncodeView(buf));
  // shirley note: this is a hack bc account table never changes so we can directly read from index_info using sid = 1
  uint64_t cid_1 = account_ptr_1->ReadWithVersion(1)->template ToType<Account::Value>().CustomerID;
  
  auto account_key_2 = Account::Key::New(account_id_2);
  auto account_ptr_2 = mgr.Get<Account>().Search(account_key_2.EncodeView(buf));
  // shirley note: this is a hack bc account table never changes so we can directly read from index_info using sid = 1
  uint64_t cid_2 = account_ptr_2->ReadWithVersion(1)->template ToType<Account::Value>().CustomerID;

  auto saving_key_1 = Saving::Key::New(cid_1);
  auto checking_key_1 = Checking::Key::New(cid_1);
  auto checking_key_2 = Checking::Key::New(cid_2);
  TxnIndexLookup<DummySliceRouter, AmalgamateState::Completion, void>(
      nullptr, 
      KeyParam<Saving>(saving_key_1),
      KeyParam<Checking>(checking_key_1),
      KeyParam<Checking>(checking_key_2));
}

void AmalgamateTxn::Run() {
  auto aff = std::numeric_limits<uint64_t>::max();

  root->AttachRoutine(
    MakeContext(), 1,
    [](const auto &ctx) {
      auto &[state, index_handle] = ctx;
      TxnRow vhandle_sv_1 = index_handle(state->saving_1);
      TxnRow vhandle_ck_1 = index_handle(state->checking_1);
      TxnRow vhandle_ck_2 = index_handle(state->checking_2);
      auto sv_1 = vhandle_sv_1.Read<Saving::Value>();
      auto ck_1 = vhandle_ck_1.Read<Checking::Value>();
      auto ck_2 = vhandle_ck_2.Read<Checking::Value>();
      ck_2.BalanceCk += sv_1.BalanceSv;
      ck_2.BalanceCk += ck_1.BalanceCk;
      sv_1.BalanceSv = 0;
      ck_1.BalanceCk = 0;
      vhandle_ck_2.Write(ck_2);
      vhandle_sv_1.Write(sv_1);
      vhandle_ck_1.Write(ck_1);
    },
    aff
  );
    
  // shirley zen: add sfence after txn run
  if (felis::Options::kEnableZen) {
    _mm_sfence();
  }
}

} // namespace smallbank
