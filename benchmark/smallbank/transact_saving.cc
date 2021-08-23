#include "transact_saving.h"

namespace smallbank {

template <>
TransactSavingStruct ClientBase::GenerateTransactionInput<TransactSavingStruct>() {
  TransactSavingStruct s;
  s.account_id = PickAccount();
  s.transact_v = RandomNumber(1, 100);
  return s;
}

TransactSavingTxn::TransactSavingTxn(Client *client, uint64_t serial_id)
    : Txn<TransactSavingState>(serial_id),
      TransactSavingStruct(client->GenerateTransactionInput<TransactSavingStruct>()),
      client(client) {}

void TransactSavingTxn::Prepare() {
  INIT_ROUTINE_BRK(8192);
  auto &mgr = util::Instance<TableManager>();
  void *buf = alloca(512);
  auto account_key = Account::Key::New(account_id);
  auto account_ptr = mgr.Get<Account>().Search(account_key.EncodeView(buf));
  // shirley note: this is a hack bc account table never changes so we can directly read from index_info using sid = 1
  uint64_t cid = account_ptr->ReadWithVersion(1)->template ToType<Account::Value>().CustomerID;

  auto saving_key = Saving::Key::New(cid);
  TxnIndexLookup<DummySliceRouter, TransactSavingState::Completion, void>(
      nullptr, KeyParam<Saving>(saving_key));
}

void TransactSavingTxn::Run() {
  auto aff = std::numeric_limits<uint64_t>::max();

  root->AttachRoutine(
    MakeContext(transact_v), 1,
    [](const auto &ctx) {
      auto &[state, index_handle, transact_v] = ctx;
      TxnRow vhandle = index_handle(state->saving);
      auto sv = vhandle.Read<Saving::Value>();
      sv.BalanceSv += transact_v;
      vhandle.Write(sv);
    },
    aff
  );
    
  // shirley zen: add sfence after txn run
  // _mm_sfence();
}

} // namespace smallbank
