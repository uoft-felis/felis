#include "balance.h"

namespace smallbank {

template <>
BalanceStruct ClientBase::GenerateTransactionInput<BalanceStruct>() {
  BalanceStruct s;
  s.account_id = PickAccount();
  return s;
}

BalanceTxn::BalanceTxn(Client *client, uint64_t serial_id)
    : Txn<BalanceState>(serial_id),
      BalanceStruct(client->GenerateTransactionInput<BalanceStruct>()),
      client(client) {}

BalanceTxn::BalanceTxn(Client *client, uint64_t serial_id, BalanceStruct *input)
    : Txn<BalanceState>(serial_id),
      client(client) 
{
  RecoverInputStruct(input);
}

void BalanceTxn::Prepare() {
  auto &mgr = util::Instance<TableManager>();
  void *buf = alloca(512);
  auto account_key = Account::Key::New(account_id);
  auto account_ptr = mgr.Get<Account>().Search(account_key.EncodeView(buf));
  // shirley note: this is a hack bc account table never changes so we can directly read from index_info using sid = 1
  uint64_t cid = account_ptr->ReadWithVersion(1)->template ToType<Account::Value>().CustomerID;

  auto saving_key = Saving::Key::New(cid);
  auto checking_key = Checking::Key::New(cid);
  state->saving = mgr.Get<Saving>().Search(saving_key.EncodeView(buf));
  state->checking = mgr.Get<Checking>().Search(checking_key.EncodeView(buf));
}

uint64_t BalanceTxn::ReadSavingBalance(TxnRow vhandle)
{
  return vhandle.Read<Saving::Value>().BalanceSv;
}

uint64_t BalanceTxn::ReadCheckingBalance(TxnRow vhandle)
{
  return vhandle.Read<Checking::Value>().BalanceCk;
}

void BalanceTxn::Run() {
  auto aff = std::numeric_limits<uint64_t>::max();

  root->AttachRoutine(
    MakeContext(), 1,
    [](const auto &ctx) {
      auto &[state, index_handle] = ctx;
      auto saving_balance = ReadSavingBalance(index_handle(state->saving));
      auto checking_balance = ReadCheckingBalance(index_handle(state->checking));
      auto total = saving_balance + checking_balance;
    },
    aff
  );

  // shirley zen: add sfence after txn run
  // _mm_sfence();
}

} // namespace smallbank
