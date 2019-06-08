#include "ycsb.h"
#include "index.h"
#include "txn.h"

namespace ycsb {

using namespace felis;

static constexpr int kExtraRead = 0;
static constexpr int kTotal = 10;

static int DummySliceRouter(int16_t slice_id) { return 1; } // Always on node 1

struct RMWStruct {
  uint64_t keys[kTotal];
};

struct RMWState {
  VHandle *rows[kTotal];

  struct LookupCompletion : public TxnStateCompletion<RMWState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->rows[id] = rows[0];
      if (id < kTotal - kExtraRead)
        handle(rows[0]).AppendNewVersion();
    }
  };
};

template <>
RMWStruct Client::GenerateTransactionInput<RMWStruct>()
{
  RMWStruct s;

  for (int i = 0; i < kTotal; i++) {
 again:
    s.keys[i] = rand.next();
    for (int j = 0; j < i; j++)
      if (s.keys[i] == s.keys[j])
        goto again;
  }

  return s;
}

class RMWTxn : public Txn<RMWState>, public RMWStruct {
  Client *client;
 public:
  RMWTxn(Client *client, uint64_t serial_id);
  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final {}
};

RMWTxn::RMWTxn(Client *client, uint64_t serial_id)
    : Txn<RMWState>(serial_id),
      RMWStruct(client->GenerateTransactionInput<RMWStruct>()),
      client(client)
{}

void RMWTxn::Prepare()
{
  Ycsb::Key dbk[kTotal];
  for (int i = 0; i < kTotal; i++) dbk[i].k = keys[i];

  INIT_ROUTINE_BRK(8192);

  // Omit the return value because this workload is totally single node
  TxnIndexLookup<RMWState::LookupCompletion, void>(
      DummySliceRouter,
      nullptr,
      KeyParam<Ycsb>(dbk, kTotal));
}

void RMWTxn::Run()
{
  proc
      | TxnProc(
          1, // Always on node 1
          [](const auto &ctx, auto args) -> Optional<VoidValue> {
            auto &[state, index_handle] = ctx;
            for (int i = 0; i < kTotal; i++) {
              TxnVHandle vhandle = index_handle(state->rows[i]);
              auto dbv = vhandle.Read<Ycsb::Value>();

              if (i < kTotal - kExtraRead) {
                dbv.v.resize_junk(90);
                vhandle.Write(dbv);
              }
            }
            return nullopt;
          });
}

void YcsbLoader::Run()
{
  auto &mgr = util::Instance<felis::RelationManager>();
  int table_id = static_cast<int>(TableType::Ycsb);
  mgr.GetRelationOrCreate(table_id);

  void *large_buf = alloca(1024);

  auto nr_threads = NodeConfiguration::g_nr_threads;
  for (auto t = 0; t < nr_threads; t++) {
    mem::ParallelPool::SetCurrentAffinity(t);
    util::PinToCPU(t + NodeConfiguration::g_core_shifting);
    for (unsigned long i = t; i < Client::kTableSize; i += nr_threads) {
      Ycsb::Key dbk;
      Ycsb::Value dbv;
      dbk.k = i;
      dbv.v.resize_junk(90);
      auto handle = mgr[table_id].SearchOrCreate(dbk.EncodeFromAlloca(large_buf));
      // TODO: slice mapping table stuff?
      felis::InitVersion(handle, dbv.Encode());
    }
  }
  util::PinToCPU(go::Scheduler::CurrentThreadPoolId() - 1 + NodeConfiguration::g_core_shifting);
  mem::ParallelPool::SetCurrentAffinity(-1);
  finish.unlock();
}

Client::Client() noexcept
{
  rand.init(kTableSize, kTheta, 1238);
}

BaseTxn *Client::CreateTxn(uint64_t serial_id)
{
  return new RMWTxn(this, serial_id);
}

}
