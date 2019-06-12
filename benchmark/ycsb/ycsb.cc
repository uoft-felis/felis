#include "ycsb.h"
#include "index.h"
#include "txn.h"

namespace ycsb {

using namespace felis;

static constexpr int kExtraRead = 0;
static constexpr int kTotal = 10;

static int DummySliceRouter(int16_t slice_id) { return 1; } // Always on node 1

static uint64_t *g_permutation_map;

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
    s.keys[i] = g_permutation_map[rand.next() % g_table_size];
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

  template <typename Func>
  void RunOnPartition(Func f) {
    auto root = proc.promise();
    auto handle = index_handle();
    for (int i = 0; i < kTotal; i++) {
      auto part = keys[i] % NodeConfiguration::g_nr_threads;
      f(part, root, Tuple<unsigned long, int, decltype(state), decltype(handle)>(keys[i], i, state, handle));
    }
  }
};

RMWTxn::RMWTxn(Client *client, uint64_t serial_id)
    : Txn<RMWState>(serial_id),
      RMWStruct(client->GenerateTransactionInput<RMWStruct>()),
      client(client)
{}

void RMWTxn::Prepare()
{
  if (!Client::g_enable_partition) {
    Ycsb::Key dbk[kTotal];
    for (int i = 0; i < kTotal; i++) dbk[i].k = keys[i];
    INIT_ROUTINE_BRK(8192);

    // Omit the return value because this workload is totally single node
    TxnIndexLookup<RMWState::LookupCompletion, void>(
        DummySliceRouter,
        nullptr,
        KeyParam<Ycsb>(dbk, kTotal));
  } else {
    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          root->Then(
              t, 1, // Always on the local node.
              [](auto &ctx, auto _) -> Optional<VoidValue> {
                INIT_ROUTINE_BRK(1024);
                auto &rel = util::Instance<RelationManager>()[static_cast<int>(Ycsb::kTable)];
                auto [k, i, state, handle] = ctx;
                Ycsb::Key dbk;
                dbk.k = k;
                state->rows[i] = rel.Search(dbk.EncodeFromRoutine());
                if (i < kTotal - kExtraRead)
                  handle(state->rows[i]).AppendNewVersion();
                return nullopt;
              },
              part); // Partitioning affinity.
        });
  }
}

void RMWTxn::Run()
{
  if (!Client::g_enable_partition) {
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
  } else {
    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          root->Then(
              t, 1,
              [](auto &ctx, auto _) -> Optional<VoidValue> {
                auto [k, i, state, handle] = ctx;
                TxnVHandle vhandle = handle(state->rows[i]);
                auto dbv = vhandle.Read<Ycsb::Value>();

                if (i < kTotal - kExtraRead) {
                  dbv.v.resize_junk(90);
                  vhandle.Write(dbv);
                }
                return nullopt;
              },
              part);
        });
  }
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
    for (unsigned long i = t; i < Client::g_table_size; i += nr_threads) {
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

  // Generate a random permutation
  g_permutation_map = new uint64_t[Client::g_table_size];
  for (size_t i = 0; i < Client::g_table_size; i++) {
    g_permutation_map[i] = i;
  }
  util::FastRandom perm_rand(1001);
  for (size_t i = Client::g_table_size - 1; i >= 1; i--) {
    auto j = perm_rand.next() % (i + 1);
    std::swap(g_permutation_map[j], g_permutation_map[i]);
  }
}

size_t Client::g_table_size = 400;
double Client::g_theta = 0.00;
bool Client::g_enable_partition = false;

Client::Client() noexcept
{
  rand.init(g_table_size, g_theta, 1238);
}

BaseTxn *Client::CreateTxn(uint64_t serial_id)
{
  return new RMWTxn(this, serial_id);
}

}
