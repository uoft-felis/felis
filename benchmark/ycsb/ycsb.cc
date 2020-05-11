#include "ycsb.h"
#include "index.h"
#include "txn_cc.h"

namespace ycsb {

using namespace felis;

static constexpr int kTotal = 10;
static constexpr int kNrMSBContentionKey = 6;

class DummySliceRouter {
 public:
  static int SliceToNodeId(int16_t slice_id) { return 1; } // Always on node 1
};


// static uint64_t *g_permutation_map;

struct RMWStruct {
  uint64_t keys[kTotal];
};

struct RMWState {
  VHandle *rows[kTotal];
  unsigned long signal; // Used only if g_dependency
  FutureValue<VHandle *> futures[kTotal];

  struct LookupCompletion : public TxnStateCompletion<RMWState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->rows[id] = rows[0];
      if (id < kTotal - Client::g_extra_read) {
        bool last = (id == kTotal - Client::g_extra_read - 1);
        handle(rows[0]).AppendNewVersion(!last);
      }
    }
  };
};

template <>
RMWStruct Client::GenerateTransactionInput<RMWStruct>()
{
  RMWStruct s;

  int nr_lsb = 63 - __builtin_clzll(g_table_size) - kNrMSBContentionKey;
  size_t mask = 0;
  if (nr_lsb > 0) mask = (1 << nr_lsb) - 1;

  for (int i = 0; i < kTotal; i++) {
 again:
    // s.keys[i] = g_permutation_map[rand.next() % g_table_size];
    s.keys[i] = rand.next() % g_table_size;
    if (i < g_contention_key) {
      s.keys[i] &= ~mask;
    } else {
      if ((s.keys[i] & mask) == 0)
        goto again;
    }
    for (int j = 0; j < i; j++)
      if (s.keys[i] == s.keys[j])
        goto again;
  }

  return s;
}

char Client::zero_data[100];

class RMWTxn : public Txn<RMWState>, public RMWStruct {
  Client *client;
 public:
  RMWTxn(Client *client, uint64_t serial_id);
  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final {}
  static void WriteRow(TxnVHandle vhandle);
  static void ReadRow(TxnVHandle vhandle);

  template <typename Func>
  void RunOnPartition(Func f) {
    auto handle = index_handle();
    for (int i = 0; i < kTotal; i++) {
      auto part = (keys[i] * NodeConfiguration::g_nr_threads) / Client::g_table_size;
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
  if (Client::g_enable_granola)
    return;

  if (!Client::g_enable_lock_elision) {
    Ycsb::Key dbk[kTotal];
    for (int i = 0; i < kTotal; i++) dbk[i].k = keys[i];
    INIT_ROUTINE_BRK(8192);

    // Omit the return value because this workload is totally single node
    TxnIndexLookup<DummySliceRouter, RMWState::LookupCompletion, void>(
        nullptr,
        KeyParam<Ycsb>(dbk, kTotal));
  } else {
    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          root->Then(
              t, 1, // Always on the local node.
              [](auto &ctx, auto _) -> Optional<VoidValue> {
                INIT_ROUTINE_BRK(1024);
                auto &rel = util::Instance<TableManager>().Get<ycsb::Ycsb>();
                auto [k, i, state, handle] = ctx;
                Ycsb::Key dbk;
                dbk.k = k;
                state->rows[i] = rel.Search(dbk.EncodeFromRoutine());
                if (i < kTotal - Client::g_extra_read)
                  handle(state->rows[i]).AppendNewVersion();
                return nullopt;
              },
              part); // Partitioning affinity.
        });
  }
}

void RMWTxn::WriteRow(TxnVHandle vhandle)
{
  auto dbv = vhandle.Read<Ycsb::Value>();
  dbv.v.assign(Client::zero_data, 100);
  dbv.v.resize_junk(999);
  vhandle.Write(dbv);
}

void RMWTxn::ReadRow(TxnVHandle vhandle)
{
  vhandle.Read<Ycsb::Value>();
}

void RMWTxn::Run()
{
  if (!Client::g_enable_partition) {
    auto bitmap = 1ULL << (kTotal - Client::g_extra_read - 1);
    for (int i = 0; i < kTotal - Client::g_extra_read - 1; i++) {
      UpdateForKey(
          &state->futures[i], 1, state->rows[i],
          [](const auto &ctx, VHandle *row) -> VHandle * {
            auto &[state, index_handle] = ctx;
            WriteRow(index_handle(row));
            return row;
          });

      if (state->futures[i].has_callback())
        bitmap |= 1ULL << i;
    }

    auto aff = std::numeric_limits<uint64_t>::max();
    // auto aff = AffinityFromRows(bitmap, state->rows);
    root->Then(
        MakeContext(), 1,
        [](const auto &ctx, auto _) -> Optional<VoidValue> {
          auto &[state, index_handle] = ctx;
          for (int i = 0; i < kTotal - Client::g_extra_read - 1; i++) {
            state->futures[i].Invoke(&state, index_handle);
          }
          if (Client::g_dependency) {
            for (int i = 0; i < kTotal - Client::g_extra_read - 1; i++) {
              state->futures[i].Wait();
            }
          }
          WriteRow(index_handle(state->rows[kTotal - Client::g_extra_read - 1]));
          for (auto i = kTotal - Client::g_extra_read; i < kTotal; i++) {
            ReadRow(index_handle(state->rows[i]));
          }
          return nullopt;
        },
        aff);

  } else {
    if (Client::g_dependency)
      state->signal = 0;

    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          root->Then(
              t, 1,
              [](auto &ctx, auto _) -> Optional<VoidValue> {
                auto [k, i, state, handle] = ctx;

                if (Client::g_enable_granola && Client::g_dependency
                    && i == kTotal - Client::g_extra_read - 1) {
                  while (state->signal != i) {
                    _mm_pause();
                  }
                }

                if (Client::g_enable_granola) {
                  auto &rel = util::Instance<TableManager>().Get<ycsb::Ycsb>();
                  Ycsb::Key dbk;
                  dbk.k = k;
                  state->rows[i] = rel.Search(dbk.EncodeFromRoutine());
                }

                TxnVHandle vhandle = handle(state->rows[i]);
                auto dbv = vhandle.Read<Ycsb::Value>();

                static thread_local volatile char buffer[100];
                std::copy(dbv.v.data(), dbv.v.data() + 100, buffer);

                if (i < kTotal - Client::g_extra_read) {
                  dbv.v.resize_junk(90);
                  vhandle.Write(dbv);
                  if (Client::g_enable_granola && Client::g_dependency
                      && i < kTotal - Client::g_extra_read - 1) {
                    __sync_fetch_and_add(&state->signal, 1);
                  }
                }
                return nullopt;
              },
              part);
        });
  }
}

void YcsbLoader::Run()
{
  auto &mgr = util::Instance<felis::TableManager>();
  mgr.Create<Ycsb>();

  void *large_buf = alloca(1024);

  auto nr_threads = NodeConfiguration::g_nr_threads;
  for (auto t = 0; t < nr_threads; t++) {
    printf("t = %d\n", t);
    MasstreeIndex::ResetThreadInfo();

    mem::ParallelPool::SetCurrentAffinity(t);
    util::PinToCPU(t + NodeConfiguration::g_core_shifting);

    unsigned long start = t * Client::g_table_size / nr_threads;
    unsigned long end = (t + 1) * Client::g_table_size / nr_threads;

    for (unsigned long i = start; i < end; i++) {
      Ycsb::Key dbk;
      Ycsb::Value dbv;
      dbk.k = i;
      dbv.v.resize_junk(999);
      auto handle = mgr.Get<ycsb::Ycsb>().SearchOrCreate(dbk.EncodeFromPtr(large_buf));
      // TODO: slice mapping table stuff?
      felis::InitVersion(handle, dbv.Encode());
    }
  }
  util::PinToCPU(go::Scheduler::CurrentThreadPoolId() - 1 + NodeConfiguration::g_core_shifting);
  mem::ParallelPool::SetCurrentAffinity(-1);
  MasstreeIndex::ResetThreadInfo();

  done = true;

  // Generate a random permutation
#if 0
  g_permutation_map = new uint64_t[Client::g_table_size];
  for (size_t i = 0; i < Client::g_table_size; i++) {
    g_permutation_map[i] = i;
  }
  util::FastRandom perm_rand(1001);
  for (size_t i = Client::g_table_size - 1; i >= 1; i--) {
    auto j = perm_rand.next() % (i + 1);
    std::swap(g_permutation_map[j], g_permutation_map[i]);
  }
#endif
}

size_t Client::g_table_size = 10000000;
double Client::g_theta = 0.00;
bool Client::g_enable_partition = false;
bool Client::g_enable_lock_elision = false;
int Client::g_extra_read = 0;
int Client::g_contention_key = 0;
bool Client::g_dependency = false;

Client::Client() noexcept
{
  rand.init(g_table_size, g_theta, 1238);
}

BaseTxn *Client::CreateTxn(uint64_t serial_id)
{
  return new RMWTxn(this, serial_id);
}

}
