#include "ycsb.h"
#include "index.h"
#include "txn_cc.h"
#include "pwv_graph.h"
#include "util/os.h"

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
  IndexInfo *rows[kTotal];
  InvokeHandle<RMWState> futures[kTotal];

  std::atomic_ulong signal; // Used only if g_dependency
  FutureValue<void> deps; // Used only if g_dependency

  struct LookupCompletion : public TxnStateCompletion<RMWState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->rows[id] = rows[0];
      if (id < kTotal - Client::g_extra_read) {
        bool last = (id == kTotal - Client::g_extra_read - 1);
        handle(rows[0]).AppendNewVersion(last ? 0 : 1);
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
  RMWTxn(Client *client, uint64_t serial_id, RMWStruct *input);
  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final {}
  static void WriteRow(TxnRow vhandle);
  static void ReadRow(TxnRow vhandle);

  void RecoverInputStruct(RMWStruct *input) {
    for (int i = 0; i < kTotal; i++) {
      this->keys[i] = input->keys[i];
    }
  }

  template <typename Func>
  void RunOnPartition(Func f) {
    auto handle = index_handle();
    for (int i = 0; i < kTotal; i++) {
      auto part = (keys[i] * NodeConfiguration::g_nr_threads) / Client::g_table_size;
      f(part, root, Tuple<unsigned long, int, decltype(state), decltype(handle), int>(keys[i], i, state, handle, part));
    }
  }
};

RMWTxn::RMWTxn(Client *client, uint64_t serial_id)
    : Txn<RMWState>(serial_id),
      RMWStruct(client->GenerateTransactionInput<RMWStruct>()),
      client(client)
{}

RMWTxn::RMWTxn(Client *client, uint64_t serial_id, RMWStruct *input)
    : Txn<RMWState>(serial_id),
      client(client)
{
  RecoverInputStruct(input);
}

void RMWTxn::Prepare()
{
  if (!VHandleSyncService::g_lock_elision) {
    Ycsb::Key dbk[kTotal];
    for (int i = 0; i < kTotal; i++) dbk[i].k = keys[i];
    INIT_ROUTINE_BRK(8192);

    // Omit the return value because this workload is totally single node
    TxnIndexLookup<DummySliceRouter, RMWState::LookupCompletion, void>(
        nullptr,
        KeyParam<Ycsb>(dbk, kTotal));
  } else {
    static constexpr auto LookupIndex = [](auto k, int i, auto state, auto handle) {
      auto &rel = util::Instance<TableManager>().Get<ycsb::Ycsb>();
      Ycsb::Key dbk;
      dbk.k = k;
      void *buf = alloca(512);
      state->rows[i] = rel.Search(dbk.EncodeView(buf));
      if (i < kTotal - Client::g_extra_read)
        handle(state->rows[i]).AppendNewVersion();
    };
    if (Client::g_enable_pwv) {
      RunOnPartition(
          [this](auto part, auto root, const auto &t) {
            auto [_1, i, _2, _3, _part] = t;
            util::Instance<PWVGraphManager>()[part]->ReserveEdge(serial_id());
          });
    }
    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          root->AttachRoutine(
              t, 1, // Always on the local node.
              [](auto &ctx) {
                auto [k, i, state, handle, part] = ctx;
                LookupIndex(k, i, state, handle);

                if (Client::g_enable_pwv)
                  util::Instance<PWVGraphManager>()[part]->AddResource(
                      handle.serial_id(), PWVGraph::VHandleToResource(state->rows[i]));
              },
              part); // Partitioning affinity.

        });

  }
}

void RMWTxn::WriteRow(TxnRow vhandle)
{
  auto dbv = vhandle.Read<Ycsb::Value>();
  dbv.v.assign(Client::zero_data, 100);
  dbv.v.resize_junk(999);
  vhandle.Write(dbv);
}

void RMWTxn::ReadRow(TxnRow vhandle)
{
  vhandle.Read<Ycsb::Value>();
}

void RMWTxn::Run()
{
  if (Client::g_dependency)
    state->signal = 0;

  if (!Options::kEnablePartition) {
    auto bitmap = 1ULL << (kTotal - Client::g_extra_read - 1);
    for (int i = 0; i < kTotal - Client::g_extra_read - 1; i++) {
      state->futures[i] = UpdateForKey(
          1, state->rows[i],
          [](const auto &ctx, IndexInfo *row) {
            auto &[state, index_handle] = ctx;
            WriteRow(index_handle(row));
            if (Client::g_dependency
                && state->signal.fetch_add(1) + 1 == kTotal - Client::g_extra_read - 1)
              state->deps.Signal();
          });

      if (state->futures[i].has_callback())
        bitmap |= 1ULL << i;
    }

    auto aff = std::numeric_limits<uint64_t>::max();
    // auto aff = AffinityFromRows(bitmap, state->rows);
    root->AttachRoutine(
        MakeContext(), 1,
        [](const auto &ctx) {
          auto &[state, index_handle] = ctx;
          for (int i = 0; i < kTotal - Client::g_extra_read - 1; i++) {
            state->futures[i].Invoke(state, index_handle);
          }
          if (Client::g_dependency) {
            state->deps.Wait();
          }
          WriteRow(index_handle(state->rows[kTotal - Client::g_extra_read - 1]));
          for (auto i = kTotal - Client::g_extra_read; i < kTotal; i++) {
            ReadRow(index_handle(state->rows[i]));
          }
        },
        aff);

  } else if (Client::g_enable_granola || Client::g_enable_pwv) {
    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          root->AttachRoutine(
              t, 1,
              [](auto &ctx) {
                auto &[k, i, state, handle, _part] = ctx;

                if (Client::g_dependency && i == kTotal - Client::g_extra_read - 1) {
                  while (state->signal != i) _mm_pause();
                }

                TxnRow vhandle = handle(state->rows[i]);
                auto dbv = vhandle.Read<Ycsb::Value>();

                static thread_local volatile char buffer[100];
                std::copy(dbv.v.data(), dbv.v.data() + 100, buffer);

                if (i < kTotal - Client::g_extra_read) {
                  dbv.v.resize_junk(90);
                  vhandle.Write(dbv);
                  if (Client::g_dependency && i < kTotal - Client::g_extra_read - 1) {
                    state->signal.fetch_add(1);
                  }
                }

                if (Client::g_enable_pwv) {
                  util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                      handle.serial_id(), PWVGraph::VHandleToResource(state->rows[i]));
                }
              },
              part);
        });
  } else {
    // Bohm
    RunOnPartition(
        [this](auto part, auto root, const auto &t) {
          const auto &[k, i, _1, _2, _part] = t;
          if (i > kTotal - Client::g_extra_read) return;

          static thread_local volatile char buffer[100];

          if (i == kTotal - Client::g_extra_read) {
            // All reads here
            root->AttachRoutine(
                t, 1,
                [](auto &ctx) {
                  auto [k, i, state, handle, _part] = ctx;

                  TxnRow vhandle = handle(state->rows[i]);
                  auto v = vhandle.Read<Ycsb::Value>();
                  std::copy(v.v.data(), v.v.data() + 100, buffer);
                });
          } else {
            root->AttachRoutine(
                t, 1,
                [](auto &ctx) {
                  auto [k, i, state, handle, _part] = ctx;
                  // Last write
                  if (Client::g_dependency && i == kTotal - Client::g_extra_read - 1) {
                    while (state->signal != i) _mm_pause();
                  }

                  TxnRow vhandle = handle(state->rows[i]);
                  auto v = vhandle.Read<Ycsb::Value>();

                  std::copy(v.v.data(), v.v.data() + 100, buffer);

                  v.v.resize_junk(90);
                  vhandle.Write(v);
                  state->signal.fetch_add(1);
                }, part);
          }
        });
  }
  // shirley zen: add sfence after txn run
  if (felis::Options::kEnableZen) {
    // _mm_sfence();
  }
}

void YcsbLoader::Run()
{
  auto &mgr = util::Instance<felis::TableManager>();
  mgr.Create<Ycsb>();

  // shirley: don't load init database if is recovery
  if (felis::Options::kRecovery) {
    done = true;
    return;
  }

  void *buf = alloca(512);

  auto nr_threads = NodeConfiguration::g_nr_threads;
  for (auto t = 0; t < nr_threads; t++) {
    printf("t = %d\n", t);
    MasstreeIndex::ResetThreadInfo();

    mem::ParallelPool::SetCurrentAffinity(t);
    mem::GetExternalPmemPool().SetCurrentAffinity(t);
    felis::VHandle::PoolSetCurrentAffinity(t);
    util::Cpu info;
    info.set_affinity(t);
    info.Pin();

    unsigned long start = t * Client::g_table_size / nr_threads;
    unsigned long end = (t + 1) * Client::g_table_size / nr_threads;

    for (unsigned long i = start; i < end; i++) {
      Ycsb::Key dbk;
      Ycsb::Value dbv;
      dbk.k = i;
      dbv.v.resize_junk(999);
      auto handle = mgr.Get<ycsb::Ycsb>().SearchOrCreate(dbk.EncodeView(buf));
      // TODO: slice mapping table stuff?
      // shirley: initial database should be allocated from inline pmem.
      auto p = handle->vhandle_ptr()->AllocFromInline(dbv.EncodeSize());
      felis::InitVersion(handle, i, -1, -1, -1, (int)ycsb::TableType::Ycsb, dbv.EncodeToPtrOrDefault(p));
    }
  }
  util::Cpu info;
  info.set_affinity(go::Scheduler::CurrentThreadPoolId() - 1);
  info.Pin();

  mem::ParallelPool::SetCurrentAffinity(-1);
  mem::GetExternalPmemPool().SetCurrentAffinity(-1);
  felis::VHandle::PoolSetCurrentAffinity(-1);
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

void YcsbLoaderRecovery::DoLoadRecovery() {
  // shirley: recover vhandles if is recovery
  if (felis::Options::kRecovery) {
    void *large_buf = alloca(1024);
    int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    uint64_t curr_ep = util::Instance<EpochManager>().current_epoch_nr();
    mem::BrkWFree *vhandles_brk = felis::VHandle::inline_pool.get_pool(core_id);
    uint8_t *data = vhandles_brk->get_data();
    uint64_t *ring_buffer = vhandles_brk->get_ring_buffer(); 
    size_t data_offset = vhandles_brk->get_cached_offset();
    size_t initial_offset_freelist = vhandles_brk->get_cached_initial_offset_freelist();
    size_t initial_offset_pending_freelist = vhandles_brk->get_cached_initial_offset_pending_freelist();
    size_t data_block_size = vhandles_brk->get_cached_block_size();
    size_t lmt_rb = vhandles_brk->get_cached_limit_ring_buffer();

    // reset deleted vhandles
    // note: assuming we'll never delete too much into pending freelist that it loops over to freelist
    for (size_t i_off = initial_offset_freelist; i_off != initial_offset_pending_freelist; i_off++) {
      if (i_off == lmt_rb) {
        i_off = 0;
        if (i_off == initial_offset_pending_freelist) {
          // we've scanned tne entire freelist.
          break;
        }
      }
      std::memset((uint8_t *)(ring_buffer[i_off]), 0, 64);
      // shirley pmem shirey test
      // _mm_clwb((uint64_t *)(ring_buffer[i_off]));
    }
    // shirley pmem shirey test
    // _mm_sfence();

    // now read vhandles and rebuild index
    for (uint64_t i = 0; i < data_offset; i += data_block_size) {
      VHandle *vhdl_row = (VHandle *)(data + i);
      int table_id = vhdl_row->table_id;
      if (table_id == 0) continue; // shirley: this row was deleted and reseted during freelist
      int key0 = vhdl_row->key_0;
      Ycsb::Key dbk;
      dbk.k = key0;
      auto handle =
          util::Instance<felis::TableManager>()
              .Get<ycsb::Ycsb>()
              .RecoverySearchOrCreate(dbk.EncodeView(large_buf), vhdl_row);
      
      // shirley: no non-determinism in ycsb. dont need to revert. but need to rebuild major GC list
      uint64_t vhdl_sid2 = vhdl_row->GetInlineSid(felis::SortedArrayVHandle::SidType2);
      uint64_t vhdl_sid1 = vhdl_row->GetInlineSid(felis::SortedArrayVHandle::SidType1);
      if (!vhdl_sid2) {
        continue;
      }
      if (vhdl_sid2 >> 32 == curr_ep) {
        continue;
      }
      if (vhdl_sid1 != vhdl_sid2) {
        vhdl_row->add_majorGC_if_ext();
      }
    }
  }
  return;
}

size_t Client::g_table_size = 10000000;
double Client::g_theta = 0.00;
int Client::g_extra_read = 0;
int Client::g_contention_key = 0;
bool Client::g_dependency = false;

Client::Client() noexcept
{
  rand.init(g_table_size, g_theta, 1238);
}

BaseTxn *Client::CreateTxn(uint64_t serial_id, void *txntype_id, void *txn_struct_buffer)
{
  felis::BaseTxn *base_txn = new RMWTxn(this, serial_id);

  return base_txn;
}

felis::BaseTxn *Client::CreateTxnRecovery(uint64_t serial_id, int txntype_id, void *txn_struct_buffer) {
  felis::BaseTxn *base_txn = new ycsb::RMWTxn(this, serial_id, (RMWStruct *)txn_struct_buffer);;
  return base_txn;
}

size_t Client::TxnInputSize(int txn_id) {
  return util::Align(sizeof(RMWStruct), 8);
}

void Client::PersistTxnStruct(int txn_id, void *base_txn, void *txn_struct_buffer) {
  RMWStruct txn_struct = *(RMWTxn *)base_txn;
  memcpy(txn_struct_buffer, &txn_struct, sizeof(RMWStruct));
  return;
}

}
