#include "smallbank.h"
#include "index.h"
#include "txn_cc.h"
#include "util/os.h"

#include "balance.h"
#include "deposit_checking.h"
#include "transact_saving.h"
#include "amalgamate.h"
#include "write_check.h"

namespace smallbank {

using felis::IndexInfo;
using felis::VHandle;

Config::Config() {
  nr_accounts = 1800000; // original 18000
  hotspot_percent = 90; // 90% txns to hotspots
  hotspot_number = 100000; // original 1000 // #hotspot accounts out of nr_accounts
}

Config g_smallbank_config;


ClientBase::ClientBase(const util::FastRandom &r)
    : r(r) {}

// utils for generating random #s and strings
int ClientBase::CheckBetweenInclusive(int v, int lower, int upper) {
  assert(v >= lower);
  assert(v <= upper);
  return v;
}

int ClientBase::RandomNumber(int min, int max) {
  return CheckBetweenInclusive((int)(r.next_uniform() * (max - min + 1) + min),
                               min, max);
}

int ClientBase::NonUniformRandom(int A, int C, int min, int max) {
  return (((RandomNumber(0, A) | RandomNumber(min, max)) + C) %
          (max - min + 1)) +
         min;
}


std::string ClientBase::RandomStr(uint len) {
  if (len == 0)
    return "";

  uint i = 0;
  std::string buf(len - 1, 0);
  while (i < (len - 1)) {
    const char c = (char)r.next_char();
    if (!isalnum(c))
      continue;
    buf[i++] = c;
  }
  return buf;
}

std::string ClientBase::RandomNStr(uint len) {
  const char base = '0';
  std::string buf(len, 0);
  for (uint i = 0; i < len; i++)
    buf[i] = (char)(base + (r.next() % 10));
  return buf;
}

uint64_t ClientBase::PickAccount() {
  int rand = RandomNumber(1, 100);
  bool hotspot = false;
  if (rand <= g_smallbank_config.hotspot_percent) {
    hotspot = true;
  }
  if (hotspot) {
    return RandomNumber(0, g_smallbank_config.hotspot_number - 1);
  }
  else {
    return RandomNumber(g_smallbank_config.hotspot_number, g_smallbank_config.nr_accounts - 1);
  }
}


void SmallBankLoader::Run()
{
  auto &mgr = util::Instance<felis::TableManager>();
  mgr.Create<Checking, Saving, Account>();
  // mgr.Create<Account>();

  // shirley: don't load init database if is recovery
  if (felis::Options::kRecovery) {
    done = true;
    return;
  }

  void *buf = alloca(512);
  void *buf_sv = alloca(512);
  void *buf_ck = alloca(512);

  auto nr_threads = felis::NodeConfiguration::g_nr_threads;
  for (auto t = 0; t < nr_threads; t++) {
    printf("t = %d\n", t);
    felis::MasstreeIndex::ResetThreadInfo();

    mem::ParallelPool::SetCurrentAffinity(t);
    util::Cpu info;
    info.set_affinity(t);
    info.Pin();

    unsigned long start = t * g_smallbank_config.nr_accounts / nr_threads;
    unsigned long end = (t + 1) * g_smallbank_config.nr_accounts / nr_threads;

    for (unsigned long i = start; i < end; i++) {
      Account::Key k_acc;
      Account::Value v_acc;
      k_acc.AccountName = i;
      v_acc.CustomerID = i + 1;

      auto handle_a = mgr.Get<smallbank::Account>().SearchOrCreate(k_acc.EncodeView(buf));
      // shirley: init tables. probe transient vs persistent (optional)
      // felis::probes::TransientPersistentCount{true}();

      auto p_a = handle_a->vhandle_ptr()->AllocFromInline(v_acc.EncodeSize());
      felis::InitVersion(handle_a, i, -1, -1, -1, (int)smallbank::TableType::Account, v_acc.EncodeToPtrOrDefault(p_a));

      Saving::Key k_sv;
      Saving::Value v_sv;
      k_sv.CustomerIDSv = i + 1;
      v_sv.BalanceSv = 10000;

      auto handle_s = mgr.Get<smallbank::Saving>().SearchOrCreate(k_sv.EncodeView(buf_sv));
      // shirley: init tables. probe transient vs persistent (optional)
      // felis::probes::TransientPersistentCount{true}();

      auto p_s = handle_s->vhandle_ptr()->AllocFromInline(v_sv.EncodeSize());
      felis::InitVersion(handle_s, i + 1, -1, -1, -1, (int)smallbank::TableType::Saving, v_sv.EncodeToPtrOrDefault(p_s));

      Checking::Key k_ck;
      Checking::Value v_ck;
      k_ck.CustomerIDCk = i + 1;
      v_ck.BalanceCk = 50000;

      auto handle_c = mgr.Get<smallbank::Checking>().SearchOrCreate(k_ck.EncodeView(buf_ck));
      // shirley: init tables. probe transient vs persistent (optional)
      // felis::probes::TransientPersistentCount{true}();

      auto p_c = handle_c->vhandle_ptr()->AllocFromInline(v_ck.EncodeSize());
      felis::InitVersion(handle_c, i + 1, -1, -1, -1, (int)smallbank::TableType::Checking, v_ck.EncodeToPtrOrDefault(p_c));
    }
  }
  util::Cpu info;
  info.set_affinity(go::Scheduler::CurrentThreadPoolId() - 1);
  info.Pin();

  mem::ParallelPool::SetCurrentAffinity(-1);
  felis::MasstreeIndex::ResetThreadInfo();

  done = true;
}

void SmallBankLoaderRecovery::DoLoadRecovery() {
  // shirley: recover vhandles if is recovery
  if (felis::Options::kRecovery) {
    void *large_buf = alloca(1024);
    auto &mgr = util::Instance<felis::TableManager>();
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
      // shirley pmem shirley test
      _mm_clwb((uint64_t *)(ring_buffer[i_off]));
    }
    // shirley pmem shirley test
    _mm_sfence();

    // now read vhandles and rebuild index
    for (uint64_t i = 0; i < data_offset; i += data_block_size) {
      VHandle *vhdl_row = (VHandle *)(data + i);
      int table_id = vhdl_row->table_id;
      if (table_id == 0)
        continue; // shirley: this row was deleted and reseted during freelist
      int key0 = vhdl_row->key_0;
      switch (table_id) {
        case ((int)smallbank::TableType::Account): {
          auto k = smallbank::Account::Key::New(key0);
          auto handle = mgr.Get<smallbank::Account>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)smallbank::TableType::Saving: {
          auto k = smallbank::Saving::Key::New(key0);
          auto handle = mgr.Get<smallbank::Saving>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        case (int)smallbank::TableType::Checking: {
          auto k = smallbank::Checking::Key::New(key0);
          auto handle = mgr.Get<smallbank::Checking>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        default: {
          printf("smallbank recovery loader unknown table_id = %d\n", table_id);
          std::abort();
          break;
        }
      }
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

// 20, 20, 20, 20, 20
static constexpr int kSmallBankTxnMix[] = {20, 20, 20, 20, 20};

felis::BaseTxn *Client::CreateTxn(uint64_t serial_id, void *txntype_id, void *txn_struct_buffer) {
  int rd = r.next_u32() % 100;
  int txn_type_id = 0;
  while (true) {
    int threshold = kSmallBankTxnMix[txn_type_id];
    if (rd < threshold)
      break;
    rd -= threshold;
    txn_type_id++;
  }
  felis::BaseTxn *base_txn = TxnFactory::Create(TxnType(txn_type_id), this, serial_id);
  base_txn->txn_typeid = txn_type_id;

  return base_txn;
}

felis::BaseTxn *Client::CreateTxnRecovery(uint64_t serial_id, int txntype_id, void *txn_struct_buffer) {
  felis::BaseTxn *base_txn = nullptr;
  switch (txntype_id) {
    case (int)(smallbank::TxnType::Balance): {
      base_txn = new smallbank::BalanceTxn(this, serial_id, (BalanceStruct *)txn_struct_buffer);
      break;
    }
    case (int)(smallbank::TxnType::DepositChecking): {
      base_txn = new smallbank::DepositCheckingTxn(this, serial_id, (DepositCheckingStruct *)txn_struct_buffer);
      break;
    }
    case (int)(smallbank::TxnType::TransactSaving): {
      base_txn = new smallbank::TransactSavingTxn(this, serial_id, (TransactSavingStruct *)txn_struct_buffer);
      break;
    }
    case (int)(smallbank::TxnType::Amalgamate): {
      base_txn = new smallbank::AmalgamateTxn(this, serial_id, (AmalgamateStruct *)txn_struct_buffer);
      break;
    }
    case (int)(smallbank::TxnType::WriteCheck): {
      base_txn = new smallbank::WriteCheckTxn(this, serial_id, (WriteCheckStruct *)txn_struct_buffer);
      break;
    }
    default: {
      printf("smallbank CreateTxnRecovery unknown txn_id = %d\n", txntype_id);
      std::abort();
    }
  }
  base_txn->txn_typeid = txntype_id;
  return base_txn;
}

size_t Client::TxnInputSize(int txn_id) {
  size_t input_size;
  switch (txn_id) {
    case (int)(smallbank::TxnType::Balance): {
      input_size = util::Align(sizeof(BalanceStruct), 8);
      break;
    }
    case (int)(smallbank::TxnType::DepositChecking): {
      input_size = util::Align(sizeof(DepositCheckingStruct), 8);
      break;
    }
    case (int)(smallbank::TxnType::TransactSaving): {
      input_size = util::Align(sizeof(TransactSavingStruct), 8);
      break;
    }
    case (int)(smallbank::TxnType::Amalgamate): {
      input_size = util::Align(sizeof(AmalgamateStruct), 8);
      break;
    }
    case (int)(smallbank::TxnType::WriteCheck): {
      input_size = util::Align(sizeof(WriteCheckStruct), 8);
      break;
    }
    default: {
      printf("smallbank TxnInputSize unknown txn_id = %d\n", txn_id);
      std::abort();
    }
  }
  return input_size;
}

void Client::PersistTxnStruct(int txn_id, void *base_txn, void *txn_struct_buffer) {
  switch (txn_id) {
    case (int)(smallbank::TxnType::Balance): {
      BalanceStruct txn_struct = *(BalanceTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(BalanceStruct));
      break;
    }
    case (int)(smallbank::TxnType::DepositChecking): {
      DepositCheckingStruct txn_struct = *(DepositCheckingTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(DepositCheckingStruct));
      break;
    }
    case (int)(smallbank::TxnType::TransactSaving): {
      TransactSavingStruct txn_struct = *(TransactSavingTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(TransactSavingStruct));
      break;
    }
    case (int)(smallbank::TxnType::Amalgamate): {
      AmalgamateStruct txn_struct = *(AmalgamateTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(AmalgamateStruct));
      break;
    }
    case (int)(smallbank::TxnType::WriteCheck): {
      WriteCheckStruct txn_struct = *(WriteCheckTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(WriteCheckStruct));
      break;
    }
    default: {
      printf("smallbank PersistTxnStruct unknown txn_id = %d\n", txn_id);
      std::abort();
    }
  }
  return;
}

} // namespace smallbank
