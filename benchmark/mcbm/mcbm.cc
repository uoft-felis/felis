#include "mcbm.h"
#include "index.h"
#include "txn_cc.h"
#include "util/os.h"

#include "mcbm_insert.h"
#include "mcbm_lookup.h"
#include "mcbm_rangescan.h"
#include "mcbm_update.h"
#include "mcbm_delete.h"

#include <chrono>
#include <thread>

namespace mcbm {

using felis::IndexInfo;
using felis::VHandle;

Config::Config() {
  nr_rows = 10000000; // 10 million
  hotspot_percent = 90; // 90% to hotspots
  hotspot_number = 100000; // 1% of rows are hotspot
  insert_cnt = 0;
}

Config g_mcbm_config;

// insert, lookup, rangescan, update, delete
// currently don't support a mix of insert with others.
static constexpr int kMcBmTxnMix[] = {100, 0, 0, 0, 0};


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

uint64_t ClientBase::PickRow() {
  int rand = RandomNumber(1, 100);
  bool hotspot = false;
  if (rand <= g_mcbm_config.hotspot_percent) {
    hotspot = true;
  }
  if (hotspot) {
    return RandomNumber(0, g_mcbm_config.hotspot_number - 1);
  }
  else {
    return RandomNumber(g_mcbm_config.hotspot_number, g_mcbm_config.nr_rows - 1);
  }
}

uint64_t ClientBase::PickRowNoDup() {
  int idx = g_mcbm_config.insert_cnt.fetch_add(1);
  return g_mcbm_config.insert_row_ids[idx];
}


void McBmLoader::Run()
{
  auto &mgr = util::Instance<felis::TableManager>();
  mgr.Create<MBTable>();

  // shirley: don't load init database if is recovery
  if (felis::Options::kRecovery) {
    done = true;
    return;
  }

  // shirley: don't load init database if it's insert workload
  if (kMcBmTxnMix[0]) {
    done = true;
    return;
  }

  void *large_buf = alloca(1024);

  auto nr_threads = felis::NodeConfiguration::g_nr_threads;
  for (auto t = 0; t < nr_threads; t++) {
    printf("t = %d\n", t);
    felis::MasstreeIndex::ResetThreadInfo();

    mem::ParallelPool::SetCurrentAffinity(t);
    util::Cpu info;
    info.set_affinity(t);
    info.Pin();

    unsigned long start = t * g_mcbm_config.nr_rows / nr_threads;
    unsigned long end = (t + 1) * g_mcbm_config.nr_rows / nr_threads;

    for (uint64_t i = start; i < end; i++) {
      auto key = MBTable::Key::New(i);
      MBTable::Value val;
      val.v = i;

      auto handle = mgr.Get<mcbm::MBTable>().SearchOrCreate(key.EncodeView(large_buf));
      // shirley: init tables. probe transient vs persistent (optional)
      // felis::probes::TransientPersistentCount{true}();

      auto p = handle->vhandle_ptr()->AllocFromInline(val.EncodeSize());
      felis::InitVersion(handle, i, -1, -1, -1, (int)mcbm::TableType::MBTable, val.EncodeToPtrOrDefault(p));
    }
  }
  util::Cpu info;
  info.set_affinity(go::Scheduler::CurrentThreadPoolId() - 1);
  info.Pin();

  mem::ParallelPool::SetCurrentAffinity(-1);
  felis::MasstreeIndex::ResetThreadInfo();

  // shirley: also trigger merge for dptree index
  mgr.Get<mcbm::MBTable>().IndexMerge();

  // shirley: hack: index log ensures any existing merge completes before returning.
  // shirley: sleep first so the merge actually started and we dont actually log anything
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  mgr.Get<mcbm::MBTable>().IndexLog();

  done = true;
}

void McBmLoaderRecovery::DoLoadRecovery() {
  // shirley: recover vhandles if is recovery
  if (felis::Options::kRecovery) {
    void *large_buf = alloca(1024);
    auto &mgr = util::Instance<felis::TableManager>();
    int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    
    // uint64_t curr_ep_stale = util::Instance<EpochManager>().current_epoch_nr();
    uint64_t curr_ep = (mem::GetPmemPersistInfo()->largest_sid >> 32) + 1;
    // shirley: epoch manager haven't advanced yet.
    // printf("EpochManager ep = %lu, pmem ep = %lu\n", curr_ep_stale, curr_ep);

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
      if (table_id == 0)
        continue; // shirley: this row was deleted and reseted during freelist
      int key0 = vhdl_row->key_0;
      switch (table_id) {
        case ((int)mcbm::TableType::MBTable): {
          auto k = mcbm::MBTable::Key::New(key0);
          auto handle = mgr.Get<mcbm::MBTable>().RecoverySearchOrCreate(k.EncodeView(large_buf), vhdl_row);
          break;
        }
        default: {
          printf("mcbm recovery loader unknown table_id = %d\n", table_id);
          std::abort();
          break;
        }
      }
      // shirley: no non-determinism in mcbm. dont need to revert. but need to rebuild major GC list
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


felis::BaseTxn *Client::CreateTxn(uint64_t serial_id, void *txntype_id, void *txn_struct_buffer) {
  int rd = r.next_u32() % 100;
  int txn_type_id = 0;
  while (true) {
    int threshold = kMcBmTxnMix[txn_type_id];
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
    case (int)(mcbm::TxnType::Insert): {
      base_txn = new mcbm::InsertTxn(this, serial_id, (InsertStruct *)txn_struct_buffer);
      break;
    }
    case (int)(mcbm::TxnType::Lookup): {
      base_txn = new mcbm::LookupTxn(this, serial_id, (LookupStruct *)txn_struct_buffer);
      break;
    }
    case (int)(mcbm::TxnType::Rangescan): {
      base_txn = new mcbm::RangescanTxn(this, serial_id, (RangescanStruct *)txn_struct_buffer);
      break;
    }
    case (int)(mcbm::TxnType::Update): {
      base_txn = new mcbm::UpdateTxn(this, serial_id, (UpdateStruct *)txn_struct_buffer);
      break;
    }
    case (int)(mcbm::TxnType::Delete): {
      base_txn = new mcbm::DeleteTxn(this, serial_id, (DeleteStruct *)txn_struct_buffer);
      break;
    }
    default: {
      printf("mcbm CreateTxnRecovery unknown txn_id = %d\n", txntype_id);
      std::abort();
    }
  }
  base_txn->txn_typeid = txntype_id;
  return base_txn;
}

size_t Client::TxnInputSize(int txn_id) {
  size_t input_size;
  switch (txn_id) {
    case (int)(mcbm::TxnType::Insert): {
      input_size = util::Align(sizeof(InsertStruct), 8);
      break;
    }
    case (int)(mcbm::TxnType::Lookup): {
      input_size = util::Align(sizeof(LookupStruct), 8);
      break;
    }
    case (int)(mcbm::TxnType::Rangescan): {
      input_size = util::Align(sizeof(RangescanStruct), 8);
      break;
    }
    case (int)(mcbm::TxnType::Update): {
      input_size = util::Align(sizeof(UpdateStruct), 8);
      break;
    }
    case (int)(mcbm::TxnType::Delete): {
      input_size = util::Align(sizeof(DeleteStruct), 8);
      break;
    }
    default: {
      printf("mcbm TxnInputSize unknown txn_id = %d\n", txn_id);
      std::abort();
    }
  }
  return input_size;
}

void Client::PersistTxnStruct(int txn_id, void *base_txn, void *txn_struct_buffer) {
  switch (txn_id) {
    case (int)(mcbm::TxnType::Insert): {
      InsertStruct txn_struct = *(InsertTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(InsertStruct));
      break;
    }
    case (int)(mcbm::TxnType::Lookup): {
      LookupStruct txn_struct = *(LookupTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(LookupStruct));
      break;
    }
    case (int)(mcbm::TxnType::Rangescan): {
      RangescanStruct txn_struct = *(RangescanTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(RangescanStruct));
      break;
    }
    case (int)(mcbm::TxnType::Update): {
      UpdateStruct txn_struct = *(UpdateTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(UpdateStruct));
      break;
    }
    case (int)(mcbm::TxnType::Delete): {
      DeleteStruct txn_struct = *(DeleteTxn *)base_txn;
      memcpy(txn_struct_buffer, &txn_struct, sizeof(DeleteStruct));
      break;
    }
    default: {
      printf("mcbm PersistTxnStruct unknown txn_id = %d\n", txn_id);
      std::abort();
    }
  }
  return;
}

void Client::IdxMerge() {
  util::Instance<TableManager>().Get<mcbm::MBTable>().IndexMerge();
  return;
}

void Client::IdxLog() {
  util::Instance<TableManager>().Get<mcbm::MBTable>().IndexLog();
  return;
}

} // namespace mcbm
