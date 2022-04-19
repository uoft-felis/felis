#include "mcbm_rangescan.h"

namespace mcbm {

template <>
RangescanStruct ClientBase::GenerateTransactionInput<RangescanStruct>() {
  RangescanStruct s;
  s.row_id = PickRowNoDup();
  // s.row_id = PickRow();
  return s;
}

RangescanTxn::RangescanTxn(Client *client, uint64_t serial_id)
    : Txn<RangescanState>(serial_id),
      RangescanStruct(client->GenerateTransactionInput<RangescanStruct>()),
      client(client) {}

RangescanTxn::RangescanTxn(Client *client, uint64_t serial_id, RangescanStruct *input)
    : Txn<RangescanState>(serial_id),
      client(client) 
{
  RecoverInputStruct(input);
}

void RangescanTxn::Prepare() {
  auto &mgr = util::Instance<TableManager>();
  void *buf = alloca(2048);
  go::RoutineScopedData _(mem::Brk::New(buf, 2048));

  auto key_start = MBTable::Key::New(row_id);
  auto key_end = MBTable::Key::New((uint64_t) std::min(row_id + MAX_SCANRANGE - 1, g_mcbm_config.nr_rows - 1));

  Tuple<uint64_t> arg0 = Tuple<uint64_t>(row_id);
  TxnIndexLookup<DummySliceRouter, RangescanState::Completion>(
              &arg0,
              // KeyParam<MBTable>(key_start));
              RangeParam<MBTable>(key_start, key_end));
}

void RangescanTxn::ReadRow(TxnRow vhandle)
{
    uint64_t value = vhandle.Read<MBTable::Value>().v;
    // printf("value %lu\n", value);
}

void RangescanTxn::Run() {
  auto aff = std::numeric_limits<uint64_t>::max();

  root->AttachRoutine(
    MakeContext(), 1,
    [](const auto &ctx) {
      auto &[state, index_handle] = ctx;
      for (int i = 0; i < MAX_SCANRANGE; i++) {
        if (state->scan_rows[i]) {
          // printf("reading range start: %lu, current_expected[%d]: %lu\n", state->rowid, i, state->rowid + i);
          ReadRow(index_handle(state->scan_rows[i]));
        }
        else {
          break;
        }
      }
    },
    aff
  );

  // shirley zen: add sfence after txn run
  if (felis::Options::kEnableZen) {
    // _mm_sfence();
  }
}

} // namespace mcbm
