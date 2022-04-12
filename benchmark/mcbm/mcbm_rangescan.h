#ifndef MCBM_RANGESCAN_H
#define MCBM_RANGESCAN_H

#include "mcbm.h"
#include "txn_cc.h"

#include "piece_cc.h"
#include "pwv_graph.h"
#include <tuple>
#include <string_view>

namespace mcbm {

using namespace felis;

#define MAX_SCANRANGE 100 // shirley note: must make sure range is <= kMaxRangeScanKeys in txn.h

struct RangescanStruct {
  uint64_t row_id;
};

struct RangescanState {
  IndexInfo *scan_rows[MAX_SCANRANGE];
  uint64_t rowid = -1;
  struct Completion : public TxnStateCompletion<RangescanState> {
    Tuple<uint64_t> args;
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      if (id == 1) {
        return; // End of scan, we don't care here.
      }
      
      state->rowid = args.value;

      if (id == 0) {
        for (int i = 0; i < MAX_SCANRANGE; i++) {
          state->scan_rows[i] = rows[i];
          if (rows[i] == nullptr) break;
        }
      }
      else {
        printf("RangescanState Completion: unknown id %d\n", id);
        std::abort();
      }
    }
  };
};

class RangescanTxn : public Txn<RangescanState>, public RangescanStruct {
  Client *client;

public:
  RangescanTxn(Client *client, uint64_t serial_id);
  RangescanTxn(Client *client, uint64_t serial_id, RangescanStruct *input);

  static void ReadRow(TxnRow vhandle);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}
  void RecoverInputStruct(RangescanStruct *input) {
    this->row_id = input->row_id;
  }
};

} // namespace mcbm

#endif
