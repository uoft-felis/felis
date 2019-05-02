#ifndef TPCC_DELIVERY_H
#define TPCC_DELIVERY_H

#include "tpcc.h"
#include "txn.h"
#include "promise.h"
#include <tuple>

namespace tpcc {

using namespace felis;

struct DeliveryStruct {
  uint warehouse_id;
  uint o_carrier_id;
  uint32_t ts;

  int32_t last_no_o_ids[10];
};

struct DeliveryState {
  VHandle *new_orders[10]; // NewOrder per-district
  VHandle *order_lines[10][15]; // OrderLines per NewOrder
  VHandle *oorders[10];
  VHandle *customers[10];

  NodeBitmap nodes[10];
  struct Completion : public TxnStateCompletion<DeliveryState> {
    Tuple<int> args;
    bool should_skip = false;
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      auto [i] = args;
      if (id == 0) {
        for (int j = 0; j < 15; j++) {
          state->order_lines[i][j] = rows[j];
          if (rows[j] == nullptr) break;
          if (rows[j]->first_version() >= handle.serial_id()) {
            should_skip = true;
          }

          if (!should_skip) handle(rows[j]).AppendNewVersion();
        }
        if (should_skip)
          state->nodes[i] = NodeBitmap();

      } else if (id == 2) {
        state->oorders[i] = rows[0];
        if (!should_skip) handle(rows[0]).AppendNewVersion();
      } else if (id == 3) {
        state->customers[i] = rows[0];
        if (!should_skip) handle(rows[0]).AppendNewVersion();
      } else if (id == 4) {
        state->new_orders[i] = rows[0];
        if (!should_skip) handle(rows[0]).AppendNewVersion();
      }
    }
  };
};

}

#endif
