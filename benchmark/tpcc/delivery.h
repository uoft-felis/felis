#ifndef TPCC_DELIVERY_H
#define TPCC_DELIVERY_H

#include "tpcc.h"
#include "txn_cc.h"
#include "promise.h"
#include <tuple>
#include <string_view>

namespace tpcc {

using namespace felis;

struct DeliveryStruct {
  uint warehouse_id;
  uint o_carrier_id;
  uint32_t ts;
};

struct DeliveryState {
  uint16_t initialize_aff;
  VHandle *new_orders[10]; // NewOrder per-district
  VHandle *order_lines[10][15]; // OrderLines per NewOrder
  VHandle *oorders[10];
  VHandle *customers[10];

  NodeBitmap nodes[10];
  struct Completion : public TxnStateCompletion<DeliveryState> {
    Tuple<int> args;
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      auto [i] = args;
      if (id == 0) {
        for (int j = 0; j < 15; j++) {
          state->order_lines[i][j] = rows[j];
          if (rows[j] == nullptr) break;
          if (rows[j]->ShouldScanSkip(handle.serial_id())) {
            std::abort();
          }
          handle(rows[j]).AppendNewVersion();
        }
      } else if (id == 2) {
        state->oorders[i] = rows[0];
        // abort_if(rows[0]->ShouldScanSkip(handle.serial_id()), "sid {} row {}", handle.serial_id(), (void *) rows[0]);

        // logger->info("i {} sid {} oorder row {}", i, handle.serial_id(), (void *) rows[0]);
        handle(rows[0]).AppendNewVersion();
      } else if (id == 3) {
        state->customers[i] = rows[0];
        handle(rows[0]).AppendNewVersion();
      } else if (id == 4) {
        state->new_orders[i] = rows[0];
        // abort_if(rows[0]->ShouldScanSkip(handle.serial_id()), "sid {} row {}", handle.serial_id(), (void *) rows[0]);
        handle(rows[0]).AppendNewVersion();
      }
    }
  };
};

}

#endif
