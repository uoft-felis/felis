#ifndef TPCC_DELIVERY_H
#define TPCC_DELIVERY_H

#include "tpcc.h"
#include "txn_cc.h"
#include "promise.h"
#include "pwv_graph.h"
#include <tuple>
#include <string_view>

namespace tpcc {

using namespace felis;

struct DeliveryStruct {
  uint warehouse_id;
  uint o_carrier_id;
  uint32_t ts;
  int oid[10];
};

struct DeliveryState {
  VHandle *new_orders[10]; // NewOrder per-district
  VHandle *order_lines[10][15]; // OrderLines per NewOrder
  VHandle *oorders[10];
  VHandle *customers[10];

  NodeBitmap nodes[10];
  InvokeHandle<DeliveryState, int, uint32_t> customer_future[10];
  PromiseRoutine *customer_last[10];

  FutureValue<int> sum_future_values[10];

  struct Completion : public TxnStateCompletion<DeliveryState> {
    Tuple<int> args;
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      if (id == 1) return; // End of scan, we don't care here.

      auto [i] = args;
      if (id == 0) {
        for (int j = 0; j < 15; j++) {
          state->order_lines[i][j] = rows[j];
          if (rows[j] == nullptr) break;

          abort_if(rows[j]->ShouldScanSkip(handle.serial_id()),
                   "TPC-C Delivery should not catch up with the NewOrder as the spec requires.");

          handle(rows[j]).AppendNewVersion();
        }
      } else if (id == 2) {
        state->oorders[i] = rows[0];
        // abort_if(rows[0]->ShouldScanSkip(handle.serial_id()), "sid {} row {}", handle.serial_id(), (void *) rows[0]);

        // logger->info("i {} sid {} oorder row {}", i, handle.serial_id(), (void *) rows[0]);
        handle(rows[0]).AppendNewVersion();
      } else if (id == 3) {
        state->customers[i] = rows[0];
        handle(rows[0]).AppendNewVersion(2);
      } else if (id == 4) {
        state->new_orders[i] = rows[0];
        // abort_if(rows[0]->ShouldScanSkip(handle.serial_id()), "sid {} row {}", handle.serial_id(), (void *) rows[0]);
        handle(rows[0]).AppendNewVersion();
      }

      if (Client::g_enable_pwv)
        util::Instance<PWVGraphManager>().local_graph()->AddResource(
            handle.serial_id(), PWVGraph::VHandleToResource(rows[0]));
    }
  };
};

}

#endif
