#include <gtest/gtest.h>
#include "benchmark/tpcc/tpcc.h"
#include "txn.h"
#include "benchmark/tpcc/new_order.h"

namespace felis {
class XnodeMeasureTest: public testing::Test {
public:
};


TEST_F(XnodeMeasureTest, SimpleTest) {
  const int nr_nodes = 2;
  const int nr_warehouses =  tpcc::kTPCCConfig.nr_warehouses;

  int txn_count[nr_nodes+1] = {0, 300000, 600000};
  int local_txn_count[nr_nodes + 1];
  int xwarehouse_txn_count[nr_nodes + 1];
  int xnode_txn_count[nr_nodes + 1];

  tpcc::kTPCCConfig.hotspot_warehouse_bitmap = 0x00000010; // warehouse 5

  // offload point, < will be migrated to node1, >= will stay on node2, 0 means no offload
  const bool offload = true;
  const uint district_p = 5;    // district range [1, 10]
  const uint customer_p = 1501; // customer range [1, 3000]
  const uint stock_p = 50001;   // item range [1, 100000]

  //scanf("%d %ud %ud %ud", &offload, &district_p, &customer_p, &stock_p);

  for (int node_id = 1; node_id <= nr_nodes; node_id++) {
    static constexpr unsigned long kClientSeed = 0xfeedcabe;
    tpcc::ClientBase *clientBase = new tpcc::ClientBase(kClientSeed, node_id, nr_nodes);

    local_txn_count[node_id] = 0;
    xwarehouse_txn_count[node_id] = 0;
    xnode_txn_count[node_id] = 0;
    
    int min_warehouse = nr_warehouses * (node_id - 1) / nr_nodes + 1;
    int max_warehouse = nr_warehouses * node_id / nr_nodes;

    for (int i = 0; i < txn_count[node_id]; i++) {
      auto input = clientBase->GenerateTransactionInput<tpcc::NewOrderStruct>();

      bool local = true;
      bool xwarehouse = true;

      for (int j = 0; j < input.nr_items; j++) {
        // determine supplier_warehouse_id  
        int supplier_wid = input.supplier_warehouse_id[j];
        if (supplier_wid != input.warehouse_id) {
          local = false;
          if (supplier_wid < min_warehouse || supplier_wid > max_warehouse) {
            xwarehouse = false;
            ++xnode_txn_count[node_id];
            break; // goto flag(a couple lines under);
          }
        }
      }

      if (!local && !xwarehouse) // flag
        continue; // xnode_txn_count already incremented

      if (offload && node_id == 2 && input.warehouse_id == 5) {
        // determine district
        if (input.district_id < district_p) {
          ++xnode_txn_count[node_id];
          continue;
        }
        // determine customer
        if (input.customer_id < customer_p) {
          ++xnode_txn_count[node_id];
          continue;
        }

        // determine item
        bool breaked = false; 
        for (int j = 0; j < input.nr_items; j++) {
          if (input.item_id[j] < stock_p) {
            ++xnode_txn_count[node_id];
            breaked = true;
            break;
          }
        }
        if (breaked) {
          continue;
        }
      }

      if (local) {
        ++local_txn_count[node_id];
        continue;
      } else if (xwarehouse) {
        ++xwarehouse_txn_count[node_id];
        continue;
      }

    }
  
    printf("[node %d]local txns: %d\t\tpercentage: %.2f%%\n", node_id, local_txn_count[node_id], (float)100 * local_txn_count[node_id] / txn_count[node_id]);
    printf("[node %d]cross warehouse txns: %d\tpercentage: %.2f%%\n", node_id, xwarehouse_txn_count[node_id], (float)100 * xwarehouse_txn_count[node_id] / txn_count[node_id]);
    printf("[node %d]cross node txns: %d\t\tpercentage: %.2f%%\n\n", node_id, xnode_txn_count[node_id], (float)100 * xnode_txn_count[node_id] / txn_count[node_id]);
  }

  int total_local_txn = local_txn_count[1] + local_txn_count[2];
  int total_xwarehouse_txn = xwarehouse_txn_count[1] + xwarehouse_txn_count[2];
  int total_xnode_txn = xnode_txn_count[1] + xnode_txn_count[2];
  int total_txn_count = txn_count[1] + txn_count[2];
  printf("[total]local txns: %d\t\tpercentage: %.2f%%\n", total_local_txn, (float)100 * total_local_txn / total_txn_count );
  printf("[total]cross warehouse txns: %d\tpercentage: %.2f%%\n",total_xwarehouse_txn, (float)100 * total_xwarehouse_txn / total_txn_count );
  printf("[total]cross node txns: %d\t\tpercentage: %.2f%%\n", total_xnode_txn, (float)100 * total_xnode_txn / total_txn_count );
}

}