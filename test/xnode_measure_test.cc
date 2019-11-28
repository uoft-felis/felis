#include <gtest/gtest.h>
#include "benchmark/tpcc/tpcc.h"
#include "txn.h"
#include "benchmark/tpcc/new_order.h"

namespace felis {
class XnodeMeasureTest: public testing::Test {
public:
};

// offload point, < will be migrated to node1, >= will stay on node2, 0 means no offload
const bool offload = true;
const uint district_p = 6;    // district range [1, 10]
const uint stock_p = 16384;   // item range [1, 100000]

// return values
// 0: access to the stock is local
// 1: access to the stock is cross-warehouse (but not cross node)
// 2: access to the stock is cross-node
int DetermineStock(uint home_w_id, uint supply_w_id, uint item_id, int min_w, int max_w, int node_id, uint district_id) {
  if (home_w_id == supply_w_id) {
    if (!offload) {
      return 0;
    }

    if (home_w_id == 5) {
      int execute_node_id = (district_id < district_p) ? 1 : 2;
      if (execute_node_id == 1) {
        if (item_id < stock_p)
          return 0;
        else
          return 2;
      } else { // execute_node_id == 2
        if (item_id < stock_p)
          return 2;
        else
          return 0;
      }
    }

    return 0;
  } else { // home_w_id != supply_w_id
    if (offload && supply_w_id == 5) {
      int stock_on_node = (item_id < stock_p) ? 1 : 2;
      if (node_id != stock_on_node) {
        return 2;
      }
      return 1;
    }
    if (supply_w_id < min_w || supply_w_id > max_w) {
      return 2;
    }
    return 1;
  }
  return -1;
}

TEST_F(XnodeMeasureTest, SimpleTest) {
  const int nr_nodes = 2;
  const int nr_warehouses =  tpcc::g_tpcc_config.nr_warehouses;
  tpcc::g_tpcc_config.hotspot_warehouse_bitmap = 0x00000010; // warehouse 5

  int txn_count[nr_nodes+1] = {0, 300000, 600000};
  int local_txn_count[nr_nodes + 1];
  int xwarehouse_txn_count[nr_nodes + 1];
  int xnode_txn_count[nr_nodes + 1];
  memset(local_txn_count, 0, sizeof(local_txn_count));
  memset(xwarehouse_txn_count, 0, sizeof(xwarehouse_txn_count));
  memset(xnode_txn_count, 0, sizeof(xnode_txn_count));

  int execute_core_id[nr_warehouses + 1];
  memset(execute_core_id, 0, sizeof(execute_core_id));

  for (int node_id = 1; node_id <= nr_nodes; node_id++) {
    static constexpr unsigned long kClientSeed = 0xfeedcabe;
    tpcc::ClientBase *clientBase = new tpcc::ClientBase(kClientSeed, node_id, nr_nodes);

    int min_warehouse = nr_warehouses * (node_id - 1) / nr_nodes + 1;
    int max_warehouse = nr_warehouses * node_id / nr_nodes;

    for (int i = 0; i < txn_count[node_id]; i++) {
      auto input = clientBase->GenerateTransactionInput<tpcc::NewOrderStruct>();

      int level = 0;
      int execute_node_id = node_id; // which node this txn is executed on, 1 or 2

      for (int j = 0; j < input.nr_items; j++) {
        // determine supplier_warehouse_id
        int l = DetermineStock(input.warehouse_id, input.supplier_warehouse_id[j],
          input.item_id[j], min_warehouse, max_warehouse, node_id, input.district_id);
        ASSERT_NE(-1, l);
        level = (l>level) ? l : level;
      }

      if (offload && input.warehouse_id == 5) {
        if (input.district_id < district_p) {
          execute_node_id = 1;
          ++execute_core_id[0];
        }
        else {
          execute_node_id = 2;
          ++execute_core_id[5];
        }
      } else {
        ++execute_core_id[input.warehouse_id];
      }

      switch (level) {
        case 0:
          ++local_txn_count[execute_node_id];
          break;
        case 1:
          ++xwarehouse_txn_count[execute_node_id];
          break;
        case 2:
          ++xnode_txn_count[execute_node_id];
          break;
      }
    }

    if (offload && node_id == 1) {
      printf("\n[node %d-before]local txns: %d\t\tpercentage: %.2f%%\n", node_id, local_txn_count[node_id], (float)100 * local_txn_count[node_id] / txn_count[node_id]);
      printf("[node %d-before]cross warehouse txns: %d\tpercentage: %.2f%%\n", node_id, xwarehouse_txn_count[node_id], (float)100 * xwarehouse_txn_count[node_id] / txn_count[node_id]);
      printf("[node %d-before]cross node txns: %d\t\tpercentage: %.2f%%\n", node_id, xnode_txn_count[node_id], (float)100 * xnode_txn_count[node_id] / txn_count[node_id]);
      printf("[node %d-before]total txns: %d\n", node_id, local_txn_count[node_id] + xwarehouse_txn_count[node_id] + xnode_txn_count[node_id]);
    }
  }

  for (int node_id = 1; node_id <= nr_nodes; node_id++) {
    txn_count[node_id] = local_txn_count[node_id] + xwarehouse_txn_count[node_id] + xnode_txn_count[node_id];
    printf("\n[node %d]local txns: %d\t\tpercentage: %.2f%%\n", node_id, local_txn_count[node_id], (float)100 * local_txn_count[node_id] / txn_count[node_id]);
    printf("[node %d]cross warehouse txns: %d\tpercentage: %.2f%%\n", node_id, xwarehouse_txn_count[node_id], (float)100 * xwarehouse_txn_count[node_id] / txn_count[node_id]);
    printf("[node %d]cross node txns: %d\t\tpercentage: %.2f%%\n", node_id, xnode_txn_count[node_id], (float)100 * xnode_txn_count[node_id] / txn_count[node_id]);
    printf("[node %d]total txns: %d\n", node_id, txn_count[node_id]);
  }

  int total_local_txn = local_txn_count[1] + local_txn_count[2];
  int total_xwarehouse_txn = xwarehouse_txn_count[1] + xwarehouse_txn_count[2];
  int total_xnode_txn = xnode_txn_count[1] + xnode_txn_count[2];
  int total_txn_count = txn_count[1] + txn_count[2];
  printf("\n[total]local txns: %d\t\tpercentage: %.2f%%\n", total_local_txn, (float)100 * total_local_txn / total_txn_count );
  printf("[total]cross warehouse txns: %d\tpercentage: %.2f%%\n",total_xwarehouse_txn, (float)100 * total_xwarehouse_txn / total_txn_count );
  printf("[total]cross node txns: %d\t\tpercentage: %.2f%%\n", total_xnode_txn, (float)100 * total_xnode_txn / total_txn_count );
  printf("[total]total txns: %d\n", total_txn_count);

  /*
  //-----for data analyis
  for (int node_id = 1; node_id <= nr_nodes; node_id++) {
    txn_count[node_id] = local_txn_count[node_id] + xwarehouse_txn_count[node_id] + xnode_txn_count[node_id];
    printf("%d %.5f ", local_txn_count[node_id], (float)1 * local_txn_count[node_id] / txn_count[node_id]);
    printf("%d %.5f ", xwarehouse_txn_count[node_id], (float)1 * xwarehouse_txn_count[node_id] / txn_count[node_id]);
    printf("%d %.5f ", xnode_txn_count[node_id], (float)1 * xnode_txn_count[node_id] / txn_count[node_id]);
  }
  printf("%d %.5f ", total_local_txn, (float)1 * total_local_txn / total_txn_count );
  printf("%d %.5f ",total_xwarehouse_txn, (float)1 * total_xwarehouse_txn / total_txn_count );
  printf("%d %.5f", total_xnode_txn, (float)1 * total_xnode_txn / total_txn_count );

  printf("\n[total]warehouse execution detail:\n");
  for (int i = 1; i <= 4; i++) {
    printf("%d:%d   ", i, execute_core_id[i]);
  }

  if (offload)
    printf("\n5_origin:%d  5_remote:%d  ", execute_core_id[5], execute_core_id[0]);
  else
    printf("\n5:%d  ", execute_core_id[5]);

  for (int i = 6; i <= 8; i++) {
    printf("%d:%d   ", i, execute_core_id[i]);
  }
  printf("\n\n");
  //-----for data analyis
  */
}

}
