#include "slice.h"
#include "log.h"

namespace felis {

void SliceQueue::Append(ShippingHandle *handle)
{
  std::unique_lock l(lock, std::defer_lock);
  if (need_lock)
    l.lock();
  handle->InsertAfter(queue.prev);
}

Slice::Slice(int slice_id) : slice_id(slice_id)
{
  shared_q->need_lock = true;
}

void Slice::Append(ShippingHandle *handle)
{
  // Detect current execution environment
  auto sched = go::Scheduler::Current();
  SliceQueue *q = &shared_q.elem;
  if (sched && !sched->current_routine()->is_share()) {
    int coreid = sched->CurrentThreadPoolId() - 1;
    if (coreid > 0)
      q = &per_core_q[coreid].elem;
  }

  q->Append(handle);
}

SliceMappingTable::SliceMappingTable() {
  std::fill_n(node_compress, NodeConfiguration::kMaxNrNode, -1);
}

void SliceMappingTable::InitNode(int node_id) {
  assert(node_compress[node_id] == -1);

  int compressed_id = nr_nodes++;
  node_compress[node_id] = compressed_id;
  slice_owners[compressed_id].id = node_id;

  logger->info("InitNode [ node_id={}, compressed_id={} ]", node_id, compressed_id);
}

void SliceMappingTable::SetEntry(int slice_id, bool owned, SliceOwnerType type,
                                 int node_id, bool broadcast) {
  if (node_id == -1) {
    node_id = util::Instance<NodeConfiguration>().node_id();
  }

  assert(node_compress[node_id] != -1);
  assert(0 <= slice_id && slice_id < kNrMaxSlices);

  slice_owners[node_compress[node_id]].owned[type][slice_id] = owned;

  if (broadcast) {
    int op = 0;
    assert(0 <= node_id && node_id < (1 << 6));
    assert(0 <= slice_id && slice_id < (1 << 23));
    op |= node_id;
    op <<= 2;
    op |= type;
    op <<= 1;
    op |= owned;
    op <<= 23;
    op |= slice_id;
    broadcast_buffer.push_back(op);
  }
}

void SliceMappingTable::ReplayUpdate(int op) {
  int slice_id = op & ((1 << 23) - 1);
  bool owned = (op >> 23) & 1;
  SliceOwnerType type = static_cast<SliceOwnerType>((op >> 24) & 3);
  int node_id = (op >> 26);
  logger->info("Replaying [ node_id={}, owned={}, type={}, slice={} ]", node_id, owned, type, slice_id);
  SetEntry(slice_id, owned, type, node_id, false);  // Apply with no circular broadcast
}

std::vector<int> SliceMappingTable::LocateNodeInsert(int slice_id, SliceOwnerType type) {
  std::vector<int> nodes;
  for (int i = 0; i < nr_nodes; i++) {
    if (slice_owners[i].owned[type][slice_id]) {
      nodes.push_back(slice_owners[i].id);
    }
  }

  return nodes;
}

int SliceMappingTable::LocateNodeLookup(int slice_id, SliceOwnerType type) {
  assert(0 <= slice_id && slice_id < kNrMaxSlices);

  int ptr;
  do {
    ptr = next_node.load();
    ptr = (ptr + 1) % nr_nodes;
  } while (!slice_owners[ptr].owned[type][slice_id]);

  next_node = ptr;
  return slice_owners[ptr].id;
}

}

namespace util {
  felis::SliceMappingTable *util::InstanceInit<felis::SliceMappingTable>::instance;
}
