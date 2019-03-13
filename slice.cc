#include "slice.h"

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

void SliceMappingTable::InitNode(int node_id) {
  assert(node_compress[node_id] == -1);

  int compressed_id = nr_nodes++;
  node_compress[node_id] = compressed_id;
  slice_owners[compressed_id].id = node_id;
}

void SliceMappingTable::SetEntry(int slice_id, bool owned, SliceOwnerType type,
                                 int node_id, bool broadcast) {
  if (node_id == -1) {
    node_id = util::Instance<NodeConfiguration>().node_id();
  }

  assert(node_compress[node_id] != -1);
  assert(0 <= slice_id && slice_id < kNrMaxSlices);

  slice_owners[node_compress[node_id]].owned[type][slice_id] = owned;
  // TODO: buffer if broadcast
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
