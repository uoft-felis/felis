#include "gc.h"
#include "util.h"
#include "log.h"
#include "vhandle.h"
#include "index.h"
#include "node_config.h"

namespace felis {

using util::Instance;
using util::InstanceInit;

static InstanceInit<DeletedGarbageHeads> _;

struct DeletedGarbage : public util::ListNode {
  int relation_id;
  VarStr key;
  VHandle *handle;
  uint64_t epoch_nr;

  DeletedGarbage(int relation_id, VarStr key, VHandle *vhandle, uint64_t epoch_nr)
      : relation_id(relation_id), key(key), handle(vhandle), epoch_nr(epoch_nr) {}
};

DeletedGarbageHeads::DeletedGarbageHeads()
{
  for (int i = 0; i < NodeConfiguration::kNrThreads; i++) {
    garbage_heads[i].Initialize();
  }
}

#define PROACTIVE_GC

void DeletedGarbageHeads::AttachGarbage(int rel_id, VarStr key, VHandle *vhandle, uint64_t epoch_nr)
{
#ifdef PROACTIVE_GC
  int idx = go::Scheduler::CurrentThreadPoolId() - 1;
  auto ent = new DeletedGarbage(rel_id, key, vhandle, epoch_nr);
  ent->InsertAfter(&garbage_heads[idx]);
#else
  delete g->key;
  delete g;
#endif
}

void DeletedGarbageHeads::CollectGarbage(uint64_t epoch_nr)
{
#ifdef PROACTIVE_GC
  int idx = go::Scheduler::CurrentThreadPoolId() - 1;
  ListNode *head = &garbage_heads[idx];
  ListNode *ent = head->prev;
  size_t gc_count = 0;
  auto &mgr = Instance<RelationManager>();
  while (ent != head) {
    auto prev = ent->prev;
    auto *entry = (DeletedGarbage *) ent;
    if (epoch_nr - entry->epoch_nr < 2)
      break;
    auto &rel = mgr(entry->relation_id);

    // We don't have to search over the index again, unless this handle is
    // freed.

    // auto handle = rel.Search(&entry->key);
    auto *handle = entry->handle;
    if (handle->last_update_epoch() == entry->epoch_nr) {
      rel.ImmediateDelete(&entry->key);
      // Nobody should be able to reach handle after we immediately delete from
      // the index.
      //
      // However this assumes there should not be double deletion within an
      // epoch. We **must** check if the delete is the last operation in the
      // epoch before marking it as garbage.
      delete handle;
    }

    gc_count++;
    ent->Remove();
    delete entry;

    ent = prev;
  }
  DTRACE_PROBE1(felis, deleted_gc_per_core, gc_count);
  logger->info("Proactive GC {} cleaned {} garbage keys", idx, gc_count);
#endif
}

}
