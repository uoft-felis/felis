#include "gc.h"
#include "util.h"
#include "log.h"
#include "vhandle.h"
#include "index.h"
#include "node_config.h"
#include "epoch.h"

#include "literals.h"

namespace felis {

mem::ParallelPool GC::g_block_pool;

void GC::InitPool()
{
  g_block_pool = mem::ParallelPool(mem::VhandlePool, Block::kBlockSize, 4096);
}

void GC::AddVHandle(VHandle *handle)
{
  // Either there's nothing to collect, or it's already in the GC queue.
  if (handle->size != 2)
    return;
  VHandleCollectionHandler<GC>::AddVHandle(handle);
}

void GC::RunGC()
{
  cur_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
  VHandleCollectionHandler<GC>::RunHandler();
}

void GC::Process(VHandle *handle)
{
  util::MCSSpinLock::QNode qnode;
  handle->lock.Lock(&qnode);
  Collect(handle);
  handle->lock.Unlock(&qnode);
}

void GC::Collect(VHandle *handle)
{
  auto *versions = handle->versions;
  uintptr_t *objects = handle->versions + handle->capacity;
  int i = 0;
  while (i < handle->size - 1 && (versions[i + 1] >> 32) < cur_epoch_nr) {
    i++;
  }
  for (auto j = 0; j < i; j++) {
    delete (VarStr *) objects[j];
  }
  std::move(objects + i, objects + handle->size, objects);
  std::move(versions + i, versions + handle->size, versions);
  handle->size -= i;
  handle->latest_version.fetch_sub(i);
}

}

namespace util {

using namespace felis;

static GC g_gc;

GC *InstanceInit<GC>::instance = &g_gc;

}
