#include "slice.h"

namespace felis {

void Slice::SliceQueue::Append(ShippingHandle *handle)
{
  std::unique_lock l(lock, std::defer_lock);
  if (need_lock)
    l.lock();
  handle->InsertAfter(queue.prev);
}

Slice::Slice()
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


}
