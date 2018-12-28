#include "vhandle.h"
#include "vhandle_sync.h"
#include "iface.h"

namespace felis {

void SpinnerSlot::WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver,
                              void *handle)
{
  DTRACE_PROBE1(felis, version_read, handle);

  uintptr_t oldval = *addr;
  if (!IsPendingVal(oldval)) return;
  DTRACE_PROBE1(felis, blocking_version_read, handle);

  int core = go::Scheduler::CurrentThreadPoolId() - 1;
  uint64_t mask = 1ULL << core;

  while (true) {
    uintptr_t val = oldval;
    uintptr_t newval = val & ~mask;
    if (newval == val
        || (oldval = __sync_val_compare_and_swap(addr, val, newval)) == val) {
      Spin(sid, ver);
      oldval = *addr;
    }
    if (!IsPendingVal(oldval))
      return;
    go::Scheduler::Current()->RunNext(go::Scheduler::State::ReadyState);
  }
}

void SpinnerSlot::OfferData(volatile uintptr_t *addr, uintptr_t obj)
{
  auto oldval = *addr;
  auto newval = obj;

  // installing newval
  while (true) {
    uintptr_t val = __sync_val_compare_and_swap(addr, oldval, newval);
    if (val == oldval) break;
    oldval = val;
  }

  // need to notify according to the bitmaps, which is oldval
  uint64_t mask = (1ULL << 32) - 1;
  uint64_t bitmap = mask - (oldval & mask);
  Notify(bitmap);
}


void SpinnerSlot::Spin(uint64_t sid, uint64_t ver)
{
  int idx = go::Scheduler::CurrentThreadPoolId() - 1;
  abort_if(idx < 0, "We should not running on thread pool 0!");

  unsigned long wait_cnt = 0;
  while (!slots[idx].done) {
    asm("pause" : : :"memory");
    wait_cnt++;
    if ((wait_cnt & 0x7FFFFFF) == 0) {
      printf("Deadlock? %lu waiting for %lu\n", sid, ver);
      util::Impl<PromiseRoutineDispatchService>().PrintInfo();
    }
  }

  asm volatile("" : : :"memory");

  DTRACE_PROBE3(felis, wait_jiffies, wait_cnt, sid, ver);
  slots[idx].done = 0;
}

void SpinnerSlot::Notify(uint64_t bitmap)
{
  while (bitmap) {
    int idx = __builtin_ctzll(bitmap);
    slots[idx].done = 1;
    bitmap &= ~(1 << idx);
  }
}

} // namespace felis
