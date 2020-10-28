#include <sys/time.h>
#include <syscall.h>
#include "vhandle.h"
#include "vhandle_sync.h"
#include "priority.h"

namespace felis {

static void *AllocateBuffer()
{
  auto nr_zone = NodeConfiguration::g_nr_threads / mem::kNrCorePerNode;
  auto p = (uint8_t *) mmap(
      nullptr,
      4096 * nr_zone,
      PROT_READ | PROT_WRITE,
      MAP_ANONYMOUS | MAP_PRIVATE,
      -1, 0);
  if (p == MAP_FAILED) {
    perror("mmap");
    std::abort();
  }
  for (int i = 0; i < nr_zone; i++) {
    long nodemask = 1 << i;
    if (syscall(__NR_mbind, p + i * 4096, 4096,
                2, &nodemask, sizeof(long) * 8, 1) < 0) {
      perror("mbind");
      std::abort();
    }
  }
  if (mlock(p, 4096 * nr_zone) < 0) {
    perror("mlock");
    std::abort();
  }
  memset(p, 0, 4096 * nr_zone);
  return p;
}

struct SpinnerSlotData {
  std::atomic_bool done;
  long wait_cnt;
  uint8_t __padding__[48];
};

static_assert(sizeof(SpinnerSlotData) == 64);

SpinnerSlotData *SpinnerSlot::slot(int idx)
{
  auto d = std::div(idx, mem::kNrCorePerNode);
  return buffer + 64 * d.quot + d.rem;
}

SpinnerSlot::SpinnerSlot()
{
  buffer = (SpinnerSlotData *) AllocateBuffer();
}

void SpinnerSlot::ClearWaitCountStats()
{
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    slot(i)->wait_cnt = 0;
  }
}

long SpinnerSlot::GetWaitCountStat(int core)
{
  return slot(core)->wait_cnt;
}

void SpinnerSlot::WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver,
                              void *handle)
{
  probes::VersionRead{false, handle}();

  uintptr_t oldval = *addr;
  if (!IsPendingVal(oldval)) return;

  probes::VersionRead{true, handle}();

  int core = go::Scheduler::CurrentThreadPoolId() - 1;
  uint64_t mask = 1ULL << core;
  ulong wait_cnt = 2;

  while (true) {
    uintptr_t val = oldval;
    uintptr_t newval = val & ~mask;
    bool notified = false;
    if ((oldval = __sync_val_compare_and_swap(addr, val, newval)) == val) {
      notified = Spin(sid, ver, wait_cnt, addr);
      oldval = *addr;
    }
    if (!IsPendingVal(oldval)) {
      slot(core)->wait_cnt += wait_cnt;
      return;
    }
  }
}

void SpinnerSlot::OfferData(volatile uintptr_t *addr, uintptr_t obj)
{
  auto oldval = *addr;
  auto newval = obj;

  if (IsPendingVal(oldval) && IsDeletePendingVal(newval))
    newval = (0xFFFFFFFF00000000 & newval) | (((1ULL << 32) - 1) & oldval);

  if (IsDeletePendingVal(oldval) && IsPendingVal(newval))
    newval = (0xFFFFFFFF00000000 & newval) | (((1ULL << 32) - 1) & oldval);

  // installing newval
  while (true) {
    if (!IsPendingVal(oldval)) {
      if (IsDeletePendingVal(oldval) && (!IsPendingVal(newval) || newval != (uintptr_t)nullptr))
        continue; // spin
      if (oldval == (uintptr_t)nullptr)
        std::abort(); // batched txn's write is onto a deleted version, abort
      if (IsPendingVal(newval))
        return; // other core's write beats kDeletePendingVal's install
      logger->critical("strange oldval {0:x}", oldval);
      std::abort();
    }

    uintptr_t val = __sync_val_compare_and_swap(addr, oldval, newval);
    if (val == oldval) break;
    oldval = val;
  }

  // need to notify according to the bitmaps, which is oldval
  uint64_t mask = (1ULL << 32) - 1;
  uint64_t bitmap = mask - (oldval & mask);
  Notify(bitmap);
}

bool SpinnerSlot::Spin(uint64_t sid, uint64_t ver, ulong &wait_cnt, volatile uintptr_t *ptr)
{
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  auto sched = go::Scheduler::Current();
  auto &transport = util::Impl<PromiseRoutineTransportService>();
  auto &dispatch = util::Impl<PromiseRoutineDispatchService>();
  auto routine = sched->current_routine();
  // routine->set_busy_poll(true);

  abort_if(core_id < 0, "We should not run on thread pool 0!");

  while (!slot(core_id)->done.load(std::memory_order_acquire)) {
    wait_cnt++;

    if (unlikely((wait_cnt & 0x7FFFFFF) == 0)) {
      int dep = dispatch.TraceDependency(ver);
      printf("Deadlock on core %d? %lu (using %p) waiting for %lu (%d) node (%lu), ptr %p\n",
             core_id, sid, routine, ver, dep, ver & 0xFF, ptr);
      sleep(600);
    }

    if ((wait_cnt & 0x0FFFF) == 0) {
      // Because periodic flush will run on all cores, we just have to flush our
      // own per-core buffer.
      transport.PeriodicIO(core_id);

      if (((BasePromise::ExecutionRoutine *) routine)->Preempt()) {
        // logger->info("Preempt back");
        return true;
      }
    }
    if (slot(core_id)->done.load(std::memory_order_acquire))
      break;
    _mm_pause();
  }

  probes::WaitCounters{wait_cnt, sid, ver}();
  slot(core_id)->done.store(false, std::memory_order_release);
  return true;
}

void SpinnerSlot::Notify(uint64_t bitmap)
{
  while (bitmap) {
    int idx = __builtin_ctzll(bitmap);
    slot(idx)->done.store(true, std::memory_order_release);
    bitmap &= ~(1 << idx);
  }
}

struct SimpleSyncData {
  long wait_cnt;
  uint8_t __padding__[56];
};

static_assert(sizeof(SimpleSyncData) == 64);

SimpleSync::SimpleSync()
{
  buffer = (SimpleSyncData *) AllocateBuffer();
}

void SimpleSync::WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver, void *handle)
{
  long wait_cnt = 2;
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  auto sched = go::Scheduler::Current();
  auto &transport = util::Impl<PromiseRoutineTransportService>();
  auto &dispatch = util::Impl<PromiseRoutineDispatchService>();
  auto routine = sched->current_routine();

  while (IsPendingVal(*addr)) {
    wait_cnt++;
    if (unlikely((wait_cnt & 0x7FFFFFF) == 0)) {
      int dep = dispatch.TraceDependency(ver);
      printf("Deadlock on core %d? %lu (using %p) waiting for %lu (%d) node (%lu)\n",
             core_id, sid, routine, ver, dep, ver & 0xFF);
      sleep(600);
    }

    if ((wait_cnt & 0x0FFFF) == 0) {
      transport.PeriodicIO(core_id);
      if (((BasePromise::ExecutionRoutine *) routine)->Preempt()) {
        continue;
      }
    }
    if (!IsPendingVal(*addr))
      break;
    _mm_pause();
  }
  auto d = std::div(core_id, mem::kNrCorePerNode);
  buffer[64 * d.quot + d.rem].wait_cnt += wait_cnt;
}

void SimpleSync::ClearWaitCountStats()
{
  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto d = std::div(i, mem::kNrCorePerNode);
    buffer[64 * d.quot + d.rem].wait_cnt = 0;
  }
}

long SimpleSync::GetWaitCountStat(int core)
{
  auto d = std::div(core, mem::kNrCorePerNode);
  return buffer[64 * d.quot + d.rem].wait_cnt;
}

void SimpleSync::OfferData(volatile uintptr_t *addr, uintptr_t obj)
{
  *addr = obj;
}

} // namespace felis
