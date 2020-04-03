#include "priority.h"
#include "opts.h"

namespace felis {

size_t PriorityTxnService::g_queue_length = 32_K;
size_t PriorityTxnService::g_slot_percentage = 0;

PriorityTxnService::PriorityTxnService()
{
  if (Options::kTxnQueueLength)
    PriorityTxnService::g_queue_length = Options::kTxnQueueLength.ToLargeNumber();
  if (Options::kSlotPercentage)
    PriorityTxnService::g_slot_percentage = Options::kSlotPercentage.ToInt();
  this->core = 0;
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
    auto r = go::Make([this, i] {
      exec_progress[i] = new uint64_t(0);
    });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }
}

void PriorityTxnService::PushTxn(PriorityTxn* txn) {
  abort_if(!NodeConfiguration::g_priority_txn,
           "[pri] Priority txn is turned off. Why are you trying to push a PriorityTxn?");
  int core_id = this->core.fetch_add(1) % NodeConfiguration::g_nr_threads;
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  svc.Add(core_id, txn);
}

uint64_t PriorityTxnService::GetSIDLowerBound()
{
  uint64_t max = this->GetMaxProgress();
  uint64_t node_id = max & 0xFF, epoch_nr = max >> 32, seq = max >> 8 & 0xFFFFFF;

  // TODO: backoff scheme
  uint64_t new_seq = seq + 1;

  return (epoch_nr << 32) | (new_seq << 8) | node_id;
}

// TODO: multi-threaded, multi entrance
uint64_t PriorityTxnService::GetAvailableSID()
{
  uint64_t lower_bound = GetSIDLowerBound();
  printf("lower_bound: %lu\n", lower_bound);

  uint64_t node_id = lower_bound & 0xFF, epoch_nr = lower_bound >> 32;
  uint64_t seq = lower_bound >> 8 & 0xFFFFFF;

  abort_if(PriorityTxnService::g_slot_percentage <= 0, "pri % is {}",
           PriorityTxnService::g_slot_percentage);
  // every k serial id has 1 slot reversed for priority txn in the back
  int k = 1 + 100 / PriorityTxnService::g_slot_percentage;
  uint64_t new_seq = (seq/k + 1) * k;

  // uint64_t res = find(start from lower_bound);
  // if (not found)
  //   return -1;
  return (epoch_nr << 32) | (new_seq << 8) | node_id;
}

// do the ad hoc initialization.
// maybe we shouldn't expose the VHandle* to the workload programmer?
bool PriorityTxn::Init()
{
  // you must call Init() after the register calls, once and only once
  if (this->initialized)
    return false;

  sid = util::Instance<PriorityTxnService>().GetAvailableSID();
  printf("sid: %lu\n", sid);

  if (sid == -1)
    return false;

  int failed = 0; // if failed, # of handles we need to set to kIgnoreValue
  for (int i = 0; i < update_handles.size(); ++i) {
    // TODO: in other places, why is epoch_nr always given and not calculated
    //       by shifting ? need to ask Mike about it
    update_handles[i]->AppendNewVersion(sid, sid >> 32);
    if (util::Instance<PriorityTxnService>().HasProgressPassed(sid)) {
      failed = i + 1;
      logger->info("[pri] epoch {} txn {} failed on {}", sid >> 32, sid >> 8 & 0xFFFFFF, failed);
      break;
    }
  }
  // or, we only check HasProgressPassed() once, which would be here

  if (failed) {
  // set inserted version to "kIgnoreValue"
    for (int i = 0; i < failed; ++i) {
      update_handles[i]->WriteWithVersion(sid, (VarStr*)kIgnoreValue, sid >> 32);
      printf("reverted handle %p\n", update_handles[i]);
    }
    logger->info("[Pri] reverted {} rows", failed);

    return false;
  }

  this->initialized = true;
  return true;
}

} // namespace felis
