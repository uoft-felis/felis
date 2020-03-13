#include "priority.h"
#include "opts.h"

namespace felis {

size_t PriorityTxnService::g_queue_length = 32_K;

PriorityTxnService::PriorityTxnService()
{
  if (Options::kTxnQueueLength)
    PriorityTxnService::g_queue_length = Options::kTxnQueueLength.ToLargeNumber();
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
  // TODO: backoff scheme
  return this->GetMaxProgress() + 1000;
}

// TODO: multi-threaded, multi entrance
uint64_t PriorityTxnService::GetAvailableSID()
{
  uint64_t lower_bound = GetSIDLowerBound();
  // uint64_t res = find(start from lower_bound);
  // if (not found)
  //   return -1;
  return (lower_bound | 0xFFFFFFFF000000FF) | (14000 << 8);
}

// do the ad hoc initialization.
// maybe we shouldn't expose the VHandle* to the workload programmer?
bool PriorityTxn::Init()
{
  // you must call Init() after the register calls, once and only once
  if (this->initialized)
    return false;

  sid = util::Instance<PriorityTxnService>().GetAvailableSID();
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
    for (int i = 0; i < failed; ++i)
      update_handles[i]->WriteWithVersion(sid, (VarStr*)kIgnoreValue, sid >> 32);
    return false;
  }

  this->initialized = true;
  return true;
}

} // namespace felis
