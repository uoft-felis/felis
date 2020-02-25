#include "priority.h"

namespace felis {

PriorityTxnService::PriorityTxnService()
{
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
    auto r = go::Make([this, i] {
      exec_progress[i] = new uint64_t(0);
    });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }
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
  return lower_bound + 1;
}


// find the VHandle in Masstree, append it, return success or not
template <typename Table>
bool PriorityTxn::InitRegisterUpdate(std::vector<typename Table::Key> keys,
                                     std::vector<VHandle*>& handles)
{
  if (this->initialized)
    return false;

  for (auto key : keys) {
    int table = static_cast<int>(Table::kTable);
    auto &rel = util::Instance<RelationManager>()[table];
    auto handle = rel.SearchOrDefault(nullptr, [] { std::abort(); });
    // it's an update, you should always find it

    this->update_handles.push_back(handle);
    handles.push_back(handle);
  }
  return true;
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
