#include "priority.h"

namespace felis {

uint64_t PriorityTxn::GetDistance(bool backoff)
{
  // if (!backoff) {
  //   return max_progress() + 1000;
  // } else {
  //
  // }
  return -1;
}

bool PriorityTxn::FindAvailableSID()
{
  uint64_t lower_bound = GetDistance();
  // start from lower bound, find one;
  // sid_candidate = found one;
  return false;
}

// find the VHandle in Masstree, append it, return success or not
template <typename Table>
bool PriorityTxn::InitRegisterUpdate(std::vector<typename Table::Key> keys,
                                     std::vector<VHandle*>& handles)
{
  // you cannot InitRegister after you called Init()
  if (initialized)
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
  // TODO: bunch of stuff
  this->initialized = true;
  return true;
}

PriorityTxnService::PriorityTxnService() {
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
    auto r = go::Make([this, i] {
      exec_progress[i] = new uint64_t(0);
    });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }
}

} // namespace felis
