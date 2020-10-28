#include "priority.h"
#include "opts.h"

namespace felis {

bool PriorityTxnService::g_read_bit = false;
bool PriorityTxnService::g_fastest_core = false;
bool PriorityTxnService::g_negative_distance = false;
size_t PriorityTxnService::g_queue_length = 32_K;
size_t PriorityTxnService::g_slot_percentage = 0;
size_t PriorityTxnService::g_backoff_distance = 100;
size_t PriorityTxnService::g_nr_priority_txn;
size_t PriorityTxnService::g_interval_priority_txn;

unsigned long long PriorityTxnService::g_tsc = 0;

PriorityTxnService::PriorityTxnService()
{
  if (Options::kReadBit)
    g_read_bit = true;
  if (Options::kFastestCore)
    g_fastest_core = true;
  if (Options::kNegativeDistance)
    g_negative_distance = true;
  if (Options::kTxnQueueLength)
    g_queue_length = Options::kTxnQueueLength.ToLargeNumber();
  if (Options::kSlotPercentage)
    g_slot_percentage = Options::kSlotPercentage.ToInt();
  if (Options::kBackoffDist)
    g_backoff_distance = Options::kBackoffDist.ToInt();


  if (Options::kIncomingRate) {
    // two ways: you either specify incoming rate, or specify both # of priTxn per epoch and interval
    if (Options::kNrPriorityTxn || Options::kIntervalPriorityTxn) {
      logger->critical("When IncomingRate is specified, please do not specify NrPriorityTxn or IntervalPriorityTxn");
      std::abort();
    }
    int incoming_rate = Options::kIncomingRate.ToInt();
    constexpr int exec_time = 85;
    g_nr_priority_txn = incoming_rate * exec_time / 1000;
    // for now, the number of priTxn in exec phase, execution phase takes 85ms, 1s = 1000ms
    abort_if(g_nr_priority_txn == 0, "too less PriTxn in one epoch, please raise IncomingRate");
    g_interval_priority_txn = exec_time * 1000 / g_nr_priority_txn; // ms to us
  } else {
    if (!Options::kNrPriorityTxn || !Options::kIntervalPriorityTxn) {
      logger->critical("Please specify both NrPriorityTxn and IntervalPriorityTxn (or specify IncomingRate)");
      std::abort();
    }
    g_nr_priority_txn = Options::kNrPriorityTxn.ToInt();
    g_interval_priority_txn = Options::kIntervalPriorityTxn.ToInt();
  }
  logger->info("[Pri-init] NrPriorityTxn: {}  IntervalPriorityTxn: {}", g_nr_priority_txn, g_interval_priority_txn);

  this->core = 0;
  this->last_sid = 0;
  this->epoch_nr = 0;
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
    auto r = go::Make([this, i] {
      exec_progress[i] = new uint64_t(0);
    });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }
}

// push a txn into txn queue, round robin
void PriorityTxnService::PushTxn(PriorityTxn* txn) {
  abort_if(!NodeConfiguration::g_priority_txn,
           "[pri] Priority txn is turned off. Why are you trying to push a PriorityTxn?");
  int core_id = this->core.fetch_add(1) % NodeConfiguration::g_nr_threads;
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  svc.Add(core_id, txn); // txn is copied to the core it's adding to
}

std::string format_sid(uint64_t sid)
{
  return "node_id " + std::to_string(sid & 0x000000FF) +
         ", epoch " + std::to_string(sid >> 32) +
         ", txn sequence " + std::to_string(sid >> 8 & 0xFFFFFF);
}

void PriorityTxnService::UpdateProgress(int core_id, uint64_t progress)
{
  if (unlikely(exec_progress[core_id] == nullptr))
    return;
  if (progress > *exec_progress[core_id]) {
    uint64_t old_nr = *exec_progress[core_id] >> 32, new_nr = progress >> 32;
    if (unlikely(new_nr > old_nr)) {
      if (epoch_nr.compare_exchange_strong(old_nr, new_nr))
        PriorityTxnService::g_tsc = __rdtsc();
    }
    *exec_progress[core_id] = progress;
  }
}

void PriorityTxnService::PrintProgress(void)
{
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
    logger->info("progress on core {:2d}: {}", i, format_sid(*exec_progress[i]));
  }
}

uint64_t PriorityTxnService::GetMaxProgress(void)
{
  uint64_t max = 0;
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i)
    max = (*exec_progress[i] > max) ? *exec_progress[i] : max;
  return max;
}

int PriorityTxnService::GetFastestCore(void)
{
  uint64_t max = 0;
  int core = -1;
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
    if (*exec_progress[i] > max) {
      max = *exec_progress[i];
      core = i;
    }
  }
  return core;
}

uint64_t PriorityTxnService::GetProgress(int core_id)
{
  return *exec_progress[core_id];
}

bool PriorityTxnService::MaxProgressPassed(uint64_t sid)
{
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
    if (*exec_progress[i] > sid) {
      // debug(TRACE_PRIORITY "progress passed sid {}, at core {} it's {}", format_sid(sid), i, format_sid(*exec_progress[i]));
      return true;
    }
  }
  return false;
}

bool PriorityTxnService::isPriorityTxn(uint64_t sid) {
  if (!NodeConfiguration::g_priority_txn)
    return false;
  if (sid == 0)
    return false;
  uint64_t seq = sid >> 8 & 0xFFFFFF;
  int k = 100 / PriorityTxnService::g_slot_percentage + 1;
  if (seq % k == 0)
    return true;
  return false;
}

void PriorityTxnService::PrintStats() {
  if (!NodeConfiguration::g_priority_txn)
    return;
  logger->info("[Pri-Stat] NrPriorityTxn: {}  IntervalPriorityTxn: {}  BackOffDist: {}", g_nr_priority_txn, g_interval_priority_txn, g_backoff_distance);
}

// A. how far ahead should we put the priority txn
// TODO: scheme 2, time-adjusting backoff
uint64_t PriorityTxnService::SIDLowerBound()
{
  uint64_t max = this->GetMaxProgress();
  uint64_t node_id = max & 0xFF, epoch_nr = max >> 32, seq = max >> 8 & 0xFFFFFF;
  // debug(TRACE_PRIORITY "max prog:    {}", format_sid(max));

  // scheme 1: backoff fixed distance
  uint64_t new_seq;
  // default false
  if (g_negative_distance) {
    if (seq > g_backoff_distance)
      new_seq = seq - g_backoff_distance;
    else
      new_seq = 0;
  } else {
    new_seq = seq + g_backoff_distance;
  }

  return (epoch_nr << 32) | (new_seq << 8) | node_id;
}

// B. find a serial id for the calling priority txn
uint64_t PriorityTxnService::GetSID(PriorityTxn* txn)
{
  lock.Lock();
  uint64_t lb = SIDLowerBound();
  /* using read bit to acquire SID is way too slow
  if (g_read_bit) {
    uint64_t prev = 0;
    for (int i = 0; i < txn->update_handles.size(); ++i)
      prev = txn->update_handles[i]->GetAvailableSID(prev);
    if (prev < lb) {
      if (prev == 0 || prev >> 32 < lb >> 32)
        lb = lb & 0xFFFFFFFF000000FF;
      else
        lb = prev;
    }
  }
  */
  if (last_sid > lb) lb = last_sid;
  // debug(TRACE_PRIORITY "lower_bound: {}", format_sid(lb));
  uint64_t node_id = lb & 0xFF, epoch_nr = lb >> 32, seq = lb >> 8 & 0xFFFFFF;

  // leave empty slots
  //   every k serial id has 1 slot reserved for priority txn in the back
  //   e.g. percentage=20, then k=6 (1~5 is batched txns, 6 is the slot reserved)
  abort_if(PriorityTxnService::g_slot_percentage <= 0, "pri % is {}",
           PriorityTxnService::g_slot_percentage);
  int k = 100 / PriorityTxnService::g_slot_percentage + 1;
  uint64_t new_seq = (seq/k + 1) * k;
  uint64_t sid = (epoch_nr << 32) | (new_seq << 8) | node_id;
  this->last_sid = sid;
  lock.Unlock();
  return sid;
}



// do the ad hoc initialization
// including 1) acquire SID  2) apply changes  3) validate  4) success/rollback
bool PriorityTxn::Init()
{
  if (this->initialized)
    return false; // you must call Init() after the register calls, once and only once

  // acquire row lock in order (here addr order) to prevent deadlock
  std::sort(update_handles.begin(), update_handles.end());

  // 1) acquire SID
  sid = util::Instance<PriorityTxnService>().GetSID(this);
  // debug(TRACE_PRIORITY "sid:         {}", format_sid(sid));
  if (sid == -1)
    return false; // hack

  // 2) apply changes, 3) validate, 4) rollback
  int update_cnt = 0, delete_cnt = 0, insert_cnt = 0; // count for rollback
  // updates
  for (int i = 0; i < update_handles.size(); ++i) {
    // pre-checking
    if (PriorityTxnService::g_read_bit && CheckUpdateConflict(update_handles[i])) {
      Rollback(update_cnt, delete_cnt, insert_cnt);
      return false;
    }
    // apply changes
    if (!update_handles[i]->AppendNewVersion(sid, sid >> 32, true)) {
      Rollback(update_cnt, delete_cnt, insert_cnt);
      return false;
    }
    update_cnt++;
    if (CheckUpdateConflict(update_handles[i])) {
      Rollback(update_cnt, delete_cnt, insert_cnt);
      return false;
    }
  }
  // deletes
  for (int i = 0; i < delete_handles.size(); ++i) {
    if (PriorityTxnService::g_read_bit) {
      // pre-checking
      if (CheckDeleteConflict(delete_handles[i])) {
        Rollback(update_cnt, delete_cnt, insert_cnt);
        return false;
      }
    }
    // apply changes
    delete_cnt++;
    if (!delete_handles[i]->InitDelete(sid)) {
      Rollback(update_cnt, delete_cnt, insert_cnt);
      return false;
    }
    if (CheckDeleteConflict(delete_handles[i])) {
      Rollback(update_cnt, delete_cnt, insert_cnt);
      return false;
    }
  }
  // inserts: TODO

  this->initialized = true;
  return true;
}

// return TRUE if update's initialization has conflict with batched txns
bool PriorityTxn::CheckUpdateConflict(VHandle* handle) {
  if (PriorityTxnService::g_read_bit)
    return handle->CheckReadBit(this->sid);
  return util::Instance<PriorityTxnService>().MaxProgressPassed(this->sid);
}

bool PriorityTxn::CheckDeleteConflict(VHandle* handle) {
  if (PriorityTxnService::g_read_bit)
    return handle->CheckReadBit(this->sid);
  return util::Instance<PriorityTxnService>().MaxProgressPassed(this->sid);
}

bool PriorityTxn::CheckInsertConflict(VHandle* handle) {
  return true; // TODO
}

void PriorityTxn::Rollback(int update_cnt, int delete_cnt, int insert_cnt) {
  for (int i = 0; i < update_cnt; ++i)
    update_handles[i]->WriteWithVersion(sid, (VarStr*)kIgnoreValue, sid >> 32);
  for (int i = 0; i < delete_cnt; ++i) {
    delete_handles[i]->RevertInitDelete(sid);
  }
  // inserts: TODO
}

} // namespace felis
