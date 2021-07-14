#include "priority.h"
#include "opts.h"
#include "routine_sched.h"

namespace felis {

size_t PriorityTxnService::g_queue_length = 32_K;

size_t PriorityTxnService::g_nr_priority_txn;
size_t PriorityTxnService::g_interval_priority_txn;

size_t PriorityTxnService::g_strip_batched = 1;
size_t PriorityTxnService::g_strip_priority = 1;
bool PriorityTxnService::g_sid_global_inc = false;
bool PriorityTxnService::g_sid_local_inc = false;
bool PriorityTxnService::g_sid_bitmap = false;

bool PriorityTxnService::g_read_bit = false;
bool PriorityTxnService::g_conflict_read_bit = false;
bool PriorityTxnService::g_sid_read_bit = false;
bool PriorityTxnService::g_sid_forward_read_bit = false;

int PriorityTxnService::g_backoff_distance = -100;
bool PriorityTxnService::g_distance_exponential_backoff = true;
bool PriorityTxnService::g_fastest_core = false;

unsigned long long PriorityTxnService::g_tsc = 0;

mem::ParallelSlabPool BaseInsertKey::pool;

PriorityTxnService::PriorityTxnService()
{
  if (Options::kTxnQueueLength)
    g_queue_length = Options::kTxnQueueLength.ToLargeNumber();

  if (Options::kPercentagePriorityTxn) {
    // two ways: you either specify percentage, or specify both # of priTxn per epoch and interval
    if (Options::kNrPriorityTxn || Options::kIntervalPriorityTxn) {
      logger->critical("When PercentagePriorityTxn is specified, "
                       "please do not specify NrPriorityTxn or IntervalPriorityTxn");
      std::abort();
    }
    // _exec_time: time of execution phase when batched epoch size=100k. out of experience
    const double _exec_time = 24.8;
    int percentage = Options::kPercentagePriorityTxn.ToInt();
    abort_if(percentage < 0, "priority transaction percentage cannot be smaller than 0");
    int exec_time = int(_exec_time + double(percentage) * 1.1);
    g_nr_priority_txn = EpochClient::g_txn_per_epoch * percentage / 100;
    g_interval_priority_txn = (percentage == 0) ? 0 : (exec_time * 1000000 / g_nr_priority_txn); // ms to ns
  } else {
    if (!Options::kNrPriorityTxn || !Options::kIntervalPriorityTxn) {
      logger->critical("Please specify both NrPriorityTxn and IntervalPriorityTxn "
                       "(or only specify PercentagePriorityTxn)");
      std::abort();
    }
    g_nr_priority_txn = Options::kNrPriorityTxn.ToInt();
    g_interval_priority_txn = Options::kIntervalPriorityTxn.ToInt();
  }
  logger->info("[Pri-init] NrPriorityTxn: {}  IntervalPriorityTxn: {} ns",
               g_nr_priority_txn, g_interval_priority_txn);

  if (Options::kSlotPercentage) {
    if (Options::kStripBatched || Options::kStripPriority) {
      logger->critical("When SlotPercentage is specified, "
                       "please do not specify StripBatched or StripPriority");
      std::abort();
    }
    size_t slot_percentage = Options::kSlotPercentage.ToInt();
    abort_if(slot_percentage <= 0, "slot percentage <= 0, is {}", slot_percentage);
    int k = 100 / slot_percentage;
    abort_if(100 % slot_percentage != 0,
             "Please give a PriTxn % that can divide 100 with no remainder");
    g_strip_batched = k;
    g_strip_priority = 1;
  } else {
    // for instance, if kStripBatched = 1, kStripPriority = 2
    // then txn 1 is batched txn, txn 2&3 are priority txn slots, ...
    // (txn sequence number starts at 1)
    if (Options::kStripBatched)
      g_strip_batched = Options::kStripBatched.ToInt();
    if (Options::kStripPriority)
      g_strip_priority = Options::kStripPriority.ToInt();
  }

  if (Options::kSIDGlobalInc) g_sid_global_inc = true;
  if (Options::kSIDLocalInc) g_sid_local_inc = true;
  if (Options::kSIDBitmap) g_sid_bitmap = true;
  int count = g_sid_global_inc + g_sid_local_inc + g_sid_bitmap;
  abort_if(count != 1, "Please specify one (and only one) way for SID reuse detection");
  if (g_sid_bitmap || g_sid_local_inc)
    abort_if(NodeConfiguration::g_nr_threads != g_strip_priority,
            "plz make priority strip = # cores"); // allow me to be lazy once

  if (Options::kReadBit) {
    g_read_bit = true;
    if (Options::kConflictReadBit)
      g_conflict_read_bit = true;
    if (Options::kSIDReadBit)
      g_sid_read_bit = true;
    if (Options::kSIDForwardReadBit)
      g_sid_forward_read_bit = true;
  } else {
    abort_if(Options::kConflictReadBit, "-XConflictReadBit requires -XReadBit");
    abort_if(Options::kSIDReadBit, "-XSIDReadBit requires -XReadBit");
    abort_if(Options::kSIDForwardReadBit, "-XSIDForwardReadBit requires -XReadBit");
  }
  abort_if(g_sid_read_bit + g_sid_forward_read_bit > 1,
           "plz only choose one between SIDReadBit and SIDForwardReadBit");

  int logical_dist = -1;
  if (Options::kBackoffDist) {
    logical_dist = Options::kBackoffDist.ToInt(); // independent of priority txn slot ratio
    g_backoff_distance = logical_dist * (g_strip_batched + g_strip_priority);
  }
  if (Options::kNoExpBackoff)
    g_distance_exponential_backoff = false;
  if (Options::kFastestCore)
    g_fastest_core = true;
  logger->info("[Pri-init] Strip: Batched {} + Priority {}, BackoffDist: logical {}, physical {}",
               g_strip_batched, g_strip_priority, logical_dist, g_backoff_distance);

  this->core = 0;
  this->global_last_sid = 0;
  this->epoch_nr = 0;
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
    auto r = go::Make([this, i] {
      // mem from heap may not be NUMA aware, so get them from the pool
      int numa_node = i / mem::kNrCorePerNode;
      auto buf = mem::AllocMemory(mem::MemAllocType::GenericMemory, sizeof(uint64_t), numa_node);
      exec_progress[i] = new (buf) uint64_t(0);
      if (g_sid_bitmap) {
        auto buf = mem::AllocMemory(mem::MemAllocType::GenericMemory, sizeof(Bitmap), numa_node);
        seq_bitmap[i] = new (buf) Bitmap(EpochClient::g_txn_per_epoch);
        // since g_strip_priority = #core (see above, lazy once), bitmap size = # of txn per epoch
      } else {
        seq_bitmap[i] = nullptr;
      }
      if (g_sid_local_inc) {
        auto buf = mem::AllocMemory(mem::MemAllocType::GenericMemory, sizeof(uint64_t), numa_node);
        local_last_sid[i] = new (buf) uint64_t(0);
      } else {
        local_last_sid[i] = nullptr;
      }
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
      // trace(TRACE_PRIORITY "progress passed sid {}, at core {} it's {}", format_sid(sid), i, format_sid(*exec_progress[i]));
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
  int k = PriorityTxnService::g_strip_batched + PriorityTxnService::g_strip_priority;
  if ((seq - 1) % k >= PriorityTxnService::g_strip_batched)
    return true;
  return false;
}

void PriorityTxnService::PrintStats() {
  if (!NodeConfiguration::g_priority_txn)
    return;
  logger->info("[Pri-Stat] NrPriorityTxn: {}  IntervalPriorityTxn: {} ns  physical BackOffDist: {}",
               g_nr_priority_txn, g_interval_priority_txn, g_backoff_distance);
}

// find a serial id for the calling priority txn
uint64_t PriorityTxnService::GetSID(PriorityTxn *txn, VHandle **handles, int size)
{
  uint64_t prog = this->GetMaxProgress(), new_seq;
  int seq = prog >> 8 & 0xFFFFFF;
  int &backoff_dist = txn->backoff_distance;

  if (seq + backoff_dist < 1)
    new_seq = 1;
  else
    new_seq = seq + backoff_dist;

  if (g_sid_read_bit | g_sid_forward_read_bit) {
    uint64_t min = (prog & 0xFFFFFFFF000000FF) | (new_seq << 8);
    if (txn->min_sid > min)
      min = txn->min_sid;

    uint64_t last = 0;
    if (g_sid_global_inc)
      last = this->global_last_sid;
    if (g_sid_local_inc)
      last = *local_last_sid[go::Scheduler::CurrentThreadPoolId() - 1];
    if (last > min)
      min = last;

    if (g_sid_read_bit) {
      for (int i = 0; i < size; ++i)
        min = handles[i]->SIDBackwardSearch(min);
    } else {
      for (int i = 0; i < size; ++i)
        min = handles[i]->SIDForwardSearch(min);
    }
    if (min >> 32 < prog >> 32) // min is from last epoch
      new_seq = 1;
    else
      new_seq = min >> 8 & 0xFFFFFF;
  }
  return GetNextSIDSlot(new_seq);
}

uint64_t PriorityTxnService::GetNextSIDSlot(uint64_t sequence)
{
  uint64_t prog = this->GetMaxProgress();
  int k = this->g_strip_batched + this->g_strip_priority; // strip width

  if (g_sid_global_inc) {
    lock.Lock();
    uint64_t next_slot = (sequence/k + 1) * k;
    // maximum slot ratio it can utilize is priority:batched = 1:1
    uint64_t sid = (prog & 0xFFFFFFFF000000FF) | (next_slot << 8);
    if (this->global_last_sid >= sid) {
      next_slot = (this->global_last_sid >> 8 & 0xFFFFFF) + k;
      sid = (prog & 0xFFFFFFFF000000FF) | (next_slot << 8);
    }
    this->global_last_sid = sid;
    lock.Unlock();
    return sid;
  }

  if (g_sid_local_inc) {
    // based on g_strip_priorty = # of cores
    int core_id = go::Scheduler::CurrentThreadPoolId() - 1; // 0~31
    uint64_t next_slot = sequence/k * k + g_strip_batched + core_id + 1;
    uint64_t sid = (prog & 0xFFFFFFFF000000FF) | (next_slot << 8);
    auto &last = *local_last_sid[core_id];
    if (last >= sid) {
      next_slot = (last >> 8 & 0xFFFFFF) + k;
      sid = (prog & 0xFFFFFFFF000000FF) | (next_slot << 8);
    }
    last = sid;
    return sid;
  }

  // use per-core bitmap to find an unused SID
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  int idx = seq2idx(sequence);
  int available_idx = seq_bitmap[core_id]->set_first_unset_idx(idx);
  uint64_t new_seq = idx2seq(available_idx, core_id);
  return (prog & 0xFFFFFFFF000000FF) | (new_seq << 8);
}

void PriorityTxnService::ClearBitMap(void)
{
  if (!PriorityTxnService::g_sid_bitmap)
    return;
  for (auto i = 0; i < NodeConfiguration::g_nr_threads; ++i) {
    auto r = go::Make([this, i] {
      this->seq_bitmap[i]->clear();
    });
    r->set_urgent(true);
    go::GetSchedulerFromPool(i + 1)->WakeUp(r);
  }
}

// do the ad hoc initialization
// including 1) acquire SID  2) apply changes  3) validate  4) success/rollback
bool PriorityTxn::Init(VHandle **update_handles, int usize, BaseInsertKey **insert_ikeys, int isize, VHandle **insert_handles)
{
  abort_if(this->initialized, "Init() cannot be called after previous Init() succeeded");

  // acquire row lock in order (here addr order) to prevent deadlock
  std::sort(update_handles, update_handles + usize);

  // 1) acquire SID
  sid = util::Instance<PriorityTxnService>().GetSID(this, update_handles, usize);
  // trace(TRACE_PRIORITY "sid:         {}", format_sid(sid));

  // 2) apply changes, 3) validate, 4) rollback
  int update_cnt = 0, insert_cnt = 0; // count for rollback
  // updates
  for (int i = 0; i < usize; ++i) {
    // pre-checking
    if (PriorityTxnService::g_conflict_read_bit && CheckUpdateConflict(update_handles[i])) {
      Rollback(update_handles, update_cnt, insert_handles, insert_cnt);
      return false;
    }
    // apply changes
    if (!update_handles[i]->AppendNewPriorityVersion(sid)) {
      Rollback(update_handles, update_cnt, insert_handles, insert_cnt);
      return false;
    }
    update_cnt++;
    if (CheckUpdateConflict(update_handles[i])) {
      Rollback(update_handles, update_cnt, insert_handles, insert_cnt);
      return false;
    }
  }
  // inserts
  for (int i = 0; i < isize; ++i) {
    // apply changes
    VHandle *handle = nullptr;
    if ((handle = insert_ikeys[i]->Insert(sid)) == nullptr) {
      Rollback(update_handles, update_cnt, insert_handles, insert_cnt);
      return false;
    }
    insert_cnt++;
    insert_handles[i] = handle;
    handle->AppendNewPriorityVersion(sid);
    // batched version array will have 0 version, priority version linked list will have 1 version
  }
  this->initialized = true;
  return true;
}

// return TRUE if update's initialization has conflict with batched txns
bool PriorityTxn::CheckUpdateConflict(VHandle* handle) {
  bool passed = util::Instance<PriorityTxnService>().MaxProgressPassed(this->sid);
  if (!passed)
    return false;
  // if max progress not passed, then read did not happen, does not need to check read bit

  if (PriorityTxnService::g_conflict_read_bit)
    return handle->CheckReadBit(this->sid);
  return true; // progress passed & no read bit to look precisely, deem it as conflict happened
}


void PriorityTxn::Rollback(VHandle **update_handles, int update_cnt, VHandle **insert_handles,int insert_cnt) {
  if (PriorityTxnService::g_distance_exponential_backoff
   && PriorityTxnService::g_backoff_distance < 0) {
    // exponential backoff, -4 -> -2 -> -1 -> 0 -> 1 -> 2 -> ...
    if (backoff_distance < 0) {
      backoff_distance /= 2;
    } else if (backoff_distance == 0) {
      backoff_distance = 1;
    } else {
      backoff_distance *= 2;
      int ori_dist = abs(PriorityTxnService::g_backoff_distance);
      if (backoff_distance > ori_dist)
        backoff_distance = ori_dist;
    }
  }
  for (int i = 0; i < update_cnt; ++i)
    update_handles[i]->WriteWithVersion(sid, (VarStr*)kIgnoreValue, sid >> 32);
  for (int i = 0; i < insert_cnt; ++i)
    insert_handles[i]->WriteWithVersion(sid, nullptr, sid >> 32);
}

} // namespace felis
