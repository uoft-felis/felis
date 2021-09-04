#include <math.h>

#include "priority.h"
#include "opts.h"
#include "routine_sched.h"
#include "util/random.h"

namespace felis {

size_t PriorityTxnService::g_queue_length = 32_K;

size_t PriorityTxnService::g_nr_priority_txn;
size_t PriorityTxnService::g_interval_priority_txn;

size_t PriorityTxnService::g_strip_batched = 1;
size_t PriorityTxnService::g_strip_priority = 1;
bool PriorityTxnService::g_sid_global_inc = false;
bool PriorityTxnService::g_sid_local_inc = false;
bool PriorityTxnService::g_sid_bitmap = false;
bool PriorityTxnService::g_tictoc_mode = false;

bool PriorityTxnService::g_read_bit = false;
bool PriorityTxnService::g_conflict_read_bit = false;
bool PriorityTxnService::g_sid_read_bit = false;
bool PriorityTxnService::g_sid_forward_read_bit = false;
bool PriorityTxnService::g_row_rts = false;
bool PriorityTxnService::g_conflict_row_rts = false;
bool PriorityTxnService::g_sid_row_rts = false;
bool PriorityTxnService::g_last_version_patch = false;

int PriorityTxnService::g_dist = -100;
int PriorityTxnService::g_exp_lambda = 2000;
bool PriorityTxnService::g_progress_backoff = true;
bool PriorityTxnService::g_exp_distri_backoff = false;
bool PriorityTxnService::g_lock_insert = false;
bool PriorityTxnService::g_hybrid_insert = false;
bool PriorityTxnService::g_sid_row_wts = false;
bool PriorityTxnService::g_fastest_core = false;
bool PriorityTxnService::g_priority_preemption = true;
bool PriorityTxnService::g_tpcc_pin = true;

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
    double _exec_time, coefficient;
    if (Options::kTpccWarehouses) {
      auto wh_num = Options::kTpccWarehouses.ToInt();
      if (wh_num == 32) { // TPCC 32 warehouses
        _exec_time = 24.8;
        coefficient = 1.1;
      } else if (wh_num == 1) { // TPCC single warehouse
        _exec_time = 76.5;
        coefficient = 5.87;
      }
    } else { // YCSB read-only 8
      _exec_time = 24.95;
      coefficient = 0.3061;
    }
    int percentage = Options::kPercentagePriorityTxn.ToInt();
    abort_if(percentage < 0, "priority transaction percentage cannot be smaller than 0");
    double exec_time = coefficient * double(percentage) + _exec_time;
    logger->info("[Pri-init] estimated exec phase time {} ms", exec_time);
    g_nr_priority_txn = EpochClient::g_txn_per_epoch * percentage / 100;
    g_interval_priority_txn = (percentage == 0) ? 0 : (exec_time * 1000000 / g_nr_priority_txn); // ms to ns
    g_nr_priority_txn *= 1.5; // make sure the actual pct we get is true
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

  if (Options::kTicTocMode) {
    // does not leave slots
    g_tictoc_mode = true;
    abort_if(Options::kSlotPercentage, "TicToc Mode does not need -XSlotPercentage");
    abort_if(Options::kStripPriority, "TicToc Mode does not need -XStripPriority");
    g_strip_batched = 1;
    g_strip_priority = 0;
  } else {
    // leave slots, calculate slot strip width
    if (Options::kSlotPercentage) {
      abort_if(Options::kStripBatched, "either SlotPercentage or StripBatched+StripPriority");
      abort_if(Options::kStripPriority, "either SlotPercentage or StripBatched+StripPriority");
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
  }

  if (Options::kReadBit) {
    g_read_bit = true;
    if (Options::kConflictReadBit)
      g_conflict_read_bit = true;
    if (Options::kSIDReadBit)
      g_sid_read_bit = true;
    if (Options::kSIDForwardReadBit) {
      g_sid_forward_read_bit = true;
      if (Options::kLastVersionPatch) {
        g_last_version_patch = true;
      } else {
        abort_if(Options::kLastVersionPatch, "-XLastVersionPatch requires -XSIDForwardReadBit")
      }
    }
  } else {
    abort_if(Options::kConflictReadBit, "-XConflictReadBit requires -XReadBit");
    abort_if(Options::kSIDReadBit, "-XSIDReadBit requires -XReadBit");
    abort_if(Options::kSIDForwardReadBit, "-XSIDForwardReadBit requires -XReadBit");
    abort_if(Options::kLastVersionPatch, "-XLastVersionPatch requires -XReadBit")
  }

  if (Options::kRowRTS) {
    abort_if(Options::kOnDemandSplitting, "cannot turn on OnDemandSplitting with RowRTS");
    // we borrowed the nr_ondsplt field, so cannot turn that on
    g_row_rts = true;
    if (Options::kConflictRowRTS)
      g_conflict_row_rts = true;
    if (Options::kSIDRowRTS)
      g_sid_row_rts = true;
  } else {
    abort_if(Options::kConflictRowRTS, "-XConflictRowRTS requires -XRowRTS");
    abort_if(Options::kSIDRowRTS, "-XSIDRowRTS requires -XRowRTS");
    abort_if(Options::kLastVersionPatch, "-XLastVersionPatch requires -XRowRTS")
  }
  abort_if(g_conflict_read_bit + g_conflict_row_rts > 1,
           "plz only choose one between ConflictReadBit and ConflictRowRTS");
  abort_if(g_sid_read_bit + g_sid_forward_read_bit + g_sid_row_rts > 1,
           "plz only choose one between SIDReadBit, SIDForwardReadBit & SIDRowRTS");

  int logical_dist = INT_MAX;
  if (Options::kDist) {
    logical_dist = Options::kDist.ToInt(); // independent of priority txn slot ratio
    g_dist = logical_dist * (g_strip_batched + g_strip_priority);
    if (Options::kSIDRowRTS) {
      logger->info("Neglecting Global Backoff Distance for RowRTS");
      g_dist = INT_MAX;
    }
  }
  if (Options::kNoProgressBackoff)
    g_progress_backoff = false;
  if (Options::kExpDistriBackoff) {
    abort_if(!g_row_rts, "-XExpDistriBackoff only works with RowRTS");
    g_progress_backoff = false;
    g_exp_distri_backoff = true;
    if (Options::kExpLambda)
      g_exp_lambda = Options::kExpLambda.ToInt();
    logger->info("[Pri-init] ExpLambda {}", g_exp_lambda);
  }
  if (Options::kLockInsert)
    g_lock_insert = true;
  if (Options::kHybridInsert) {
    g_hybrid_insert = true;
  }

  if (Options::kSIDRowWTS)
    g_sid_row_wts = true;
  logger->info("[Pri-stat] ProgressBackoff {}, ExpDistriBackoff {}, Lock Insert {}, Hybrid Insert {}, SIDRowWTS {}",
               g_progress_backoff, g_exp_distri_backoff, g_lock_insert, g_hybrid_insert, g_sid_row_wts);

  if (Options::kFastestCore)
    g_fastest_core = true;
  logger->info("[Pri-init] Strip: Batched {} + Priority {}, Dist: logical {}, physical {}",
               g_strip_batched, g_strip_priority, logical_dist, g_dist);

  if (Options::kNoPriorityPreempt)
    g_priority_preemption = false;
  if (Options::kNoTpccPin)
    g_tpcc_pin = false;

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
  abort_if(PriorityTxnService::g_tictoc_mode,
           "TicToc Mode cannot call PriorityTxnService::isPriorityTxn to distinguish");
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
  logger->info("[Pri-Stat] NrPriorityTxn: {}  IntervalPriorityTxn: {} ns  physical Dist: {}",
               g_nr_priority_txn, g_interval_priority_txn, g_dist);
  logger->info("[Pri-stat] ProgressBackoff {}, ExpDistriBackoff {}, Lock Insert {}, Hybrid Insert {}, SIDRowWTS {}",
               g_progress_backoff, g_exp_distri_backoff, g_lock_insert, g_hybrid_insert, g_sid_row_wts);

}

uint64_t seq2sid(uint64_t sequence)
{
  auto epoch = util::Instance<EpochManager>().current_epoch_nr();
  auto node_id = util::Instance<NodeConfiguration>().node_id();
  return (epoch << 32) | (sequence << 8) | node_id;
}

uint64_t sid2seq(uint64_t sid) { return sid >> 8 & 0xFFFFFF; }
uint64_t sid2epo(uint64_t sid) { return sid >> 32; }

// find a serial id for the calling priority txn
uint64_t PriorityTxnService::GetSID(PriorityTxn *txn, VHandle **handles, int size)
{
  auto cur_epoch = util::Instance<EpochManager>().current_epoch_nr();
  if (g_sid_row_rts) {
    // get maximum rts from rows
    uint64_t max_rts = 1;
    for (int i = 0; i < size; ++i) {
      auto rts = handles[i]->GetRowRTS();
      max_rts = (rts > max_rts) ? rts : max_rts;
    }
    uint64_t max_rts_epoch = max_rts >> 24;
    uint64_t max_rts_seq;
    if (max_rts_epoch < cur_epoch) {
      max_rts_seq = 1;
    } else {
      abort_if (max_rts_epoch > cur_epoch, "max_rts in epoch {}?", max_rts_epoch);
      max_rts_seq = max_rts & 0xFFFFFF;
    }

    // get maximum wts to enlarge max_rts_seq
    if (g_sid_row_wts) {
      uint64_t max_wts = 1;
      for (int i = 0; i < size; ++i) {
        auto wts = handles[i]->last_priority_version();
        max_wts = (wts > max_wts) ? wts : max_wts;
      }
      uint64_t max_wts_epoch = sid2epo(max_wts);
      uint64_t max_wts_seq;
      if (max_wts_epoch < cur_epoch) {
        max_wts_seq = 1;
      } else {
        abort_if(max_wts_epoch > cur_epoch, "max_wts in epoch {}?", max_wts_epoch);
        max_wts_seq = sid2seq(max_wts);
      }
      if (max_wts_seq > max_rts_seq) max_rts_seq = max_wts_seq;
    }

    uint64_t min_seq = max_rts_seq + 1; // the smallest seq to use, i.e. [min_seq, +inf)
    // backoff by txn->distance
    // g_progress_backoff and g_exp_distri_backoff do this differently
    int &dist = txn->distance;
    if (g_progress_backoff) {
      // dist is backward limit, based on max progress
      uint64_t limit_seq;
      if (dist == INT_MIN) {
        limit_seq = 1;
      } else {
        int max_prog_seq = sid2seq(this->GetMaxProgress());
        if (max_prog_seq + dist < 1)
          limit_seq = 1;
        else
          limit_seq = max_prog_seq + dist;
      }
      min_seq = (min_seq > limit_seq) ? min_seq : limit_seq;
    } else if (g_exp_distri_backoff) {
      // dist is forward backoff
      min_seq = min_seq + dist;
    }

    // backoff by txn->min_sid
    if (!g_exp_distri_backoff) {
      // SID cannot be in previous/same strip as txn->min_sid from last abort
      uint64_t last_time_seq = sid2seq(txn->min_sid);
      if (last_time_seq == 0) last_time_seq = 1;
      int k = g_strip_batched + g_strip_priority;
      if ((min_seq - 1) / k <= (last_time_seq - 1) / k) {
        min_seq = last_time_seq + k;
      }
    }

    if (PriorityTxnService::g_tictoc_mode) {
      return GetNextSID_TicToc(min_seq, txn, handles, size);
    }
    // find available SID after min_seq
    uint64_t available_seq = GetNextSIDSlot(min_seq);
    // record
    if (g_progress_backoff && dist == INT_MIN) {
      int max_prog_seq = sid2seq(this->GetMaxProgress());
      txn->distance = (int)available_seq - max_prog_seq;
      txn->distance_max = abs(txn->distance);
    }
    return seq2sid(available_seq);
  }

  abort_if(g_exp_distri_backoff, "exp. distri. currently only works with row rts");
  int max_prog_seq = sid2seq(this->GetMaxProgress());

  uint64_t min_seq;
  if (max_prog_seq + txn->distance < 1)
    min_seq = 1;
  else
    min_seq = max_prog_seq + txn->distance;

  if (g_sid_read_bit | g_sid_forward_read_bit) {
    // limit search by txn->min_sid
    uint64_t min_sid = seq2sid(min_seq);
    if (txn->min_sid > min_sid)
      min_sid = txn->min_sid;

    // enlarge min_sid by looking at last sid given out
    uint64_t last = 0;
    if (g_sid_global_inc)
      last = this->global_last_sid;
    if (g_sid_local_inc)
      last = *local_last_sid[go::Scheduler::CurrentThreadPoolId() - 1];
    if (last > min_sid)
      min_sid = last;

    // enlarge min_sid by read tracking
    if (g_sid_read_bit) {
      for (int i = 0; i < size; ++i)
        min_sid = handles[i]->SIDBackwardSearch(min_sid);
    } else {
      for (int i = 0; i < size; ++i)
        min_sid = handles[i]->SIDForwardSearch(min_sid);
    }
    if (sid2epo(min_sid) < cur_epoch) // min_sid is from last epoch
      min_seq = 1;
    else
      min_seq = sid2seq(min_sid);
  }
  if (PriorityTxnService::g_tictoc_mode) {
    return GetNextSID_TicToc(min_seq, txn, handles, size);
  }
  uint64_t available_seq = GetNextSIDSlot(min_seq);
  return seq2sid(available_seq);
}

uint64_t PriorityTxnService::GetNextSID_TicToc(uint64_t sequence, PriorityTxn *txn, VHandle **handles, int size)
{
  sequence++;
  while (1) {
    bool success = true;
    for (int i = 0; i < size; ++i) {
      if (handles[i]->IsExistingVersion(seq2sid(sequence))) {
        success = false;
        break;
      }
    }
    if (success)
      return seq2sid(sequence);
    else
      sequence++;
  }
}

// return next slot sequence number
uint64_t PriorityTxnService::GetNextSIDSlot(uint64_t sequence)
{
  uint64_t prog = this->GetMaxProgress();
  int k = this->g_strip_batched + this->g_strip_priority; // strip width

  if (g_sid_global_inc) {
    lock.Lock();
    uint64_t next_slot = (sequence/k + 1) * k;
    // maximum slot ratio it can utilize is priority:batched = 1:1
    uint64_t sid = seq2sid(next_slot);
    if (this->global_last_sid >= sid) {
      next_slot = sid2seq(this->global_last_sid) + k;
      sid = seq2sid(next_slot);
    }
    this->global_last_sid = sid;
    lock.Unlock();
    return next_slot;
  }

  if (g_sid_local_inc) {
    // based on g_strip_priorty = # of cores
    int core_id = go::Scheduler::CurrentThreadPoolId() - 1; // 0~31
    uint64_t next_slot = sequence/k * k + g_strip_batched + core_id + 1;
    uint64_t sid = seq2sid(next_slot);
    auto &last = *local_last_sid[core_id];
    if (last >= sid) {
      next_slot = sid2seq(last) + k;
      sid = seq2sid(next_slot);
    }
    last = sid;
    return next_slot;
  }

  // use per-core bitmap to find an unused SID
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  int idx = seq2idx(sequence);
  int available_idx = seq_bitmap[core_id]->set_first_unset_idx(idx);
  uint64_t min_seq = idx2seq(available_idx, core_id);
  return min_seq;
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

  // AppendNewPriorityVersion in order (here addr order) to prevent deadlock
  std::sort(update_handles, update_handles + usize);

  // 1) acquire SID
  sid = util::Instance<PriorityTxnService>().GetSID(this, update_handles, usize);
  // trace(TRACE_PRIORITY "sid:         {}", format_sid(sid));

  // 2) apply changes, 3) validate, 4) rollback
  int update_cnt = 0, insert_cnt = 0; // count for rollback
  // updates
  for (int i = 0; i < usize; ++i) {
    // pre-checking
    if (CheckUpdateConflict(update_handles[i])) {
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
  if (PriorityTxnService::g_conflict_row_rts) {
    auto rts = handle->GetRowRTS();
    auto wts = this->serial_id() >> 8;
    return wts <= rts;
  }
  return true; // progress passed & no read bit to look precisely, deem it as conflict happened
}

double ran_expo(double lambda){
    static util::FastRandom r(__rdtsc());
    double u = r.next_uniform(); // [0,1)
    return -log(1 - u) / lambda;
}

void PriorityTxn::Rollback(VHandle **update_handles, int update_cnt, VHandle **insert_handles,int insert_cnt) {
  if (PriorityTxnService::g_exp_distri_backoff) {
    // exponential distribution backoff
    int average_transaction_latency = PriorityTxnService::g_exp_lambda;
    distance = ran_expo(1.0/average_transaction_latency);
  }
  else if (PriorityTxnService::g_progress_backoff) {
    if (PriorityTxnService::g_dist < 0 || PriorityTxnService::g_row_rts) {
      // -5 -> -2 -> -1 -> 0 -> 1 -> 2 -> 4 -> 5 -> 5 -> ...
      if (distance < 0) {
        distance /= 2;
      } else if (distance == 0) {
        distance = 1;
      } else {
        distance *= 2;
        if (distance > this->distance_max)
          distance = this->distance_max;
      }
    } else {
      // 0 -> 1 -> 2 -> 4 -> ... -> INT_MAX
      if (distance == 0) {
        distance = 1;
      } else if (distance < INT_MAX / 2) {
        distance *= 2;
      } else {
        distance = INT_MAX;
      }
    }
  }
  for (int i = 0; i < update_cnt; ++i)
    update_handles[i]->WriteWithVersion(sid, (VarStr*)kIgnoreValue, sid >> 32);
  for (int i = 0; i < insert_cnt; ++i)
    insert_handles[i]->WriteWithVersion(sid, nullptr, sid >> 32);
  this->min_sid = this->sid; // do not retry with this sid
  if (PriorityTxnService::g_sid_bitmap && distance >= 0) {
    // since this txn aborted, reset the SID bit back to false
    auto core = go::Scheduler::CurrentThreadPoolId() - 1;
    auto seq = (this->sid >> 8) & 0xFFFFFF;
    auto idx = util::Instance<PriorityTxnService>().seq2idx(seq);
    auto &bitmap = util::Instance<PriorityTxnService>().seq_bitmap[core];
    bitmap->set(idx, false);
  }
}

} // namespace felis
