#include <vector>
#include <thread>

#include "contention_manager.h"
#include "gc.h"
#include "node_config.h"
#include "opts.h"
#include "epoch.h"

namespace felis {

struct VersionBuffer {
  static constexpr size_t kMaxBatch = 255;
  uint32_t buf_cnt;
  uint32_t ondsplt_cnt;
  uint64_t versions[kMaxBatch];
};

static_assert(sizeof(VersionBuffer) % 64 == 0);

size_t ContentionManager::g_prealloc_count = 256_K;

// Per-core buffer for each row. Each row needs a fixed size per core
// buffer. This class represent all buffer for all rows for only one core.
//
// At the beginning of this class, there is a bitmap for each row. This bit
// represent if the buffer size on this core is >= 0 or not.
struct VersionPrealloc {
  uint8_t *ptr;

  VersionPrealloc() : ptr(nullptr) {}
  VersionPrealloc(uint8_t *p) : ptr(p) {}

  uint64_t *bitmap() {
    return (uint64_t *) ptr;
  }
  VersionBuffer *version_buffers() {
    return (VersionBuffer *)(ptr + ContentionManager::g_prealloc_count / 8);
  }

  static size_t PhysicalSize() {
    return ContentionManager::g_prealloc_count / 8 + sizeof(VersionBuffer) * ContentionManager::g_prealloc_count;
  }
};

std::array<VersionPrealloc, NodeConfiguration::kMaxNrThreads> g_preallocs;

struct VersionBufferHeadAllocation {
  long base_pos;
  std::atomic_int pos;
  int owner_numa_zone;
  mem::Pool pool;

  VersionBufferHead *AllocHead(int owner_core);
};

std::array<VersionBufferHeadAllocation, kMaxNrNode / mem::kNrCorePerNode> g_alloc;

// This is a per-core allocator for rows. By moving the `pos`, we can allocate
// buffers to each row.
struct VersionBufferHead {
  static constexpr int kMaxPos = 8192;

  long base_pos;
  std::atomic_int pos;
  int owner_core;
  VersionBufferHead *next_buffer_head;
  IndexInfo *backrefs[kMaxPos];

  uint8_t *get_prealloc() {
    return g_preallocs[owner_core].ptr;
  }

  void IncrementPos() {
    auto p = pos.fetch_add(1, std::memory_order_release) + 1;
    if (p == kMaxPos)
      return;
    auto prealloc = VersionPrealloc(get_prealloc());
    auto abs_pos = base_pos + p;
    auto &buf = prealloc.version_buffers()[abs_pos];
    buf.buf_cnt = buf.ondsplt_cnt = 0;
    prealloc.bitmap()[abs_pos / 64] &= ~(1ULL << (abs_pos % 64));
  }

  long GetOrInstallBufferPos(ContentionManager *appender, IndexInfo *handle);
  VersionBufferHandle GetOrInstallBuffer(ContentionManager *appender, IndexInfo *handle) {
    auto p = GetOrInstallBufferPos(appender, handle);
    return p == -1 ? VersionBufferHandle{nullptr, 0} : VersionBufferHandle{get_prealloc(), p};
  }

  // Scan on [from, to) version_buffers in the prealloc. Since BufferHead is
  // per-core, so we don't need to worry about locks inside the buffer.
  static void ScanAndFinalize(int owner_core, long from, long to, IndexInfo **backrefs,
                              uint64_t epoch_nr, bool reset);
};

VersionBufferHead *VersionBufferHeadAllocation::AllocHead(int owner_core)
{
  auto p = (VersionBufferHead *) pool.Alloc();
  p->base_pos = base_pos + pos.fetch_add(VersionBufferHead::kMaxPos);
  p->pos = 0;
  p->owner_core = owner_core;
  p->next_buffer_head = nullptr;
  return p;
}

void VersionBufferHandle::Append(IndexInfo *handle, uint64_t sid, uint64_t epoch_nr,
                                 bool is_ondemand_split)
{
  VersionPrealloc prealloc(prealloc_ptr);
  util::MCSSpinLock::QNode qnode;

  auto buf = (prealloc.version_buffers() + pos);
  if (buf->buf_cnt == VersionBuffer::kMaxBatch) {
    handle->lock.Acquire(&qnode);

    handle->AppendNewVersionNoLock(sid, epoch_nr, is_ondemand_split);
    if (is_ondemand_split) handle->vhandle_ptr()->nr_ondsplt++;

    handle->IncreaseSize(buf->buf_cnt, epoch_nr);
    auto end = handle->size_get(handle->versions) - buf->buf_cnt;
    // handle->IncreaseSize(VersionBuffer::kMaxBatch + 1);
    // handle->BookNewVersionNoLock(sid, end);

    // end = handle->AbsorbNewVersionNoLock(end, VersionBuffer::kMaxBatch);

    FlushIntoNoLock(handle, epoch_nr, end);
    handle->lock.Release(&qnode);

    return;
  }
  if (buf->buf_cnt == 0) {
    prealloc.bitmap()[pos / 64] |= (1ULL << (pos % 64));
  }
  buf->versions[buf->buf_cnt++] = sid;
  if (is_ondemand_split) buf->ondsplt_cnt++;

  if (buf->buf_cnt > VersionBuffer::kMaxBatch / 2
      && handle->lock.TryLock(&qnode)) {
    handle->IncreaseSize(buf->buf_cnt, epoch_nr);
    auto end = handle->size_get(handle->versions) - buf->buf_cnt;
    FlushIntoNoLock(handle, epoch_nr, end);
    handle->lock.Release(&qnode);
  }
}

void VersionBufferHandle::FlushIntoNoLock(IndexInfo *handle, uint64_t epoch_nr, unsigned int end)
{
  VersionPrealloc prealloc(prealloc_ptr);
  auto buf = prealloc.version_buffers() + pos;
  std::sort(buf->versions, buf->versions + buf->buf_cnt);
  for (int i = buf->buf_cnt - 1; i >= 0; i--) {
    handle->BookNewVersionNoLock(buf->versions[i], end);
    // printf("absorb %d %d %lu %p\n", end, i, buf->versions[i], handle);
    end = handle->AbsorbNewVersionNoLock(end, i);
  }
  handle->vhandle_ptr()->nr_ondsplt += buf->ondsplt_cnt;
  buf->buf_cnt = 0;
  buf->ondsplt_cnt = 0;
  prealloc.bitmap()[pos / 64] &= ~(1ULL << (pos % 64));
}

long VersionBufferHead::GetOrInstallBufferPos(ContentionManager *appender, IndexInfo *handle)
{
  long p = handle->vhandle_ptr()->buf_pos.load();
  if (p != -1) return p;
  long new_pos = pos.load(std::memory_order_acquire);
  if (new_pos >= kMaxPos) {
    // Allocate a new buffer head and attach it to the appender. We are on
    // owner_core right now!
    auto new_buf_head = g_alloc[owner_core / mem::kNrCorePerNode].AllocHead(owner_core);
    if (new_buf_head == nullptr) {
      // logger->info("core {} needs more batchappender buffer", owner_core);
      return -1;
    }
    new_buf_head->next_buffer_head = this;
    appender->buffer_heads[owner_core] = new_buf_head;
    return new_buf_head->GetOrInstallBufferPos(appender, handle);
  } else if (handle->vhandle_ptr()->buf_pos.compare_exchange_strong(p, base_pos + new_pos)) {
    backrefs[new_pos] = handle;
    IncrementPos();
    return base_pos + new_pos;
  } else {
    return p;
  }
}

void VersionBufferHead::ScanAndFinalize(int owner_core, long from, long to,
                                        IndexInfo **backrefs, uint64_t epoch_nr,
                                        bool reset)
{
  VersionPrealloc prealloc(g_preallocs[owner_core].ptr);
  auto bitmap = prealloc.bitmap();
  int retry = 0;
  bool disable_trylock = false;

  do {
    retry = 0;
    for (long p = from; p < to; p++) {
      auto vhandle = backrefs[p - from];
      if (reset) {
        vhandle->vhandle_ptr()->buf_pos.store(-1, std::memory_order_release);
      }

      if ((bitmap[p / 64] & (1ULL << (p % 64))) == 0)
        continue;

      VersionBufferHandle buf_handle{g_preallocs[owner_core].ptr, p};
      util::MCSSpinLock::QNode qnode;
      auto buf = prealloc.version_buffers() + p;

      if (disable_trylock) {
        vhandle->lock.Acquire(&qnode);
      } else if (!vhandle->lock.TryLock(&qnode)) {
        retry++;
        continue;
      }
      vhandle->IncreaseSize(buf->buf_cnt, epoch_nr);
      buf_handle.FlushIntoNoLock(vhandle, epoch_nr, vhandle->size_get(vhandle->versions) - buf->buf_cnt);
      vhandle->lock.Release(&qnode);
    }

    if (retry == 1) disable_trylock = true;
  } while (retry > 0);
}

ContentionManager::ContentionManager()
{
  auto nr_threads = NodeConfiguration::g_nr_threads;
  auto nr_slots = g_prealloc_count / nr_threads;

  std::vector<std::thread> tasks;
  for (int i = 0; i < nr_threads; i++) {
    tasks.emplace_back(
        [i, nr_slots, this]() {
          auto length = VersionPrealloc::PhysicalSize();
          g_preallocs[i].ptr = (uint8_t *) mem::AllocMemory(
              mem::ContentionManagerPool, length, i / mem::kNrCorePerNode);
        });
  }
  for (auto &t: tasks) {
    t.join();
  }

  est_split = 0;

  auto nr_numa_zone = nr_threads / mem::kNrCorePerNode;

  auto cap = g_prealloc_count / nr_numa_zone / VersionBufferHead::kMaxPos;
  for (int i = 0; i < nr_numa_zone; i++) {
    auto &al = g_alloc[i];
    al.base_pos = g_prealloc_count / nr_numa_zone * i;
    al.pos = 0;
    al.owner_numa_zone = i;
    al.pool = mem::Pool(mem::ContentionManagerPool, sizeof(VersionBufferHead), cap, i);
  }
  buffer_heads.fill(nullptr);
  Reset();
}

VersionBufferHandle ContentionManager::GetOrInstall(IndexInfo *handle)
{
  int core = go::Scheduler::CurrentThreadPoolId() - 1;
  return buffer_heads[core]->GetOrInstallBuffer(this, handle);
}

void ContentionManager::FinalizeFlush(uint64_t epoch_nr)
{
  int core = go::Scheduler::CurrentThreadPoolId() - 1;
  auto nr_threads = NodeConfiguration::g_nr_threads;
  auto nr_slots = g_prealloc_count / nr_threads;
  for (int i = 0; i < nr_threads; i++) {
    for (auto p = buffer_heads[i]; p; p = p->next_buffer_head) {
      long from = p->base_pos;
      long to = from + p->pos.load(std::memory_order_acquire);

      // We used to set reset to true to early end the batch appender. It seems
      // a bit slower than reset in a single thread in Reset().
      VersionBufferHead::ScanAndFinalize(core, from, to, p->backrefs, epoch_nr, false);
    }
  }
}

void Binpack(IndexInfo **knapsacks, unsigned int nr_knapsack, int label, size_t limit);
void PackLeftOver(IndexInfo **knapsacks, unsigned int nr_knapsack, int label);

void ContentionManager::Reset()
{
  auto nr_threads = NodeConfiguration::g_nr_threads;
  unsigned int sum = 0, nr_cleared = 0, nr_splitted = 0;

  for (int core = 0; core < nr_threads; core++) {
    auto p = buffer_heads[core];
    for (auto next = p; p; p = next) {
      next = p->next_buffer_head;

      // Contention management
      for (long i = 0; i < p->pos.load(std::memory_order_acquire); i++) {
        auto row = p->backrefs[i];
        row->vhandle_ptr()->buf_pos.store(-1, std::memory_order_release);
        nr_cleared++;

        if (!Options::kOnDemandSplitting) continue;

        if (row->size_get(row->versions) - row->nr_updated() <= EpochClient::g_splitting_threshold) continue;
        sum += row->vhandle_ptr()->nr_ondsplt;
        nr_splitted++;
      }
    }
  }

  if (sum < NodeConfiguration::g_nr_threads) {
    sum = 0;
  }

  est_split = sum;

  IndexInfo **knapsacks = nullptr;
  size_t nr_knapsacks = 0;
  unsigned int s = 0;
  if (Options::kBinpackSplitting) {
    knapsacks = new IndexInfo *[nr_cleared];
  }

  for (int core = 0; core < nr_threads; core++) {
    auto numa_zone = core / mem::kNrCorePerNode;
    auto p = buffer_heads[core];

    for (auto next = p; p; p = next) {
      next = p->next_buffer_head;

      if (sum == 0)
        goto done;

      for (long i = 0; i < p->pos.load(std::memory_order_acquire); i++) {
        auto row = p->backrefs[i];
        if (row->size_get(row->versions) - row->nr_updated() <= EpochClient::g_splitting_threshold) continue;

        row->vhandle_ptr()->cont_affinity =
            NodeConfiguration::g_nr_threads *
            (s + row->vhandle_ptr()->nr_ondsplt / 2) / sum;
        s += row->vhandle_ptr()->nr_ondsplt;

        auto client = EpochClient::g_workload_client;
        // client->get_execution_locality_manager().PlanLoad(row->this_coreid, -1 * row->nr_ondsplt);
        if (Options::kBinpackSplitting) {
          knapsacks[nr_knapsacks++] = row;
          continue;
        }
        client->get_contention_locality_manager().PlanLoad(
            row->vhandle_ptr()->cont_affinity, row->vhandle_ptr()->nr_ondsplt);
      }
   done:
      g_alloc[numa_zone].pool.Free(p);
    }

  }

  int delta = 0;
  if (Options::kBinpackSplitting && sum > 0) {
    // Exclude the Binpacking time
    EpochClient::g_workload_client->perf.End();
    for (int core = 0; core < nr_threads - 1; core++) {
      const size_t hard_limit = sum / nr_threads;
      delta = hard_limit + delta - BinPack(knapsacks, nr_knapsacks, core, hard_limit + 1 + delta);
    }
    PackLeftOver(knapsacks, nr_knapsacks, nr_threads - 1);
    EpochClient::g_workload_client->perf.Start();
  }

  for (int n = 0; n < nr_threads / mem::kNrCorePerNode; n++) {
    g_alloc[n].pos = 0;
  }

  for (int core = 0; core < nr_threads; core++) {
    buffer_heads[core] = g_alloc[core / mem::kNrCorePerNode].AllocHead(core);
  }
  if (Options::kOnDemandSplitting) {
    logger->info("OnDemand {} Splitted/Batch {}/{} rows", s, nr_splitted, nr_cleared);
    felis::probes::OnDemandSplit{s, nr_cleared, nr_splitted}();
  } else {
    logger->info("Batch {} rows", nr_cleared);
  }

  delete [] knapsacks;
}

size_t ContentionManager::BinPack(IndexInfo **knapsacks, unsigned int nr_knapsack, int label, size_t limit)
{
  if (limit == 0) return 0;

  int *f = new int[limit];
  int *pf = new int[limit];
  auto *trace = new std::vector<bool>[nr_knapsack];
  std::fill(pf, pf + limit, 0);

  for (auto i = 0U; i < nr_knapsack; i++) {
    if (knapsacks[i] == nullptr) continue;

    trace[i].resize(limit);
    auto wi = knapsacks[i]->vhandle_ptr()->nr_ondemand_split();
    for (size_t w = 0; w < limit; w++) {
      f[w] = pf[w];
      if (w >= wi && pf[w - wi] + wi > f[w]) {
        f[w] = pf[w - wi] + wi;
        trace[i][w] = true;
      }
    }
    std::swap(pf, f);
  }

  int maxcap = pf[limit - 1];
  printf("Binpack max cap %d/%ld:", maxcap, limit);
  auto w = limit - 1;
  for (int i = nr_knapsack - 1; i >= 0; i--) {
    if (knapsacks[i] == nullptr) continue;

    auto wi = knapsacks[i]->vhandle_ptr()->nr_ondemand_split();
    if (trace[i][w]) {
      printf(" %d", wi);
      w -= wi;
      knapsacks[i]->vhandle_ptr()->cont_affinity = label;
      knapsacks[i] = nullptr;
    }
  }
  puts("");

  delete [] f;
  delete [] pf;
  delete [] trace;
  return maxcap;
}

void ContentionManager::PackLeftOver(IndexInfo **knapsacks, unsigned int nr_knapsack, int label)
{
  printf("Binpack left:");
  for (int i = 0; i < nr_knapsack; i++) {
    if (knapsacks[i] != nullptr) {
      printf(" %d", knapsacks[i]->vhandle_ptr()->nr_ondemand_split());
      knapsacks[i]->vhandle_ptr()->cont_affinity = label;
    }
  }
  puts("");
}

}

namespace util {

InstanceInit<felis::ContentionManager>::InstanceInit()
{
  instance = new felis::ContentionManager();
}

}
