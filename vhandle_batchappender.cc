#include <vector>
#include <thread>

#include "vhandle_batchappender.h"
#include "node_config.h"

namespace felis {

struct VersionBuffer {
  static constexpr size_t kMaxBatch = 15;
  size_t buf_cnt;
  uint64_t versions[kMaxBatch];
};

static_assert(sizeof(VersionBuffer) == 16 * 8);

static constexpr size_t kPreAllocCount = 256_K;

static_assert(kPreAllocCount % 64 == 0);

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
    return (VersionBuffer *)(ptr + kPreAllocCount / 8);
  }

  static size_t PhysicalSize() {
    return kPreAllocCount / 8 + sizeof(VersionBuffer) * kPreAllocCount;
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

std::array<VersionBufferHeadAllocation, NodeConfiguration::kMaxNrNode / mem::kNrCorePerNode> g_alloc;

// This is a per-core allocator for rows. By moving the `pos`, we can allocate
// buffers to each row.
struct VersionBufferHead {
  static constexpr int kMaxPos = 1024;

  long base_pos;
  std::atomic_int pos;
  int owner_core;
  VersionBufferHead *next_buffer_head;
  VHandle *backrefs[kMaxPos];

  uint8_t *get_prealloc() {
    return g_preallocs[owner_core].ptr;
  }

  void IncrementPos() {
    auto p = pos.fetch_add(1, std::memory_order_release) + 1;
    if (p == kMaxPos)
      return;
    auto prealloc = VersionPrealloc(get_prealloc());
    auto abs_pos = base_pos + p;
    prealloc.version_buffers()[abs_pos].buf_cnt = 0;
    prealloc.bitmap()[abs_pos / 64] &= ~(1ULL << (abs_pos % 64));
  }

  long GetOrInstallBufferPos(BatchAppender *appender, VHandle *handle);
  VersionBufferHandle GetOrInstallBuffer(BatchAppender *appender, VHandle *handle) {
    auto p = GetOrInstallBufferPos(appender, handle);
    return p == -1 ? VersionBufferHandle{nullptr, 0} : VersionBufferHandle{get_prealloc(), p};
  }

  // Scan on [from, to) version_buffers in the prealloc. Since BufferHead is
  // per-core, so we don't need to worry about locks inside the buffer.
  static void ScanAndFinalize(int owner_core, long from, long to, VHandle **backrefs,
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

void VersionBufferHandle::Append(VHandle *handle, uint64_t sid, uint64_t epoch_nr)
{
  VersionPrealloc prealloc(prealloc_ptr);
  auto buf = (prealloc.version_buffers() + pos);
  if (buf->buf_cnt == VersionBuffer::kMaxBatch) {
    util::MCSSpinLock::QNode qnode;
    handle->lock.Acquire(&qnode);

    FlushIntoNoLock(handle, epoch_nr);
    handle->AppendNewVersionNoLock(sid, epoch_nr);

    handle->lock.Release(&qnode);

    return;
  }
  if (buf->buf_cnt == 0) {
    prealloc.bitmap()[pos / 64] |= (1ULL << (pos % 64));
  }
  buf->versions[buf->buf_cnt++] = sid;
}

void VersionBufferHandle::FlushIntoNoLock(VHandle *handle, uint64_t epoch_nr)
{
  VersionPrealloc prealloc(prealloc_ptr);
  auto buf = prealloc.version_buffers() + pos;
  for (auto i = 0; i < buf->buf_cnt; i++) {
    handle->AppendNewVersionNoLock(buf->versions[i], epoch_nr);
  }
  buf->buf_cnt = 0;
  prealloc.bitmap()[pos / 64] &= ~(1ULL << (pos % 64));
}

long VersionBufferHead::GetOrInstallBufferPos(BatchAppender *appender, VHandle *handle)
{
  long p = handle->buf_pos.load();
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
  } else if (handle->buf_pos.compare_exchange_strong(p, base_pos + new_pos)) {
    backrefs[new_pos] = handle;
    IncrementPos();
    return base_pos + new_pos;
  } else {
    return p;
  }
}

void VersionBufferHead::ScanAndFinalize(int owner_core, long from, long to,
                                        VHandle **backrefs, uint64_t epoch_nr,
                                        bool reset)
{
  VersionPrealloc prealloc(g_preallocs[owner_core].ptr);
  auto bitmap = prealloc.bitmap();
  for (long p = from; p < to; p++) {
    if ((bitmap[p / 64] & (1ULL << (p % 64))) == 0)
      continue;

    VersionBufferHandle buf_handle{g_preallocs[owner_core].ptr, p};
    auto vhandle = backrefs[p - from];
    util::MCSSpinLock::QNode qnode;
    vhandle->lock.Acquire(&qnode);
    buf_handle.FlushIntoNoLock(vhandle, epoch_nr);
    vhandle->lock.Release(&qnode);
    if (reset) {
      vhandle->buf_pos.store(-1, std::memory_order_release);
      vhandle->contention_dice = (p - from) % NodeConfiguration::g_nr_threads;
    }
  }
}

BatchAppender::BatchAppender()
{
  auto nr_threads = NodeConfiguration::g_nr_threads;
  auto nr_slots = kPreAllocCount / nr_threads;

  std::vector<std::thread> tasks;
  for (int i = 0; i < nr_threads; i++) {
    tasks.emplace_back(
        [i, nr_slots, this]() {
          auto length = VersionPrealloc::PhysicalSize();
          g_preallocs[i].ptr = (uint8_t *) mem::MemMapAlloc(
              mem::EpochQueuePool, length, i / mem::kNrCorePerNode);
        });
  }
  for (auto &t: tasks) {
    t.join();
  }

  auto nr_numa_zone = nr_threads / mem::kNrCorePerNode;

  auto cap = kPreAllocCount / nr_numa_zone / VersionBufferHead::kMaxPos;
  for (int i = 0; i < nr_numa_zone; i++) {
    auto &al = g_alloc[i];
    al.base_pos = kPreAllocCount / nr_numa_zone * i;
    al.pos = 0;
    al.owner_numa_zone = i;
    al.pool = mem::Pool(mem::EpochQueuePool, sizeof(VersionBufferHead), cap, i);
  }
  buffer_heads.fill(nullptr);
  Reset();
}

VersionBufferHandle BatchAppender::GetOrInstall(VHandle *handle)
{
  int core = go::Scheduler::CurrentThreadPoolId() - 1;
  return buffer_heads[core]->GetOrInstallBuffer(this, handle);
}

void BatchAppender::FinalizeFlush(uint64_t epoch_nr)
{
  int core = go::Scheduler::CurrentThreadPoolId() - 1;
  auto nr_threads = NodeConfiguration::g_nr_threads;
  auto nr_slots = kPreAllocCount / nr_threads;
  for (int i = 0; i < nr_threads; i++) {
    for (auto p = buffer_heads[i]; p; p = p->next_buffer_head) {
      long from = p->base_pos;
      long to = from + p->pos.load(std::memory_order_acquire);
      VersionBufferHead::ScanAndFinalize(core, from, to, p->backrefs, epoch_nr, i == core);
    }
  }
}

void BatchAppender::Reset()
{
  auto nr_threads = NodeConfiguration::g_nr_threads;
  for (int core = 0; core < nr_threads; core++) {
    auto numa_zone = core / mem::kNrCorePerNode;
    auto p = buffer_heads[core];
    for (auto next = p; p; p = next) {
      next = p->next_buffer_head;
      g_alloc[numa_zone].pool.Free(p);
    }
  }
  for (int n = 0; n < nr_threads / mem::kNrCorePerNode; n++) {
    g_alloc[n].pos = 0;
  }
  for (int core = 0; core < nr_threads; core++) {
    buffer_heads[core] = g_alloc[core / mem::kNrCorePerNode].AllocHead(core);
  }
}

}

namespace util {

InstanceInit<felis::BatchAppender>::InstanceInit()
{
  instance = new felis::BatchAppender();
}

}
