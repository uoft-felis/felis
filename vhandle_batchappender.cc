#include <vector>
#include <thread>

#include "vhandle_batchappender.h"
#include "node_config.h"

namespace felis {

static_assert(sizeof(VersionBuffer) == 16 * 8);

static constexpr size_t kPreAllocCount = 256_K;

static_assert(kPreAllocCount % 64 == 0);

class VersionPrealloc {
  uint8_t *ptr;
 public:
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

struct VersionBufferHead {
  std::atomic_long pos;
  VHandle *backrefs[];

  static size_t MaxPos() {
    return kPreAllocCount / NodeConfiguration::g_nr_threads;
  }

  static size_t PhysicalSize() {
    auto nr_backrefs = MaxPos();
    return util::Align(sizeof(VersionBufferHead) + sizeof(VHandle *) * nr_backrefs, 64);
  }

  VersionPrealloc get_prealloc() {
    auto p = (uint8_t *) this;
    p += PhysicalSize();
    return VersionPrealloc(p);
  }

  void IncrementPos() {
    auto p = pos.fetch_and(1, std::memory_order_release);
    get_prealloc().version_buffers()[p + 1].buf_cnt = 0;
  }

  long GetOrInstallBufferPos(VHandle *handle);
  VersionBuffer *GetOrInstallBuffer(VHandle *handle) {
    auto p = GetOrInstallBufferPos(handle);
    return p == -1 ? nullptr : get_prealloc().version_buffers() + pos;
  }

  // Scan on [from, to) version_buffers in the prealloc. Since BufferHead is
  // per-core, so we don't need to worry about locks inside the buffer.
  void ScanAndFinalize(long from, long to, VHandle **backrefs, uint64_t epoch_nr);
};

long VersionBufferHead::GetOrInstallBufferPos(VHandle *handle)
{
  auto p = handle->buf_pos.load();
  if (p != -1) return p;
  auto new_pos = pos.load(std::memory_order_acquire);
  if (new_pos < MaxPos() && handle->buf_pos.compare_exchange_strong(p, new_pos)) {
    IncrementPos();
    return new_pos;
  } else {
    return p;
  }
}

void VersionBufferHead::ScanAndFinalize(long from, long to, VHandle **backrefs, uint64_t epoch_nr)
{
  auto prealloc = get_prealloc();
  for (long p = from; p < to; p++) {
    prealloc.version_buffers()[p].FlushInto(backrefs[p - from], epoch_nr);
  }
}

void VersionBuffer::FlushInto(VHandle *handle,uint64_t epoch_nr)
{
  util::MCSSpinLock::QNode qnode;
  handle->lock.Acquire(&qnode);

  for (auto i = 0; i < buf_cnt; i++) {
    handle->AppendNewVersion(versions[i], epoch_nr);
  }

  handle->lock.Release(&qnode);
}

BatchAppender::BatchAppender()
{
  auto nr_threads = NodeConfiguration::g_nr_threads;
  auto nr_slots = kPreAllocCount / nr_threads;

  std::vector<std::thread> tasks;
  for (int i = 0; i < nr_threads; i++) {
    tasks.emplace_back(
        [i, nr_slots, this](){
          auto length = VersionBufferHead::PhysicalSize() + VersionPrealloc::PhysicalSize();
          auto head = (VersionBufferHead *) mem::MemMapAlloc(
              mem::EpochQueuePool, length, i / mem::kNrCorePerNode);
          head->pos = i * nr_slots;
          memset(head->get_prealloc().bitmap(), 0, kPreAllocCount / 64);
          head->get_prealloc().version_buffers()[i * nr_slots].buf_cnt = 0;
          buffer_heads[i] = head;
        });
  }
  for (auto &t: tasks) {
    t.join();
  }
}

VersionBuffer *BatchAppender::GetOrInstall(VHandle *handle)
{
  int core = go::Scheduler::CurrentThreadPoolId() - 1;
  return buffer_heads[core]->GetOrInstallBuffer(handle);
}

void BatchAppender::FinalizeFlush(uint64_t epoch_nr)
{
  int core = go::Scheduler::CurrentThreadPoolId() - 1;
  auto current_head = buffer_heads[core];
  auto nr_threads = NodeConfiguration::g_nr_threads;
  auto nr_slots = kPreAllocCount / nr_threads;

  for (int i = 0; i < nr_threads; i++) {
    long from = i * nr_slots;
    long to = buffer_heads[i]->pos.load(std::memory_order_acquire);
    current_head->ScanAndFinalize(from, to, buffer_heads[i]->backrefs, epoch_nr);
  }
  current_head->pos.store(0, std::memory_order_release);
}

}

namespace util {

InstanceInit<felis::BatchAppender>::InstanceInit()
{
  instance = new felis::BatchAppender();
}

}
