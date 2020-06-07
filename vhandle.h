// -*- mode: c++ -*-

#ifndef VHANDLE_H
#define VHANDLE_H

#include <atomic>
#include "gopp/gopp.h"
#include "felis_probes.h"
#include "mem.h"
#include "varstr.h"
#include "shipping.h"
#include "entity.h"

namespace felis {

static const uintptr_t kPendingValue = 0xFE1FE190FFFFFFFF; // hope this pointer is weird enough

class VHandleSyncService {
 public:
  static bool g_lock_elision;
  virtual void ClearWaitCountStats() = 0;
  virtual long GetWaitCountStat(int core) = 0;
  // virtual void Notify(uint64_t bitmap) = 0;
  // virtual bool IsPendingVal(uintptr_t val) = 0;
  virtual void WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver, void *handle) = 0;
  virtual void OfferData(volatile uintptr_t *addr, uintptr_t obj) = 0;
};

class BaseVHandle {
 public:
  static mem::ParallelSlabPool pool;

  // Cicada uses inline data to reduce cache misses. These inline rows are much
  // larger: 4-cache lines.
  static mem::ParallelSlabPool inline_pool;
  static void InitPool();
  static void Quiescence() { pool.Quiescence(); inline_pool.Quiescence(); }
 public:

  VHandleSyncService &sync();
};

class RowEntity;
class SliceManager;
class VersionBuffer;

class SortedArrayVHandle : public BaseVHandle {
  friend class VersionBuffer;
  friend class RowEntity;
  friend class SliceManager;
  friend class GC;
  friend class VersionBufferHead;
  friend class VersionBufferHandle;
  friend class BatchAppender;
  friend class HashtableIndex;

  util::MCSSpinLock lock;
  uint8_t alloc_by_regionid;
  uint8_t this_coreid;
  int8_t cont_affinity;
  uint8_t inline_used;

  unsigned int capacity;
  unsigned int size;
  unsigned int cur_start;

  std::atomic_int latest_version; // the latest written version's offset in *versions
  int nr_ondsplt;
  // versions: ptr to the version array.
  // [0, capacity - 1] stores version number, [capacity, 2 * capacity - 1] stores ptr to data
  uint64_t *versions;
  util::OwnPtr<RowEntity> row_entity;
  std::atomic_long buf_pos = -1;
  std::atomic<uint64_t> gc_handle = 0;

  SortedArrayVHandle();
 public:

  static void operator delete(void *ptr) {
    SortedArrayVHandle *phandle = (SortedArrayVHandle *) ptr;
    if (phandle->is_inlined())
      inline_pool.Free(ptr, phandle->this_coreid);
    else
      pool.Free(ptr, phandle->this_coreid);
  }

  static SortedArrayVHandle *New();
  static SortedArrayVHandle *NewInline();

  bool ShouldScanSkip(uint64_t sid);
  void AppendNewVersion(uint64_t sid, uint64_t epoch_nr, bool is_ondemand_split = false);
  VarStr *ReadWithVersion(uint64_t sid);
  VarStr *ReadExactVersion(unsigned int version_idx);
  bool WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr);
  bool WriteExactVersion(unsigned int version_idx, VarStr *obj, uint64_t epoch_nr);
  // void GarbageCollect();
  void Prefetch() const { __builtin_prefetch(versions); }

  bool is_inlined() const { return inline_used != 0xFF; }

  uint8_t *AllocFromInline(size_t sz) {
    if (inline_used != 0xFF) {
      sz = util::Align(sz, 32);
      if (sz > 128) return nullptr;

      uint8_t mask = (1 << (sz >> 5)) - 1;
      for (uint8_t off = 0; off <= 4 - (sz >> 5); off++) {
        if ((inline_used & (mask << off)) == 0) {
          inline_used |= (mask << off);
          return (uint8_t *) this + 128 + (off << 5);
        }
      }
    }
    return nullptr;
  }

  void FreeToInline(uint8_t *p, size_t sz) {
    if (inline_used != 0xFF) {
      sz = util::Align(sz, 16);
      if (sz > 128) return;
      uint8_t mask = (1 << (sz >> 4)) - 1;
      uint8_t off = (p - (uint8_t *) this - 128) >> 4;
      inline_used &= ~(mask << off);
    }
  }

  // These function are racy. Be careful when you are using them. They are perfectly fine for statistics.
  const size_t nr_capacity() const { return capacity; }
  const size_t nr_versions() const { return size; }
  const size_t current_start() const { return cur_start;}
  uint64_t first_version() const { return versions[0]; }
  uint64_t last_version() const { return versions[size - 1]; }
  unsigned int nr_updated() const { return latest_version.load(std::memory_order_relaxed) + 1; }
  int nr_ondemand_split() const { return nr_ondsplt; }
  uint8_t region_id() const { return alloc_by_regionid; }
  uint8_t object_coreid() const { return this_coreid; }
  int8_t contention_affinity() const { return cont_affinity; }
 private:
  void AppendNewVersionNoLock(uint64_t sid, uint64_t epoch_nr, bool is_ondemand_split);
  unsigned int AbsorbNewVersionNoLock(unsigned int end, unsigned int extra_shift);
  void BookNewVersionNoLock(uint64_t sid, unsigned int pos) {
    versions[pos] = sid;
  }
  void IncreaseSize(int delta, uint64_t epoch_nr);
  volatile uintptr_t *WithVersion(uint64_t sid, int &pos);
};

static_assert(sizeof(SortedArrayVHandle) <= 64, "SortedArrayVHandle is larger than a cache line");

#ifdef LL_REPLAY
class LinkListVHandle : public BaseVHandle {
  int this_coreid;
  std::atomic_bool lock;

  struct Entry {
    struct Entry *next;
    uint64_t version;
    uintptr_t object;
    int alloc_by_coreid;

    static mem::Pool *pools;

    static void *operator new(size_t nr_bytes) {
      return pools[mem::CurrentAllocAffinity()].Alloc();
    }

    static void operator delete(void *ptr) {
      Entry *p = (Entry *) ptr;
      pools[p->alloc_by_coreid].Free(ptr);
    }

    static void InitPools();
  };

  static_assert(sizeof(Entry) <= 32, "LinkList VHandle Entry is too large");

  Entry *head; // head is the largest!
  size_t size;

 public:
  static void *operator new(size_t nr_bytes) {
    return pools[mem::CurrentAllocAffinity()].Alloc();
  }

  static void operator delete(void *ptr) {
    auto *phandle = (LinkListVHandle *) ptr;
    pools[phandle->this_coreid].Free(ptr);
  }

  static void InitPools() {
    BaseVHandle::InitPools();
    Entry::InitPools();
  }

  LinkListVHandle();
  LinkListVHandle(LinkListVHandle &&rhs) = delete;

  bool AppendNewVersion(uint64_t sid, uint64_t epoch_nr);
  VarStr *ReadWithVersion(uint64_t sid);
  bool WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run = false);
  void GarbageCollect();

  const size_t nr_versions() const { return size; }
};

static_assert(sizeof(LinkListVHandle) <= 64, "LinkList Handle too large!");

#endif

#ifdef CALVIN_REPLAY

class CalvinVHandle : public BaseVHandle {
  short alloc_by_coreid;
  short this_coreid;

  std::atomic_bool lock;

  size_t capacity;
  size_t size;
  std::atomic_llong pos;
  uint64_t *accesses;
  VarStr *obj;
 public:
  static void *operator new(size_t nr_bytes) {
    return pools[mem::CurrentAllocAffinity()].Alloc();
  }

  static void operator delete(void *ptr) {
    auto *phandle = (CalvinVHandle *) ptr;
    pools[phandle->this_coreid].Free(ptr);
  }

  CalvinVHandle();
  CalvinVHandle(CalvinVHandle &&rhs) = delete;

  bool AppendNewVersion(uint64_t sid, uint64_t epoch_nr);
  bool AppendNewAccess(uint64_t sid, uint64_t epoch_nr, bool is_read = false);
  VarStr *ReadWithVersion(uint64_t sid);
  VarStr *DirectRead(); // for read-only optimization
  bool WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run = false);
  void GarbageCollect();

  const size_t nr_versions() const { return size; }
 private:
  void EnsureSpace();
  uint64_t WaitForTurn(uint64_t sid);
  bool PeekForTurn(uint64_t sid);
};

static_assert(sizeof(CalvinVHandle) <= 64, "Calvin Handle too large!");

#endif

// current relation implementation
#if (defined LL_REPLAY) || (defined CALVIN_REPLAY)

#ifdef LL_REPLAY
clss VHandle : public LinkListVHandle {};
#endif

#ifdef CALVIN_REPLAY
class VHandle : public CalvinVHandle {};
#endif

#else
class VHandle : public SortedArrayVHandle {};

// class VHandle : public SkipListVHandle {};
#endif

}


#endif /* VHANDLE_H */
