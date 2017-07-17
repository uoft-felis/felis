// -*- mode: c++ -*-

#include <sys/sdt.h>
#include "epoch.h"

#ifndef VHANDLE_H
#define VHANDLE_H

namespace dolly {

static const uintptr_t kPendingValue = 0xFE1FE190FFFFFFFF; // hope this pointer is weird enough

struct ErmiaEpochGCRule {
  uint64_t last_gc_epoch;
  uint64_t min_of_epoch;

  ErmiaEpochGCRule() : last_gc_epoch(0), min_of_epoch(0) {}

  template <typename VHandle>
  void operator()(VHandle &handle, uint64_t sid) {
    uint64_t ep = Epoch::CurrentEpochNumber();
    if (ep > last_gc_epoch) {
      // gaurantee that we're the *first one* to garbage collect at the *epoch boundary*.
      handle.GarbageCollect();
      DTRACE_PROBE3(dolly, versions_per_epoch_on_gc, &handle, ep - 1, handle.nr_versions());
      min_of_epoch = sid;
      last_gc_epoch = ep;
    }

    if (min_of_epoch > sid) min_of_epoch = sid;
  }
};

class BaseVHandle {
 public:
  static mem::Pool<true> *pools;
  static void InitPools();
 protected:
  ErmiaEpochGCRule gc_rule;
 public:
  uint64_t last_update_epoch() const { return gc_rule.last_gc_epoch; }
};

class SortedArrayVHandle : public BaseVHandle {
  short alloc_by_coreid;
  short this_coreid;
  std::atomic_bool lock;
  size_t capacity;
  size_t size;
  size_t value_mark;
  uint64_t *versions;
  // uintptr_t *objects;

  // struct TxnWaitSlot {
  //   std::mutex lock;
  //   go::WaitSlot slot;
  // };
  // TxnWaitSlot *slots;

 public:
  static void *operator new(size_t nr_bytes) {
    return pools[mem::CurrentAllocAffinity()].Alloc();
  }

  static void operator delete(void *ptr) {
    SortedArrayVHandle *phandle = (SortedArrayVHandle *) ptr;
    pools[phandle->this_coreid].Free(ptr);
  }

  SortedArrayVHandle();
  SortedArrayVHandle(SortedArrayVHandle &&rhs) = delete;

  bool AppendNewVersion(uint64_t sid);
  VarStr *ReadWithVersion(uint64_t sid);
  bool WriteWithVersion(uint64_t sid, VarStr *obj, bool dry_run = false);
  void GarbageCollect();

  const size_t nr_versions() const { return size; }
 private:
  void EnsureSpace();
  volatile uintptr_t *WithVersion(uint64_t sid, int &pos);
};

static_assert(sizeof(SortedArrayVHandle) <= 64, "SortedArrayVHandle is larger than a cache line");

class LinkListVHandle : public BaseVHandle {
  int this_coreid;
  std::atomic_bool lock;

  struct Entry {
    struct Entry *next;
    uint64_t version;
    uintptr_t object;
    int alloc_by_coreid;

    static mem::Pool<true> *pools;

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

  bool AppendNewVersion(uint64_t sid);
  VarStr *ReadWithVersion(uint64_t sid);
  bool WriteWithVersion(uint64_t sid, VarStr *obj, bool dry_run = false);
  void GarbageCollect();

  const size_t nr_versions() const { return size; }
};

static_assert(sizeof(LinkListVHandle) <= 64, "LinkList Handle too large!");

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

  bool AppendNewVersion(uint64_t sid);
  bool AppendNewAccess(uint64_t sid, bool is_read = false);
  VarStr *ReadWithVersion(uint64_t sid);
  VarStr *DirectRead(); // for read-only optimization
  bool WriteWithVersion(uint64_t sid, VarStr *obj, bool dry_run = false);
  void GarbageCollect();

  const size_t nr_versions() const { return size; }
 private:
  void EnsureSpace();
  uint64_t WaitForTurn(uint64_t sid);
  bool PeekForTurn(uint64_t sid);
};

static_assert(sizeof(CalvinVHandle) <= 64, "Calvin Handle too large!");

// current relation implementation
#if (defined LL_REPLAY) || (defined CALVIN_REPLAY)

#ifdef LL_REPLAY
using VHandle = LinkListVHandle;
#endif

#ifdef CALVIN_REPLAY
using VHandle = CalvinVHandle;
#endif

#else
using VHandle = SortedArrayVHandle;
#endif

}

#endif /* VHANDLE_H */
