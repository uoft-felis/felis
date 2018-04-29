// -*- mode: c++ -*-

#ifndef VHANDLE_H
#define VHANDLE_H

#include "dolly_probes.h"
#include "mem.h"
#include "sqltypes.h"
#include "gc.h"

namespace dolly {

static const uintptr_t kPendingValue = 0xFE1FE190FFFFFFFF; // hope this pointer is weird enough

class BaseVHandle {
 public:
  static mem::Pool *pools;
  static void InitPools();
 protected:
  EpochGCRule gc_rule;
 public:
  uint64_t last_update_epoch() const { return gc_rule.last_gc_epoch; }
  void Prefetch() const {}
};

class SortedArrayVHandle : public BaseVHandle {
  std::atomic_bool lock;
  short alloc_by_coreid;
  short this_coreid;
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

  bool AppendNewVersion(uint64_t sid, uint64_t epoch_nr);
  VarStr *ReadWithVersion(uint64_t sid) __attribute__((noinline));
  bool WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr, bool dry_run = false) __attribute__((noinline));
  void GarbageCollect();
  void Prefetch() const { __builtin_prefetch(versions); }

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
#endif

}


#endif /* VHANDLE_H */
