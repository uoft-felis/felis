// -*- mode: c++ -*-

#ifndef INDEX_INFO_H
#define INDEX_INFO_H

#include "entity.h"
#include "felis_probes.h"
#include "gopp/gopp.h"
#include "mem.h"
#include "shipping.h"
#include "varstr.h"
#include <atomic>
#include "vhandle.h"
#include "util/objects.h"
#include "epoch.h"

namespace felis {

// shirley todo: move sync to index_info
class VHandleSyncService {
public:
  static bool g_lock_elision;
  virtual void ClearWaitCountStats() = 0;
  virtual long GetWaitCountStat(int core) = 0;
  // virtual void Notify(uint64_t bitmap) = 0;
  // virtual bool IsPendingVal(uintptr_t val) = 0;
  virtual void WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver,
                           void *handle) = 0;
  virtual void OfferData(volatile uintptr_t *addr, uintptr_t obj) = 0;
};

class BaseIndexInfo {
public:
  static constexpr size_t VerArrayInfoSize = 32;
  static constexpr size_t kIndexInfoSize = 32; // shirley todo: define/modify this later
  static mem::ParallelSlabPool pool;
  static void InitPool();
  static void Quiescence() { pool.Quiescence(); }

public:
  // shirley todo: move sync to index_info
  VHandleSyncService &sync();
};

class RowEntity;
class SliceManager;
class VersionBuffer;

class IndexInfo : public BaseIndexInfo {
  friend class VersionBuffer;
  friend class RowEntity;
  friend class SliceManager;
  friend class GC;
  friend class VersionBufferHead;
  friend class VersionBufferHandle;
  friend class ContentionManager;
  friend class HashtableIndex;

  util::MCSSpinLock lock;
  VHandle *vhandle;
  //[0, capacity - 1] stores version number, [capacity, 2*capacity - 1] stores ptr to data
  uint64_t versions_ep = 0;
  uint64_t *versions;

  // shirley: versions? vhandle? used by contention manager.
  // shirley todo: decide what to do with this.
  // int8_t cont_affinity;

  // shirley: used for versions, by contention manager
  // shirley todo: decide what to do with this
  // int nr_ondsplt;

  // shirley: used by contention manager
  // shirley todo: decide what to do with this
  // std::atomic_long buf_pos = -1;

  IndexInfo();

public:
  static void operator delete(void *ptr) {
    // shirley: we currently don't ever delete vhandle, so also don't delete index info
    // shirley: if we need to delete index_info later, need to store the core_id for slabpool (or use a different pool)
  }

  // Corey: New() not necessary in new design
  static IndexInfo *New();

  bool ShouldScanSkip(uint64_t sid);
  void AppendNewVersion(uint64_t sid, uint64_t epoch_nr,
                        int ondemand_split_weight = 0);
  VarStr *ReadWithVersion(uint64_t sid);
  VarStr *ReadExactVersion(unsigned int version_idx);
  bool WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr);
  bool WriteExactVersion(unsigned int version_idx, VarStr *obj,
                         uint64_t epoch_nr);

  // shirley: return pointer to vhandle
  VHandle *vhandle_ptr() {
    return vhandle;
  }

  // shirley: define the layout of the info stored in version array. should be
  // smaller than 32 bytes.
  typedef struct VerArrayInfo {
    unsigned int capacity;
    unsigned int size;
    std::atomic_int latest_version; // shirley: the latest written version's
                                    // offset in *versions
  } VerArrayInfo;

  // shirley: return pointer to versions within transient version array
  static uint64_t *versions_ptr(uint64_t *versions) {
    assert(versions);
    return (uint64_t *)(((uint8_t *)versions) + VerArrayInfoSize);
  }

  // shirley: return pointers to info given version array
  static unsigned int *capacity_ptr(uint64_t *versions) {
    assert(versions);
    return &(((VerArrayInfo *)versions)->capacity);
  }

  static unsigned int *size_ptr(uint64_t *versions) {
    assert(versions);
    return &(((VerArrayInfo *)versions)->size);
  }

  static std::atomic_int *latest_version_ptr(uint64_t *versions) {
    assert(versions);
    return &(((VerArrayInfo *)versions)->latest_version);
  }

  // shirley: set info given version array
  static void capacity_set(uint64_t *versions, unsigned int capacity) {
    assert(versions);
    ((VerArrayInfo *)versions)->capacity = capacity;
  }

  static void size_set(uint64_t *versions, unsigned int size) {
    assert(versions);
    ((VerArrayInfo *)versions)->size = size;
  }

  static void latest_version_set(uint64_t *versions, int latest_version) {
    assert(versions);
    ((VerArrayInfo *)versions)->latest_version.store(latest_version);
  }

  // shirley: get info given version array
  static unsigned int capacity_get(uint64_t *versions) {
    assert(versions);
    return ((VerArrayInfo *)versions)->capacity;
  }

  static unsigned int size_get(uint64_t *versions) {
    assert(versions);
    return ((VerArrayInfo *)versions)->size;
  }

  static int latest_version_get(uint64_t *versions) {
    assert(versions);
    return ((VerArrayInfo *)versions)->latest_version.load();
  }

  static_assert(sizeof(VerArrayInfo) <= VerArrayInfoSize,
                "VerArrayInfo is larger than VerArrayInfoSize bytes!\n");

  void Prefetch() const { __builtin_prefetch(versions); }
  void Prefetch_vhandle() const { 
    __builtin_prefetch(vhandle);
    __builtin_prefetch((char*) (vhandle + 64));
    __builtin_prefetch((char*) (vhandle + 128));
    __builtin_prefetch((char*) (vhandle + 192));
  }

  std::string ToString() const;

  // These function are racy. Be careful when you are using them. They are
  // perfectly fine for statistics.
  // const size_t nr_capacity() const { return capacity; }
  // shirley: this is only used for granola / pwv
  const size_t nr_versions() const {
    auto current_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
    if (versions_ep != current_epoch_nr)
      return 0;
    return size_get(versions);
  }
  const size_t current_start() const { return 0; }
  uint64_t first_version() const {
    auto current_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
    if (versions_ep != current_epoch_nr) {
      auto ptr2 = vhandle->GetInlinePtr(felis::SortedArrayVHandle::SidType2);
      if (ptr2){
        return vhandle->GetInlineSid(SortedArrayVHandle::SidType2);
      }
      else {
        return vhandle->GetInlineSid(SortedArrayVHandle::SidType1); 
      }
    }
    else
      return versions_ptr(versions)[0];
    // return versions[0];
  }
  uint64_t last_version() const {
    auto current_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
    if (versions_ep == current_epoch_nr)
      return versions_ptr(versions)[size_get(versions) - 1];
    // return versions[size - 1];
    else {
      // shirley: this function is called during write (execute) so versions should be current
      // shirley note: but it's also used in shipping.cc but out of date and not supporting.
      printf("last_version(), versions is not current???\n");
      std::abort();
    } // shirley: abort bc versions shouldn't be nullptr?
  }
  unsigned int nr_updated() const {
    // shirley todo: used by contention manager. TODO
    auto current_epoch_nr = util::Instance<EpochManager>().current_epoch_nr();
    assert(versions_ep == current_epoch_nr);
    return latest_version_ptr(versions)->load(std::memory_order_relaxed) + 1;
  }
  // int nr_ondemand_split() const { return nr_ondsplt; }
  // // uint8_t region_id() const { return alloc_by_regionid; } //shirley: not
  // // used?
  // uint8_t object_coreid() const { return this_coreid; }
  // int8_t contention_affinity() const { return cont_affinity; }

private:
  void AppendNewVersionNoLock(uint64_t sid, uint64_t epoch_nr,
                              int ondemand_split_weight);
  unsigned int AbsorbNewVersionNoLock(unsigned int end,
                                      unsigned int extra_shift);
  void BookNewVersionNoLock(uint64_t sid, unsigned int pos) {
    versions_ptr(versions)[pos] = sid;
    // versions[pos] = sid;
  }
  void IncreaseSize(int delta, uint64_t epoch_nr);
  volatile uintptr_t *WithVersion(uint64_t sid, int &pos);
};

// shirley: probably don't need size checking for index info
static_assert(sizeof(IndexInfo) <= BaseIndexInfo::kIndexInfoSize, "IndexInfo is too large!");


} // namespace felis

#endif /* INDEX_INFO_H */
