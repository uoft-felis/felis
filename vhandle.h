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
  //Corey: Vhandle_Metadata(64B) | Inline_Version_Array(32B) | Mask1(1B)|Mask2(1B)|MiniHeap(158B)
  //Corey: Inline_Version_Array(32B) = Sid1(8B)|Ptr1(8B)|Sid2(8B)|Ptr2(8B)
  //Corey: Set Inline offsets - Doesn't take up memory
  static constexpr size_t vhandleMetadataSize = 64;
  static constexpr size_t ineTwoVersionArraySid1Size = 8;
  static constexpr size_t inlineTwoVersionArrayPtr1Size = 8;
  static constexpr size_t inlineTwoVersionArraySid2Size = 8;
  static constexpr size_t inlineTwoVersionArrayPtr2Size = 8;
  static constexpr size_t inlineTwoVersionArraySize = ineTwoVersionArraySid1Size + inlineTwoVersionArrayPtr1Size + 
              inlineTwoVersionArraySid2Size + inlineTwoVersionArrayPtr2Size; // 32
  static constexpr size_t inlineMiniHeapMask1Size = 1;
  static constexpr size_t inlineMiniHeapMask2Size = 1;
  static constexpr size_t inlineMiniHeapSize = 158;

  //Corey TODO: Comment out Non-Inlined vhandle [vhandle info & pointer to version array | version array]
  static constexpr size_t kSize = 128;
  //Corey: Inlineded version values in new vhandle layout
  //Corey: Total size to be used for VHandle + Inline for PMem design
  static constexpr size_t kInlinedSize = 256;//vhandleMetadataSize + inlineTwoVersionArraySize +
                //inlineMiniHeapMask1Size + inlineMiniHeapMask2Size +
                //inlineMiniHeapSize; // Should be 256
  //shirley TODO: (un-inlined) pool can be removed bc all vhandles are inlined
  static mem::ParallelSlabPool pool;

  // Cicada uses inline data to reduce cache misses. These inline rows are much
  // larger: 4-cache linekInlinedSizes.
  static mem::ParallelSlabPool inline_pool;
  static void InitPool();

  // Corey: Pool is not needed
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
  friend class ContentionManager;
  friend class HashtableIndex;

  util::MCSSpinLock lock; //shirley: used by versions? vhandle?
  uint8_t alloc_by_regionid; // shirley: used by vhandle, can remove?
  uint8_t this_coreid; // shirley: can remove? used for alloc/free versions
  int8_t cont_affinity; //shirley: versions? vhandle? used by contention manager
  uint8_t inline_used; //shirley: doesn't need in new design

  unsigned int capacity; //shirley: used for versions
  unsigned int size;     // shirley: used for versions
  unsigned int cur_start; //shirley: used by GC. doesn't need in new design?

  //Corey: the latest written version's offset in *versions
  std::atomic_int latest_version; // shirley: used for versions
  int nr_ondsplt; //shirley: used for versions, by contention manager
  //[0, capacity - 1] stores version number, [capacity, 2*capacity - 1] stores ptr to data
  uint64_t *versions;
  util::OwnPtr<RowEntity> row_entity; // shirley: used for data migration?
  std::atomic_long buf_pos = -1; //shirley: used by contention manager
  std::atomic<uint64_t> gc_handle = 0; // shirley: used by minor gc? doesn't need in new design?

  SortedArrayVHandle();
 public:

  static void operator delete(void *ptr) {
    SortedArrayVHandle *phandle = (SortedArrayVHandle *) ptr;
    //shirley TODO: we should only use inlined pool bc all vhandles are inlined
    if (phandle->is_inlined())
      inline_pool.Free(ptr, phandle->this_coreid);
    else
      pool.Free(ptr, phandle->this_coreid);
  }

  //Corey: New() not necessary in new design
  static SortedArrayVHandle *New();
  static SortedArrayVHandle *NewInline();

  bool ShouldScanSkip(uint64_t sid);
  void AppendNewVersion(uint64_t sid, uint64_t epoch_nr, int ondemand_split_weight = 0);
  VarStr *ReadWithVersion(uint64_t sid);
  VarStr *ReadExactVersion(unsigned int version_idx);
  bool WriteWithVersion(uint64_t sid, VarStr *obj, uint64_t epoch_nr);
  bool WriteExactVersion(unsigned int version_idx, VarStr *obj, uint64_t epoch_nr);

  enum SidType {
    SidType1,
    SidType2
  };

  //Corey: Get Sid value
  uint64_t GetInlineSid(SidType sidType) const {
    uint8_t *sidPtr = (uint8_t *)this + vhandleMetadataSize;
    if (sidType == SidType2) {
      sidPtr += ineTwoVersionArraySid1Size + inlineTwoVersionArrayPtr1Size;
    }
    return *((uint64_t *)sidPtr);
  }
  
  //Corey: Write Sid value
  void SetInlineSid(SidType sidType, uint64_t sidValue) {
    uint8_t *sidPtr = (uint8_t *)this + vhandleMetadataSize;
    if (sidType == SidType2) {
      sidPtr += ineTwoVersionArraySid1Size + inlineTwoVersionArrayPtr1Size;
    }
    *((uint64_t *)sidPtr) = sidValue;
  }

  //Corey: Get Inline Ptr value
  //Corey: This one returns a pointer to ptr1 or ptr2
  uint8_t **GetInlinePtrPtr(SidType sidType) const {
    uint8_t **sidPtr = (uint8_t**)((uint8_t *)this + vhandleMetadataSize + ineTwoVersionArraySid1Size);
    if (sidType == SidType2) {
      sidPtr += inlineTwoVersionArrayPtr1Size + inlineTwoVersionArraySid2Size;
    }
    return sidPtr;
  }

  //Corey: This one returns ptr1 or ptr2
  uint8_t *GetInlinePtr(SidType sidType) const {
    uint8_t **sidPtr = (uint8_t**)((uint8_t *)this + vhandleMetadataSize + ineTwoVersionArraySid1Size);
    if (sidType == SidType2) {
      sidPtr += inlineTwoVersionArrayPtr1Size + inlineTwoVersionArraySid2Size;
    }
    return *sidPtr;
  }

  //Corey: Set Inline Ptr value to the address passed in to function
  void SetInlinePtr(SidType sidType, uint8_t *miniHeapPtrAddr) {
    uint8_t **sidPtr = (uint8_t **)((uint8_t *)this + vhandleMetadataSize + ineTwoVersionArraySid1Size);
    if (sidType == SidType2) {
      sidPtr += inlineTwoVersionArrayPtr1Size + inlineTwoVersionArraySid2Size;
    }
    *sidPtr = miniHeapPtrAddr;
  }

  //Corey: Free ptr1 (only if it’s external pmem call Persistent Pool. free()  )
  void FreePtr1() {
    uint8_t *sid1Ptr = GetInlinePtr(SidType1);
    uint8_t *startOfMiniHeap = (uint8_t *)this + vhandleMetadataSize + inlineTwoVersionArraySize + inlineMiniHeapMask1Size + inlineMiniHeapMask2Size;

    // Check if Ptr1 is using miniheap, if so do nothing
    if (startOfMiniHeap <= sid1Ptr && sid1Ptr < (startOfMiniHeap + inlineMiniHeapSize)) {
      return;
    } 

    //TODO: Not sure on this? Where do we extend the memory allocation for inline version array extension?
    //Need size of allocated amount or have it passed into this function
    VarStr *ins = (VarStr *) sid1Ptr;
    delete ins;
    // mem::GetPersistentPool().Free(sid1Ptr, ins-> region_id, sizeof(VarStr) + ins->len);      
  }

  //Corey: Copy (sid2, ptr2) to (sid1, ptr1)
  void CopySid2ToSid1() {
    uint64_t sid2Val = GetInlineSid(SidType2);
    uint8_t *sid2Ptr = GetInlinePtr(SidType2);

    SetInlinePtr(SidType1, sid2Ptr);
    SetInlineSid(SidType1, sid2Val);
  }

  //Corey: Set (sid2, ptr2) to (0, null) //is it correct to use 0?
  void ResetSid2() {
    SetInlineSid(SidType2, 0);
    SetInlinePtr(SidType2, nullptr);
  }

  void Prefetch() const { __builtin_prefetch(versions); }

  std::string ToString() const;

  //shirley TODO: we don't use inline_used variable for our design
  bool is_inlined() const { return inline_used != 0xFF; }

  //Corey: Old Design Alloc
  uint8_t *AllocFromInline(size_t sz) {
    // shirley: use pmem version for new vhandle layout.
    // return AllocFromInlinePmem(sz);

    // uint8_t *inline_used_duplicate = (uint8_t *) this + (64 + 32);
    // if (inline_used != *inline_used_duplicate) {
    //   printf("AllocFromInline, inline_used != *inline_used_duplicate\n");
    //   printf("inline_used = %x, *inline_used_duplicate = %x\n", inline_used, *inline_used_duplicate);
    //   printf("&inline_used = %p, inline_used_duplicate = %p\n", &inline_used, inline_used_duplicate);
    //   std::abort();
    // }
    // if (&inline_used < (uint8_t *) this || &inline_used > (uint8_t *)this + 64) {
    //   printf("AllocFromInline, weird addresses, &inline_used = %p, this = %p\n", &inline_used, this);
    //   std::abort();
    // }

    if (inline_used != 0xFF) {
      sz = util::Align(sz, 32); // Here aligns to 32 but in free uses 16
      if (sz > 128) {
        //shirley: set inline_used to 0 bc we didn't allocate for this version & we're only tracking latest alloc
        inline_used = 0;
        // *inline_used_duplicate = (uint8_t)0;

        // felis::probes::VersionAllocCountInlineToExternal{0, 1}();
        return nullptr;
      }

      uint8_t mask = (1 << (sz >> 5)) - 1; 
      for (uint8_t off = 0; off <= 4 - (sz >> 5); off++) {
        // if ((((uint8_t)*inline_used_duplicate) & (mask << off)) == 0) {
        if ((inline_used & (mask << off)) == 0) {
          //inline_used |= (mask << off);
          //shirley: what if we only record latest write? It still works.
          inline_used = (mask << off);
          // *inline_used_duplicate = (uint8_t)(mask << off);
          //printf("alloced from inline\n");
          // felis::probes::VersionAllocCountInlineToExternal{1, 0}();
          return (uint8_t *) this + 128 + (off << 5);
        }
      }
    }
    //shirley: set inline_used to 0 bc we didn't allocate for this version & we're only tracking latest alloc
    inline_used = 0;
    // *inline_used_duplicate = (uint8_t)0;
    // felis::probes::VersionAllocCountInlineToExternal{0, 1}();
    return nullptr;
  }

  //Corey: Old Design
  void FreeToInline(uint8_t *p, size_t sz) {
    if (inline_used != 0xFF) { 
      sz = util::Align(sz, 16);
      if (sz > 128) return;
      uint8_t mask = (1 << (sz >> 4)) - 1;
      uint8_t off = (p - (uint8_t *) this - 128) >> 4;
      inline_used &= ~(mask << off);
    }
  }
  
  /* Corey:
    1) Try offest [two 8-bit in mask space] | uses all bits for byte granularity
    2) Try 32 mask [one 8-bit in vhandle] | use 5-bits / wastes 3-bits for 32byte granularity
    3) Try 16 mask [two 8-bit in mask space] uses 10-bits / wastes 6-bits for 16byte granularity

    To allocate ptr1/ptr2 version data to be placed in PMem's vhandle inline version data area
    Total size to be used for VHandle + Inline for PMem design
  */
  uint8_t *AllocFromInlinePmem(size_t sz) {
    // printf("Size: %ld\n", sz);
    // return nullptr;
    sz = util::Align(sz, 16); // shirley note: need to have align or else seg fault.

    //printf("AllocFromInlinePmem: this: %p, &(this->size): %p, this->size: %u\n", this, &(this->size), this->size);
    
    // Check requests size fits in miniheap
    if (sz > inlineMiniHeapSize) {
      // felis::probes::VersionAllocCountInlineToExternal{0, 1}();
      return nullptr;
    }

    // Mask Offset stores byte offset = [0 to 158]
    uint8_t *mask1Ptr = (uint8_t *)this + (vhandleMetadataSize + inlineTwoVersionArraySize); // Store Byte Offset
    uint8_t *mask2Ptr = mask1Ptr + inlineMiniHeapMask1Size; // Store Byte Offset
    uint8_t *startOfMiniHeap = mask2Ptr + inlineMiniHeapMask2Size;
    
    //printf("Mask1: %p | Mask2: %p | MiniHeap: %p\n", mask1Ptr, mask2Ptr, startOfMiniHeap);
    //printf("Mask1Val: %d | Mask2Val: %d\n", *mask1Ptr, *mask2Ptr);

    // Check to make sure miniheap not full - Very big last version stored
    int trackedSize = (*mask1Ptr == *mask2Ptr && *mask1Ptr == 0) ? 0 : *mask2Ptr - *mask1Ptr + 1;
    //printf("TrackedSize: %d\n", trackedSize);

    if (trackedSize >= inlineMiniHeapSize) {
      // felis::probes::VersionAllocCountInlineToExternal{0, 1}();
      return nullptr;
    }
    else if (trackedSize <= 0) {
        *mask1Ptr = 0;
        *mask2Ptr = sz - 1;
        // felis::probes::VersionAllocCountInlineToExternal{1, 0}();
        // Return address start of new allocation
        //printf("Init | Mask1Val: %d | Mask2Val: %d\n\n", *mask1Ptr, *mask2Ptr);
        // assert(((startOfMiniHeap + *mask1Ptr) >= ((uint8_t *)this+64+32)) &&
        //        ((startOfMiniHeap + *mask1Ptr) < ((uint8_t *)this+256)));
        // printf("alloced from inline pmem this: %p, alloced: %p\n", this, (startOfMiniHeap + *mask1Ptr));
        return (startOfMiniHeap + *mask1Ptr);
    }

    // find closest space for allocation
    int diffSpaceAtFront = (*mask1Ptr >= sz) ? *mask1Ptr - sz : -1; // Extra space between end of new allocation and start of last
    int diffSpaceAtEnd = inlineMiniHeapSize - *mask2Ptr; // Space after end of last allocation
    diffSpaceAtEnd = (diffSpaceAtEnd >= sz) ? diffSpaceAtEnd - sz : -1; // Extra space after end of new allocation and end of miniheap
    //printf("diffSpaceAtFront: %d | diffSpaceAtEnd: %d\n", diffSpaceAtFront, diffSpaceAtEnd);
    
    // Check if there was an error in above
    if (0 > diffSpaceAtEnd && 0 > diffSpaceAtFront) {
        // Error occured
        // felis::probes::VersionAllocCountInlineToExternal{0, 1}();
        return nullptr;
    }

    // Check if space at front is closest in size - less wasted space
    bool addFront = false;
    if (0 > diffSpaceAtEnd) {
        addFront = true;
    }
    else if (0 > diffSpaceAtFront) {
        addFront = false;
    }
    else if (diffSpaceAtFront <= diffSpaceAtEnd) {
        addFront = true;
    }
    else {
        addFront = false;
    }

    // Track New Allocation
    if (addFront) {
        *mask1Ptr = 0;
        *mask2Ptr = sz - 1;
    }
    else {
        *mask1Ptr = *mask2Ptr + 1; // New start is taken from after last end
        *mask2Ptr = *mask1Ptr + sz - 1;
    }

    // felis::probes::VersionAllocCountInlineToExternal{1, 0}();
    // Return address start of new allocation
    //printf("Front: %d | Mask1Val: %d | Mask2Val: %d\n\n", addFront, *mask1Ptr, *mask2Ptr);
    // assert(((startOfMiniHeap + *mask1Ptr) >= ((uint8_t *)this+64+32)) &&
    //            ((startOfMiniHeap + *mask1Ptr) < ((uint8_t *)this+256)));
    // printf("alloced from inline pmem this: %p, alloced: %p\n", this, (startOfMiniHeap + *mask1Ptr));
    return (startOfMiniHeap + *mask1Ptr);
  }

  // These function are racy. Be careful when you are using them. They are perfectly fine for statistics.
  const size_t nr_capacity() const { return capacity; }
  const size_t nr_versions() const { return size; }
  const size_t current_start() const { return cur_start;}
  uint64_t first_version() const { 
    if (!versions)
      return GetInlineSid(SidType1); // shirley: return sid1 when version array is null
    else 
      return versions[0];
  }
  uint64_t last_version() const { 
    if (versions)
      return versions[size - 1]; 
    else {
      printf("last_version(), versions is null???\n");
      std::abort();
    } // shirley: abort bc versions shouldn't be nullptr?
  }
  unsigned int nr_updated() const { return latest_version.load(std::memory_order_relaxed) + 1; }
  int nr_ondemand_split() const { return nr_ondsplt; }
  uint8_t region_id() const { return alloc_by_regionid; } //shirley: not used?
  uint8_t object_coreid() const { return this_coreid; }
  int8_t contention_affinity() const { return cont_affinity; }
 private:
  void AppendNewVersionNoLock(uint64_t sid, uint64_t epoch_nr, int ondemand_split_weight);
  unsigned int AbsorbNewVersionNoLock(unsigned int end, unsigned int extra_shift);
  void BookNewVersionNoLock(uint64_t sid, unsigned int pos) {
    versions[pos] = sid;
  }
  void IncreaseSize(int delta, uint64_t epoch_nr);
  volatile uintptr_t *WithVersion(uint64_t sid, int &pos);
};


//shirley: can remove
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
