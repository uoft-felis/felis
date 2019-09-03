#ifndef GC_H
#define GC_H

#include "util.h"
#include "mem.h"
#include "node_config.h"

namespace felis {

class VHandle;

class GC {
  static mem::ParallelPool g_block_pool;
 public:
  void AddVHandle(VHandle *vhandle, uint64_t epoch_nr);
  void PrepareGC();
  void RunGC();
  void FinalizeGC();

  struct GarbageBlock {
    static constexpr size_t kBlockSize = 128;
    static constexpr int kMaxNrBlocks = kBlockSize / 8 - 3;
    std::array<VHandle *, kMaxNrBlocks> handles;
    int alloc_core;
    int nr_handles;
    GarbageBlock *next;
    GarbageBlock *processing_next;

    GarbageBlock() : alloc_core(mem::ParallelPool::CurrentAffinity()),
                     nr_handles(0) {}

    void Prefetch() {
      for (int i = 0; i < nr_handles; i++) {
        __builtin_prefetch(handles[i]);
      }
    }

    static void *operator new(size_t) {
      return GC::AllocBlock();
    }

    static void operator delete(void *ptr) {
      GC::FreeBlock((GarbageBlock *) ptr);
    }
  };
  static_assert(sizeof(GarbageBlock) == GarbageBlock::kBlockSize, "Block doesn't match block size?");

  static void *AllocBlock() { return g_block_pool.Alloc(); }
  static void FreeBlock(GarbageBlock *b) { return g_block_pool.Free(b, b->alloc_core); }
  static void InitPool();
 private:
  void Collect(VHandle *handle, uint64_t cur_epoch_nr);
  void Process(VHandle *handle, uint64_t cur_epoch_nr);

  struct LocalCollector {
    GarbageBlock *pending = nullptr;
    GarbageBlock *processing = nullptr;
  };
  std::array<LocalCollector, NodeConfiguration::kMaxNrThreads> local_cls;

  LocalCollector &local_collector();
  std::atomic<GarbageBlock *> processing_queue = nullptr;
};

}

namespace util {

using namespace felis;

template <> struct InstanceInit<GC> {
  static constexpr bool kHasInstance = true;
  static GC *instance;
};

}

#endif /* GC_H */
