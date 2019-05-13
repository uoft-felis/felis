#ifndef GC_H
#define GC_H

#include "util.h"
#include "vhandle_cch.h"
#include "mem.h"

namespace felis {

class GC : public VHandleCollectionHandler<GC> {
  static mem::ParallelPool g_block_pool;
  uint64_t cur_epoch_nr;
 public:
  // Hame hiding.
  void AddVHandle(VHandle *vhandle);

  void Process(VHandle *handle);
  void RunGC();

  static void *AllocBlock() { return g_block_pool.Alloc(); }
  static void FreeBlock(Block *b) { return g_block_pool.Free(b, b->alloc_core); }
  static void InitPool();
 private:
  void Collect(VHandle *handle);
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
