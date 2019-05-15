#ifndef VHANDLE_CCH_H
#define VHANDLE_CCH_H

#include <cstdlib>
#include "gopp/gopp.h"
#include "mem.h"
#include "node_config.h"

// Post processing for vhandles

namespace felis {

class VHandle;

template <typename T>
class VHandleCollectionHandler {
 protected:
  struct Block {
    static constexpr size_t kBlockSize = 4096;
    static constexpr int kMaxNrBlocks = kBlockSize / 8 - 2;
    std::array<VHandle *, kMaxNrBlocks> handles;
    int alloc_core;
    int nr_handles;
    Block *next;

    Block() : alloc_core(mem::ParallelPool::CurrentAffinity()),
              nr_handles(0) {}

    void Prefetch() {
      for (int i = 0; i < nr_handles; i++) {
        __builtin_prefetch(handles[i]);
      }
      __builtin_prefetch(next);
    }

    static void *operator new(size_t) {
      return T::AllocBlock();
    }

    static void operator delete(void *ptr) {
      T::FreeBlock((Block *) ptr);
    }
  };
  static_assert(sizeof(Block) == Block::kBlockSize, "Block doesn't match block size?");

  std::array<Block *, NodeConfiguration::kMaxNrThreads> blocks_heads;
 public:
  static void InitPool();

  VHandleCollectionHandler() {
    blocks_heads.fill(nullptr);
  }

  void AddVHandle(VHandle *vhandle) {
    auto core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    auto &blocks_head = blocks_heads[core_id];
    if (blocks_head == nullptr || blocks_head->nr_handles == Block::kMaxNrBlocks) {
      auto b = new Block();
      b->next = blocks_head;
      blocks_head = b;
    }
    blocks_head->handles[blocks_head->nr_handles++] = vhandle;
  }

  void RunHandler() {
    auto core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    auto &blocks_head = blocks_heads[core_id];
    auto nrb = 0, nr = 0;
    while (blocks_head) {
      blocks_head->Prefetch();
      for (int i = 0; i < blocks_head->nr_handles; i++) {
        ProcessVHandle(blocks_head->handles[i]);
      }
      nrb++;
      nr += blocks_head->nr_handles;
      auto n = blocks_head->next;
      delete blocks_head;
      blocks_head = n;
    }
  }

  void ProcessVHandle(VHandle *vhandle) {
    static_cast<T *>(this)->Process(vhandle);
  }
};

}

#endif
