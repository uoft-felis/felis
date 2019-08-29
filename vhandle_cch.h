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
    static constexpr size_t kBlockSize = 64;
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
    }

    static void *operator new(size_t) {
      return T::AllocBlock();
    }

    static void operator delete(void *ptr) {
      T::FreeBlock((Block *) ptr);
    }
  };
  static_assert(sizeof(Block) == Block::kBlockSize, "Block doesn't match block size?");

  std::array<Block *, NodeConfiguration::kMaxNrThreads> heads_buffer;
  util::MCSSpinLock head_lock;
  Block *head;
 public:
  static void InitPool();

  VHandleCollectionHandler() {
    head = nullptr;
    heads_buffer.fill(nullptr);
  }

  void AddVHandle(VHandle *vhandle) {
    auto core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    auto &block_head = heads_buffer[core_id];
    if (block_head == nullptr)
      block_head = new Block();

    if (block_head->nr_handles == Block::kMaxNrBlocks) {
      util::MCSSpinLock::QNode qnode;
      head_lock.Acquire(&qnode);
      block_head->next = head;
      head = block_head;
      head_lock.Release(&qnode);

      block_head = new Block();
    }
    block_head->handles[block_head->nr_handles++] = vhandle;
  }

  void RunHandler() {
    auto core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    auto nrb = 0, nr = 0;

    auto block_head = heads_buffer[core_id];
    if (!block_head) {
      block_head = new Block();
    }

    do {
      block_head->Prefetch();
      for (int i = 0; i < block_head->nr_handles; i++) {
        ProcessVHandle(block_head->handles[i]);
      }
      nrb++;
      nr += block_head->nr_handles;

      if (block_head != heads_buffer[core_id])
        delete block_head;
      else
        block_head->nr_handles = 0;

      util::MCSSpinLock::QNode qnode;
      head_lock.Acquire(&qnode);
      block_head = head;
      if (head) head = head->next;
      head_lock.Release(&qnode);
    } while (block_head);

    logger->info("Processed {} rows {} blks", nr, nrb);
  }

  void ProcessVHandle(VHandle *vhandle) {
    static_cast<T *>(this)->Process(vhandle);
  }
};

}

#endif
