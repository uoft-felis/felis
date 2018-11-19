#ifndef EPOCH_H
#define EPOCH_H

#include <cstdint>
#include <array>
#include "node_config.h"
#include "util.h"
#include "mem.h"

namespace felis {

class Epoch;
class BaseTxn;

class EpochClient {
 public:
  static EpochClient *gWorkloadClient;

  EpochClient() noexcept;
  virtual ~EpochClient() {}

  void Start();

  virtual uint LoadPercentage() = 0;
 protected:
  void Worker();
  virtual BaseTxn *RunCreateTxn() = 0;
};

class EpochManager {
  mem::Pool *pool;

  static EpochManager *instance;
  template <class T> friend T& util::Instance();

  static constexpr int kMaxConcurrentEpochs = 2;
  std::array<Epoch *, kMaxConcurrentEpochs> concurrent_epochs;
  uint64_t cur_epoch_nr;

  EpochManager();
 public:
  Epoch *epoch(uint64_t epoch_nr) const;
  uint8_t *ptr(uint64_t epoch_nr, int node_id, uint64_t offset) const;

  uint64_t current_epoch_nr() const { return cur_epoch_nr; }
  Epoch *current_epoch() const { return epoch(cur_epoch_nr); }

  void DoAdvance();
};

template <typename T>
class EpochObject {
  uint64_t epoch_nr;
  int node_id;
  uint64_t offset;

  friend class Epoch;
  EpochObject(uint64_t epoch_nr, int node_id, uint64_t offset) : epoch_nr(epoch_nr), node_id(node_id), offset(offset) {}
 public:
  EpochObject() : epoch_nr(0), node_id(0), offset(0) {}
  typedef T Type;

  operator T*() {
    return this->operator->();
  }

  T *operator->() {
    return (T *) util::Instance<EpochManager>().ptr(epoch_nr, node_id, offset);
  }

  template <typename P>
  EpochObject<P> Convert(P *ptr) {
    uint8_t *p = (uint8_t *) ptr;
    uint8_t *self = util::Instance<EpochManager>().ptr(epoch_nr, node_id, offset);
    int64_t off = p - self;
    return EpochObject<P>(epoch_nr, node_id, offset + off);
  }
};

// This where we store objects across the entire cluster. Note that these
// objects are replicated but not synchronized. We need to make these objects
// perfectly partitioned.
//
// We mainly use this to store the transaction execution states, but we could
// store other types of POJOs as well.
//
// The allocator is simply an brk. Objects were replicated by replicating the
// node_id and the offset.
class EpochMemory {
 protected:
  struct {
    uint8_t *mem;
    uint64_t off;
  } brks[NodeConfiguration::kMaxNrNode];
  mem::Pool *pool;

  friend class EpochManager;
  EpochMemory(mem::Pool *pool);
  ~EpochMemory();
 public:
};

class Epoch : public EpochMemory {
 protected:
  uint64_t epoch_nr;
  friend class EpochManager;

  std::array<int, NodeConfiguration::kMaxNrNode> counter;
 public:
  Epoch(uint64_t epoch_nr, mem::Pool *pool) : epoch_nr(epoch_nr), EpochMemory(pool) {}
  template <typename T>
  EpochObject<T> AllocateEpochObject(int node_id) {
    auto off = brks[node_id - 1].off;
    brks[node_id - 1].off += util::Align(sizeof(T), 8);
    return EpochObject<T>(epoch_nr, node_id, off);
  }

  template <typename T>
  EpochObject<T> AllocateEpochObjectOnCurrentNode() {
    return AllocateEpochObject<T>(util::Instance<NodeConfiguration>().node_id());
  }

  uint64_t id() const { return epoch_nr; }
};

}

#endif /* EPOCH_H */
