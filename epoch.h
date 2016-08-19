// -*- c++ -*-
#ifndef EPOCH_H
#define EPOCH_H

#include <cstdlib>
#include <sys/types.h>
#include <cstdint>
#include <vector>
#include <map>
#include <mutex>
#include <functional>
#include <atomic>
#include <limits.h>

#include "net-io.h"
#include "sqltypes.h"
#include "csum.h"

#include "mem.h"

#include "goplusplus/gopp.h"
#include "goplusplus/epoll-channel.h"

namespace dolly {

typedef sql::VarStr VarStr;

struct TxnKey {
  uint16_t fid; // table id
  uint8_t len;
  uint8_t data[];
} __attribute__((packed));

class Epoch;

class DivergentOutputException : public std::exception {
public:
};

class Txn;

struct TxnQueue {
  Txn *t;
  TxnQueue *next, *prev;

  void Init() {
    prev = next = this;
  }

  void Add(TxnQueue *parent) {
    prev = parent;
    next = parent->next;

    next->prev = this;
    prev->next = this;
  }

  void Detach() {
    next->prev = prev;
    prev->next = next;
    prev = next = nullptr;
  }
};

class Txn : public go::Routine {
  TxnKey *keys;
  uint16_t sz_key_buf;
  unsigned int key_crc;
  unsigned int value_crc;
  uint64_t sid;
  bool is_setup;

  go::WaitBarrier *barrier;
  int *count;
  int count_max;
  Epoch *epoch;

  TxnQueue node;
  TxnQueue *reuse_q;

public:
  uint8_t type;

  struct FinishCounter {
    int count = 0;
    int max = INT_MAX;
  } *fcnt;

  Txn() : key_crc(INITIAL_CRC32_VALUE), value_crc(INITIAL_CRC32_VALUE),
	  is_setup(true) {
    set_reuse(true);
  }

  void Initialize(go::InputSocketChannel *channel, uint16_t key_pkt_len, Epoch *epoch);
  void SetupReExec();

  unsigned int key_checksum() const { return key_crc; }
  unsigned int value_checksum() const { return value_crc; }

  void set_serializable_id(uint64_t id) { sid = id; }
  uint64_t serializable_id() const { return sid; }

  void set_wait_barrier(go::WaitBarrier *b) { barrier = b; }
  void set_counter(FinishCounter *cnt) { fcnt = cnt; }
  void set_epoch(Epoch *e) { epoch = e; }
  void set_reuse_queue(TxnQueue *reuse_queue) { reuse_q = reuse_queue; }

  void PushToReuseQueue() {
    node.t = this;
    node.Add(reuse_q->prev);
  }

  virtual void RunTxn() = 0;
  virtual int CoreAffinity() const = 0;

  void DebugKeys();
protected:
  virtual void Run();
};

class BaseRequest : public Txn {
public:
  // for parsers to create request dynamically
  static BaseRequest *CreateRequestFromChannel(go::InputSocketChannel *channel, Epoch *epoch);

  // for workload-support plugins
  typedef std::map<uint8_t, std::function<BaseRequest* (Epoch *)> > FactoryMap;
  static void LoadWorkloadSupport(const std::string &name);
  static void MergeFactoryMap(const FactoryMap &extra);
  static void LoadWorkloadSupportFromConf();

  virtual ~BaseRequest() {}
  virtual void ParseFromChannel(go::InputSocketChannel *channel) = 0;

  static FactoryMap& GetGlobalFactoryMap() {
    return factory_map;
  }

private:
  static FactoryMap factory_map;
  static std::map<std::string, void *> support_handles;
};

template <class T>
class Request : public BaseRequest, public T {
  virtual void ParseFromChannel(go::InputSocketChannel *channel);
  virtual void RunTxn();
  virtual int CoreAffinity() const;
};

struct TxnTimeStamp
{
  uint64_t commit_ts;
  int64_t skew_ts;
  Txn *txn;

  uint64_t logical_commit_ts() const {
    return skew_ts == 0 ? commit_ts : skew_ts - 1;
  }

  bool operator<(const TxnTimeStamp &rhs) const {
    if (logical_commit_ts() != rhs.logical_commit_ts()) {
      return logical_commit_ts() < rhs.logical_commit_ts();
    }
    return commit_ts < rhs.commit_ts;
  }
};

class TxnIOReader;

class Epoch {
public:
  Epoch(std::vector<go::EpollSocket *> socks);
  ~Epoch();
  static uint64_t CurrentEpochNumber();

  void *AllocFromBrk(int cpu, size_t sz) {
    // if (brks[cpu].offset + sz >= kBrkSize) std::abort();
    void *p = brks[cpu].addr + brks[cpu].offset;
    brks[cpu].offset += sz;
    return p;
  }

  void IssueReExec();
  void WaitForReExec();

  go::BufferChannel<uint8_t> *channel() { return wait_channel; }

#ifdef NR_THREADS
  static const int kNrThreads = NR_THREADS;
#else
  static const int kNrThreads = 16;
#endif

private:
  void InitBrks();
  void DestroyBrks();

private:
  go::BufferChannel<uint8_t> *wait_channel;
  TxnIOReader *readers[kNrThreads];
  go::WaitBarrier *wait_barrier;
  TxnQueue reuse_q[kNrThreads];

  struct {
    uint8_t *addr;
    int offset;
    char __padding__[4];
  } brks[kNrThreads];


protected:
  static uint64_t kGlobSID;
public:
  static const size_t kBrkSize = 16 << 20;
  typedef mem::Pool<true> BrkPool;
  static BrkPool *pools;
};

}

#endif /* EPOCH_H */
