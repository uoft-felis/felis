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

// #include "net-io.h"
#include "sqltypes.h"
#include "csum.h"

#include "mem.h"
#include "log.h"

#include "gopp/gopp.h"
#include "gopp/channels.h"
#include "gopp/barrier.h"

// #define VALIDATE_TXN 1
// #define VALIDATE_TXN_KEY 1

namespace dolly {

class ParseBufferEOF : public std::exception {
 public:
  virtual const char *what() const noexcept {
    return "ParseBuffer EOF";
  }
};

typedef sql::VarStr VarStr;

struct TxnKey {
  int16_t fid; // table id
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

  bool is_empty() const { return next == this && prev == this; }
};

class Relation;

class Txn : public go::Routine {
#ifdef CALVIN_REPLAY
  uint8_t *read_set_keys;
  uint32_t sz_read_set_key_buf;
#endif
  uint8_t *keys;
  uint16_t sz_key_buf;
  unsigned int key_crc;
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

  Txn() : key_crc(INITIAL_CRC32_VALUE), is_setup(true) {
    set_reuse(true);
  }

#ifdef CALVIN_REPLAY
  void InitializeReadSet(go::TcpInputChannel *channel, uint32_t read_key_pkt_size, Epoch *epoch);
#endif
  void Initialize(go::TcpInputChannel *channel, uint16_t key_pkt_len, Epoch *epoch);
  void SetupReExec();

  unsigned int key_checksum() const { return key_crc; }

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

  uint8_t *key_buffer() const { return keys; }
  uint16_t key_buffer_size() const { return sz_key_buf; }
 protected:
  virtual void Run();
 private:
  void GenericSetupReExec(uint8_t *key_buffer, size_t key_len,
                          std::function<bool (Relation *, const VarStr *, uint64_t)> callback);
};

class BaseRequest : public Txn {
 public:
  // for parsers to create request dynamically
  static BaseRequest *CreateRequestFromChannel(go::TcpInputChannel *channel, Epoch *epoch);

  // for workload-support plugins
  typedef std::map<uint8_t, std::function<BaseRequest* (Epoch *)> > FactoryMap;
  static void LoadWorkloadSupport(const std::string &name);
  static void MergeFactoryMap(const FactoryMap &extra);
  static void LoadWorkloadSupportFromConf();

  virtual ~BaseRequest() {}
  virtual void ParseFromChannel(go::TcpInputChannel *channel) = 0;

  static FactoryMap& GetGlobalFactoryMap() {
    return factory_map;
  }

 private:
  static FactoryMap factory_map;
  static std::map<std::string, void *> support_handles;
};

template <class T>
class Request : public BaseRequest, public T {
  virtual void ParseFromChannel(go::TcpInputChannel *channel);
  virtual void RunTxn();
  virtual int CoreAffinity() const;
};

class TxnIOReader;

class Epoch {
 public:
  Epoch(std::vector<go::TcpSocket *> socks);
  ~Epoch();
  static uint64_t CurrentEpochNumber();

  void *AllocFromBrk(int cpu, size_t sz) {
    if (brks[cpu].offset + sz >= kBrkSize) {
      logger->critical("brk is full {}, current {}", kBrkSize, brks[cpu].offset);
      std::abort();
    }
    void *p = brks[cpu].addr + brks[cpu].offset;
    brks[cpu].offset += sz;
    return p;
  }

  int IssueReExec();
  void WaitForReExec(int total = kNrThreads);

  go::BufferChannel *channel() { return wait_channel; }

#ifdef NR_THREADS
  static const int kNrThreads = NR_THREADS;
#else
  static const int kNrThreads = 16;
#endif

 private:
  void InitBrks();
  void DestroyBrks();

 private:
  go::BufferChannel *wait_channel;
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
  static const size_t kBrkSize;
  typedef mem::Pool<true> BrkPool;
  static BrkPool *pools;
};

}

#endif /* EPOCH_H */
