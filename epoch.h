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

#include "sqltypes.h"
#include "txn.h"

#include "mem.h"
#include "log.h"

#include "gopp/gopp.h"
#include "gopp/channels.h"
#include "gopp/barrier.h"

namespace dolly {

class ParseBufferEOF : public std::exception {
 public:
  virtual const char *what() const noexcept {
    return "ParseBuffer EOF";
  }
};

class Epoch;

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
};

template <class T>
class Request : public BaseRequest, public T {
  virtual void ParseFromChannel(go::TcpInputChannel *channel);
  virtual void RunTxn();
  virtual int CoreAffinity() const;
};

class Epoch {
 public:
  Epoch(std::vector<go::TcpSocket *> socks);
  ~Epoch();
  static uint64_t CurrentEpochNumber();

  void *AllocFromBrk(int cpu, size_t sz) {
    return brks[cpu].Alloc(sz);
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

  mem::Brk brks[kNrThreads];

 protected:
  static uint64_t kGlobSID;
 private:
  static const size_t kBrkSize;
  static mem::Pool *pools;
 public:
  static void InitPools();
};

}

#endif /* EPOCH_H */
