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

class Txn : public go::Routine {
  TxnKey *keys;
  uint16_t sz_key_buf;
  unsigned int key_crc;
  unsigned int value_crc;
  uint64_t sid;
  bool is_setup;

  go::BufferChannel<uint8_t> *wait_channel;
  int *count_down;

public:
  uint8_t type;

  Txn() : key_crc(INITIAL_CRC32_VALUE), value_crc(INITIAL_CRC32_VALUE),
	  is_setup(true), wait_channel(nullptr) {
    set_reuse(true);
  }

  void Initialize(go::InputSocketChannel *channel, uint16_t key_pkt_len, Epoch *epoch);
  void SetupReExec();

  unsigned int key_checksum() const { return key_crc; }
  unsigned int value_checksum() const { return value_crc; }

  void set_serializable_id(uint64_t id) { sid = id; }
  uint64_t serializable_id() const { return sid; }

  void set_wait_channel(go::BufferChannel<uint8_t> *ch) { wait_channel = ch; }
  void set_count_down(int *c) { count_down = c; }

  virtual void RunTxn() = 0;
  virtual int CoreAffinity() const = 0;
protected:
  virtual void Run() {
    if (is_setup) {
      SetupReExec();
      is_setup = false;
    } else {
      RunTxn();
    }
    --(*count_down);
    // fprintf(stderr, "countdown %d\n", *count_down);
    if (*count_down == 0 && wait_channel) {
      // fprintf(stderr, "done, notify control thread\n");
      wait_channel->Write(uint8_t{0});
    }
  }
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

class Epoch {
public:
  Epoch(std::vector<go::EpollSocket *> socks);
  ~Epoch() { DestroyBrks(); }
  static uint64_t CurrentEpochNumber();

  void Setup();
  void ReExec();

  void *AllocFromBrk(int cpu, size_t sz) {
    void *p = brks[cpu].addr + brks[cpu].offset;
    brks[cpu].offset += sz;
    if (brks[cpu].offset > kBrkSize)
      std::abort();
    return p;
  }

  static const int kNrThreads = 16;

private:
  void InitBrks();
  void DestroyBrks();

private:
  struct {
    uint8_t *addr;
    int offset;
    char __padding__[4];
  } brks[kNrThreads];

  int count_downs[kNrThreads];
  go::BufferChannel<uint8_t> *wait_channel;
  std::vector<TxnTimeStamp> tss[kNrThreads];

protected:
  static uint64_t kGlobSID;
public:
  static const size_t kBrkSize = 32 << 20;
  typedef mem::Pool<true> BrkPool;
  static BrkPool *pools;
};

}

#endif /* EPOCH_H */
