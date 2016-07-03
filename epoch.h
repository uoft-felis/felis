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

class Txn {
  TxnKey *keys;
  uint16_t sz_key_buf;
  unsigned int key_crc;
  unsigned int value_crc;
  uint64_t sid;
public:
  uint8_t type;

  Txn() : key_crc(INITIAL_CRC32_VALUE), value_crc(INITIAL_CRC32_VALUE) {}

  void Initialize(go::InputSocketChannel *channel, uint16_t key_pkt_len, Epoch *epoch);
  void SetupReExec();

  unsigned int key_checksum() const { return key_crc; }
  unsigned int value_checksum() const { return value_crc; }

  void set_serializable_id(uint64_t id) { sid = id; }
  uint64_t serializable_id() const { return sid; }
  virtual void Run() = 0;
  virtual int CoreAffinity() const = 0;
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
  virtual void Run();
  virtual int CoreAffinity() const;
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
    __builtin_prefetch(p);
    brks[cpu].offset += sz;
    if (brks[cpu].offset > kBrkSize)
      std::abort();
    return p;
  }

private:
  void InitBrks();
  void DestroyBrks();

private:
  struct {
    uint8_t *addr;
    int offset;
  } brks[31]; // hack: max nr cpu
  std::vector<Txn *> txns;
protected:
  static uint64_t kGlobSID;
public:
  static const size_t kBrkSize = 32 << 20;
  static std::mutex pool_mutex;
  static mem::LargePool<kBrkSize> *pool;
};

}

#endif /* EPOCH_H */
