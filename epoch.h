// -*- c++ -*-
#ifndef EPOCH_H
#define EPOCH_H

#include <cstdlib>
#include <sys/types.h>
#include <cstdint>
#include <vector>
#include <map>
#include <functional>

#include "net-io.h"
#include "sqltypes.h"
#include "csum.h"

namespace dolly {

typedef sql::VarStr VarStr;

struct TxnKey {
  uint16_t fid; // table id
  VarStr str; // must be the last field
};

class Txn {
  std::vector<TxnKey*> keys;
  unsigned int key_crc;
  unsigned int value_crc;
  uint64_t sid;
public:
  uint8_t type;

  Txn() : key_crc(INITIAL_CRC32_VALUE), value_crc(INITIAL_CRC32_VALUE) {}
  void Initialize(uint64_t sid, ParseBuffer &buffer, uint16_t key_pkt_len);
  void SetupReExec();

  unsigned int key_checksum() const { return key_crc; }
  unsigned int value_checksum() const { return value_crc; }

  uint64_t serializable_id() const { return sid; }
  virtual void Run() = 0;
  virtual int CoreAffinity() const = 0;
};

class BaseRequest : public Txn {
public:
  // for parsers to create request dynamically
  static BaseRequest *CreateRequestFromBuffer(uint64_t sid, ParseBuffer &buffer);

  // for workload-support plugins
  typedef std::map<uint8_t, std::function<BaseRequest* ()> > FactoryMap;
  static void LoadWorkloadSupport(const std::string &name);
  static void MergeFactoryMap(const FactoryMap &extra);
  static void LoadWorkloadSupportFromConf();

  virtual ~BaseRequest() {}
  virtual void ParseFromBuffer(ParseBuffer &buffer) = 0;

  static FactoryMap& GetGlobalFactoryMap() {
    static FactoryMap factory_map;
    return factory_map;
  }

private:
  static std::map<std::string, void *> support_handles;

};

template <class T>
class Request : public BaseRequest, public T {
  virtual void ParseFromBuffer(ParseBuffer &buffer);
  virtual void Run();
  virtual int CoreAffinity() const;
};

class Epoch {
public:
  Epoch(int *fds, ParseBuffer *buffers);
  static uint64_t CurrentEpochNumber();
private:
  std::vector<Txn*> txns;

protected:
  static uint64_t kGlobSID;
};

}

#endif /* EPOCH_H */
