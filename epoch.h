// -*- c++ -*-
#ifndef EPOCH_H
#define EPOCH_H

#include <cstdlib>
#include <sys/types.h>
#include <cstdint>
#include <vector>

#include "net-io.h"
#include "request.h"

namespace db_backup {

struct VarStr {
  uint8_t len;
  uint8_t data[];
};

struct TxnKey {
  uint16_t fid; // table id
  uint8_t len;
  uint8_t data[];
};

class Txn {
  std::vector<TxnKey*> keys;
  uint64_t sid;
public:
  Txn(uint64_t sid, ParseBuffer &buffer, uint16_t key_pkt_len);
  void SetupReExec();

  uint64_t serializable_id() const { return sid; }
};

class Epoch {
public:
  Epoch(int fd);
private:
  std::vector<BaseRequest*> requests;
  std::vector<Txn*> txns;

protected:
  static uint64_t kGlobSID;
};

}

#endif /* EPOCH_H */
