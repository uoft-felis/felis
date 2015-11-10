#include <cassert>
#include "log.h"

#include "epoch.h"
#include "net-io.h"
#include "index.h"

namespace db_backup {

uint64_t Epoch::kGlobSID = 0ULL;

RelationManager<Relation<StdMapIndex, SortedArrayVersioning> > g_relation_mgr;

Epoch::Epoch(int fd)
{
  uint64_t tot_size = 0;
  ParseBuffer::FillDataFromFD(fd, &tot_size, sizeof(uint64_t));

  logger->info("epoch size {}", tot_size);

  tot_size -= sizeof(uint64_t);

  if (tot_size == 0)
    return;

  uint8_t *ptr = (uint8_t *) malloc(tot_size);
  ParseBuffer::FillDataFromFD(fd, ptr, tot_size);
  ParseBuffer buffer(ptr, tot_size);

  while (!buffer.is_empty()) {
    logger->debug("receiving request");
    BaseRequest *req = BaseRequest::CreateRequestFromBuffer(buffer);
    uint16_t key_pkt_size;
    buffer.Read(&key_pkt_size, sizeof(uint16_t));
    logger->debug("receiving keys, total len {}", key_pkt_size);

    Txn *txn = new Txn(++kGlobSID, buffer, key_pkt_size);

    requests.push_back(req);
    txns.push_back(txn);
  }

  // phase two: SetupReExec()
  PerfLog p;
  for (auto t: txns) {
    t->SetupReExec();
  }
  p.Show("SetupReExec takes");

  g_relation_mgr.LogStat();
}

Txn::Txn(uint64_t id, ParseBuffer &buffer, uint16_t key_pkt_len) : sid(id)
{
  int cur = 0;
  while (cur < key_pkt_len) {
    uint16_t fid;
    uint8_t len;
    buffer.Read(&fid, sizeof(uint16_t));
    buffer.Read(&len, sizeof(uint8_t));
    assert(len > 0);
    // logger->debug("  key len {0:d} in table {0:d}", k.len, k.fid);
    TxnKey *k = (TxnKey *) malloc(sizeof(TxnKey) + len);
    k->fid = fid;
    k->len = len;
    buffer.Read(k->data, k->len);
    // logger->debug("  key data {}", (const char *) k.data);
    cur += sizeof(uint16_t) + sizeof(uint8_t) + k->len;

    keys.push_back(k);
  }
  assert(cur == key_pkt_len);
}

void Txn::SetupReExec()
{
  for (auto kptr : keys) {
    auto &relation = g_relation_mgr.GetRelationOrCreate(kptr->fid);
    relation.SetupReExec(IndexKey(kptr), sid);
  }
}

}
