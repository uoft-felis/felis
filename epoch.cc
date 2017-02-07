#include <cassert>
#include <fstream>
#include <future>
#include <sstream>
#include <dlfcn.h>
#include "log.h"

#include "epoch.h"
#include "net-io.h"
#include "index.h"
#include "util.h"

#include "json11/json11.hpp"

#include "gopp/gopp.h"
#include "gopp/epoll-channel.h"

#include "csum.h"

using util::Instance;

namespace dolly {

uint64_t Epoch::kGlobSID = 0ULL;

static uint64_t gGlobalEpoch = 0;

class TxnIOReader : public go::Routine {
  go::EpollSocket *sock;
  go::WaitBarrier *barrier;
  Epoch *epoch;
  Txn::FinishCounter fcnt;
  TxnQueue *reuse_q;

public:
  TxnIOReader(go::EpollSocket *s, go::WaitBarrier *bar, Epoch *e, TxnQueue *q)
    : sock(s), barrier(bar), epoch(e), reuse_q(q) {
    set_reuse(true);
  }

  Txn::FinishCounter *finish_counter() { return &fcnt; }

protected:
  virtual void Run();
};

void TxnIOReader::Run()
{
  /* Dealing with EOF and end of epoch is easy
   * End of epoch: add another TxnIOReader, and return.
   * EOF: just return
   */
  auto *channel = sock->input_channel();

  uint64_t last_commit_ts = 0;

  uint64_t commit_ts;
  int64_t skew_ts;
  bool eof = false;
  int count_max = 0;

  while (true) {
    if (!channel->Read(&commit_ts, sizeof(uint64_t))) {
      eof = true;
      break; // EOF
    }

    if (commit_ts == 0) {
      break;
    }

    if (!channel->Read(&skew_ts, sizeof(int64_t))) {
      eof = true;
      break; // EOF
    }
    logger->debug("read ts {:x} {:x}", commit_ts, skew_ts);
    if (commit_ts < last_commit_ts) {
      logger->error("last commit ts {} commit_ts {}", last_commit_ts, commit_ts);
      std::abort();
    }
    last_commit_ts = commit_ts;

    auto req = BaseRequest::CreateRequestFromChannel(channel, epoch);

    count_max++;
    uint64_t sid = skew_ts == 0 ? commit_ts * 2 : skew_ts * 2 - 1;

    // for debugging
/*
    sid <<= 16;
    sid |= (uint16_t) count_max - 1;
    sid <<= 8;
    sid |= (uint8_t) go::Scheduler::CurrentThreadPoolId();
*/

    req->set_serializable_id(sid);
    req->set_wait_barrier(barrier);
    req->set_counter(&fcnt);
    req->set_epoch(epoch);
    req->set_reuse_queue(reuse_q);

    req->PushToReuseQueue();
  }
  fcnt.max = count_max; // should never be 0 here
  logger->info("finished, socket ptr {} on {} cnt {}", (void *) sock,
	       go::Scheduler::CurrentThreadPoolId(), count_max);
  if (eof)
    epoch->channel()->Write(uint8_t{1});
  else
    epoch->channel()->Write(uint8_t{0});
}

class TxnRunner : public go::Routine {
  TxnQueue *queue;
public:
  TxnRunner(TxnQueue *q) : queue(q) {}
  virtual void Run();
};

void TxnRunner::Run()
{
  auto ent = queue->next;
  int tot = 0;
  bool done = false;
  while (ent != queue) {
    auto next = ent->next;

    ent->t->Reset();
    ent->t->StartOn(go::Scheduler::CurrentThreadPoolId());
    if (next == queue) {
      go::Scheduler::Current()->WakeUp(ent->t);
    } else {
      go::Scheduler::Current()->WakeUp(ent->t, true);
    }

    ent->Detach();
    ent = next;
    tot++;
  }
  logger->info("{} issued {}", go::Scheduler::CurrentThreadPoolId(), tot);
}

void Txn::Run()
{
  if (is_setup) {
    SetupReExec();
    is_setup = false;

    // Run me later again for RunTxn(), but now, we'll exit.
    // Exiting right now provide fastpath context switch.
    PushToReuseQueue();

    // logger->info("setting up {} done {}/{} on {}", serializable_id(), fcnt->count + 1,
    //  		 fcnt->max, go::Scheduler::CurrentThreadPoolId());

    if (++fcnt->count == fcnt->max) {
      fcnt->count = 0;
      barrier->Wait();
      auto runner = new TxnRunner(reuse_q);
      runner->StartOn(go::Scheduler::CurrentThreadPoolId());
      logger->info("{} has total {} txns", go::Scheduler::CurrentThreadPoolId(), fcnt->max);
    }
  } else {
    try {
      RunTxn();
      // logger->info("{} done on thread {}", serializable_id(), go::Scheduler::CurrentThreadPoolId());
    } catch (...) {
      DebugKeys();
      std::abort();
    }
    if (++fcnt->count == fcnt->max) {
      // notify the driver thread, which only used to hold and free stuff...
      logger->info("all txn replayed done on thread {}", go::Scheduler::CurrentThreadPoolId());
      epoch->channel()->Write(uint8_t{0});
    }
  }
}

const int Epoch::kNrThreads;

Epoch::Epoch(std::vector<go::EpollSocket *> socks)
{
  InitBrks();

  PerfLog p;
  wait_channel = new go::BufferChannel<uint8_t>(kNrThreads);
  wait_barrier = new go::WaitBarrier(kNrThreads);

  for (int i = 0; i < kNrThreads; i++) {
    auto sock = socks[i];
    reuse_q[i].Init();

    readers[i] = new TxnIOReader(socks[i], wait_barrier, this, &reuse_q[i]);
    readers[i]->StartOn(i + 1);
  }

  {
    bool eof = false;
    bool need_throw = false;
    uint8_t res[kNrThreads];

    wait_channel->Read(res, kNrThreads);
    for (int i = 0; i < kNrThreads; i++) {
      if (res[i] != 0) throw ParseBufferEOF();
    }
  }
  p.Show("IO takes");
}

Epoch::~Epoch()
{
  delete wait_barrier;
  for (int i = 0; i < kNrThreads; i++) {
    delete readers[i];
  }
  DestroyBrks();
}

int Epoch::IssueReExec()
{
  ++gGlobalEpoch;
  int nr_wait = 0;
  uint64_t tot_txns = 0;
  for (int i = 0; i < kNrThreads; i++) {
    if (reuse_q[i].is_empty()) continue;
    nr_wait++;
  }

  wait_barrier->Adjust(nr_wait);

  for (int i = 0; i < kNrThreads; i++) {
    auto runner = new TxnRunner(&reuse_q[i]);
    runner->StartOn(i + 1);
    tot_txns += readers[i]->finish_counter()->max;
  }
  logger->info("epoch contains {}", tot_txns);
  return nr_wait;
}

void Epoch::WaitForReExec(int tot)
{
  uint8_t res[tot];
  wait_channel->Read(res, tot);
}

Epoch::BrkPool *Epoch::pools;

void Epoch::InitBrks()
{
  for (int i = 0; i < kNrThreads; i++) {
    brks[i].addr = (uint8_t *) pools[i / mem::kNrCorePerNode].Alloc();
    brks[i].offset = 0;
  }
}

void Epoch::DestroyBrks()
{
  for (int i = 0; i < kNrThreads; i++) {
    pools[i / mem::kNrCorePerNode].Free(brks[i].addr);
  }
}

uint64_t Epoch::CurrentEpochNumber()
{
  return gGlobalEpoch;
}

// #define VALIDATE_TXN_KEY 1

void Txn::Initialize(go::InputSocketChannel *channel, uint16_t key_pkt_len, Epoch *epoch)
{
  int cpu = go::Scheduler::CurrentThreadPoolId() - 1;
  assert(key_pkt_len >= 8);
  uint8_t *buffer = (uint8_t *) epoch->AllocFromBrk(cpu, key_pkt_len);

  channel->Read(buffer, key_pkt_len);

  keys = (TxnKey *) buffer;
  sz_key_buf = key_pkt_len - 8;
  value_crc = *(uint32_t *) (buffer + sz_key_buf + 4);

#ifdef VALIDATE_TXN_KEY
  uint32_t orig_key_crc = *(uint32_t *) (buffer + sz_key_buf), key_crc = INITIAL_CRC32_VALUE;
  uint8_t *p = buffer;
  while (p < buffer + sz_key_buf) {
    TxnKey *k = (TxnKey *) p;
    update_crc32(k->str.data, k->str.len, &key_crc);
    p += sizeof(TxnKey) + k->str.len;
  }
  if (orig_key_crc != key_crc) {
    logger->critical("Key crc doesn't match!");
    std::abort();
  }
  logger->debug("key csum {:x} matches", key_crc);
#endif
}

void Txn::SetupReExec()
{
  /*
   * TODO:
   * Here, we could have SetupReExec() synchronously. However, it might
   * contend on some workloads. So, let's do this asynchronously.
   *
   * Since kptr->fid should never be 0, let's clear that to 0 every time we
   * sucessfully SetupReExec() one key.
   */
  auto &mgr = Instance<RelationManager>();

  uint8_t *key_buffer = (uint8_t *) keys;
  uint8_t *p = key_buffer;

  while (p < key_buffer + sz_key_buf) {
    TxnKey *kptr = (TxnKey *) p;
    VarStr var_str;
    var_str.len = kptr->len;
    var_str.data = kptr->data;
    auto &relation = mgr.GetRelationOrCreate(kptr->fid);
    relation.SetupReExec(&var_str, sid);
    p += sizeof(TxnKey) + kptr->len;
  }
}

void Txn::DebugKeys()
{
  uint8_t *key_buffer = (uint8_t *) keys;
  uint8_t *p = key_buffer;
  while (p < key_buffer + sz_key_buf) {
    TxnKey *kptr = (TxnKey *) p;
    std::stringstream ss;
    for (int i = 0; i < kptr->len; i++) {
      char buf[8];
      snprintf(buf, 8, "0x%x ", kptr->data[i]);
      ss << buf;
    }
    logger->debug("sid {} Debug Fid: {} Keys: {}", sid, kptr->fid, ss.str());
    p += sizeof(TxnKey) + kptr->len;
  }
}

BaseRequest *BaseRequest::CreateRequestFromChannel(go::InputSocketChannel *channel, Epoch *epoch)
{
  uint8_t type = 0;
  if (!channel->Read(&type, 1))
    std::abort();
  assert(type != 0);
  assert(type <= GetGlobalFactoryMap().rbegin()->first);
  logger->debug("txn req type {0:d}", type);
  auto req = GetGlobalFactoryMap().at(type)(epoch);
  req->type = type;
  uint16_t key_pkt_size;
  req->ParseFromChannel(channel);
  channel->Read(&key_pkt_size, sizeof(uint16_t));
  logger->debug("receiving keys, total len {}", key_pkt_size);
  req->Initialize(channel, key_pkt_size, epoch);

  return req;
}

std::map<std::string, void *> BaseRequest::support_handles;
BaseRequest::FactoryMap BaseRequest::factory_map;

void BaseRequest::LoadWorkloadSupport(const std::string &name)
{
  if (support_handles.find(name) != support_handles.end()) {
    logger->error("Workload Support {} already loaded", name);
    return;
  }
  void *handle = dlopen(name.c_str(), RTLD_LAZY);
  if (handle == NULL) {
    logger->error("Cannot load {}, error {}", name, dlerror());
    return;
  }
  support_handles[name] = handle;

  typedef void (*InitializeFunctionPointer)();
  InitializeFunctionPointer init_fp =
    (InitializeFunctionPointer) dlsym(handle, "InitializeWorkload");
  init_fp();
}

// TODO: unload all dl handles!

void BaseRequest::MergeFactoryMap(const FactoryMap &extra)
{
  auto &factory_map = GetGlobalFactoryMap();
  for (auto it = extra.begin(); it != extra.end(); ++it) {
    if (factory_map.find(it->first) != factory_map.end()) {
      logger->error("Cannot register transaction type {} twice!", it->first);
      std::abort();
    }
    logger->info("registered tx with type {}", (int) it->first);
    factory_map.insert(*it);
  }
}

void BaseRequest::LoadWorkloadSupportFromConf()
{
  std::string err;
  std::ifstream fin("workload_support.json");
  std::string conf_text {
    std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>() };
  json11::Json conf_doc = json11::Json::parse(conf_text, err);

  if (!err.empty()) {
    logger->critical(err);
    logger->critical("Cannot load workload support configuration");
    std::abort();
  }

  auto json_arr = conf_doc.array_items();
  for (auto it = json_arr.begin(); it != json_arr.end(); ++it) {
    logger->info("Loading Workload Support {}", it->string_value());
    LoadWorkloadSupport(it->string_value());
  }
}

}
