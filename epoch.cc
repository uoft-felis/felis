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

#include "goplusplus/gopp.h"
#include "goplusplus/epoll-channel.h"

#include "csum.h"

using util::Instance;

namespace dolly {

uint64_t Epoch::kGlobSID = 0ULL;

static uint64_t gGlobalEpoch;

class TxnIOReader : public go::Routine {
  std::vector<TxnTimeStamp> *ts_vec;
  go::EpollSocket *sock;
  go::BufferChannel<uint8_t> *wait_channel;
  Epoch *epoch;
public:
  TxnIOReader(std::vector<TxnTimeStamp> *tss, go::EpollSocket *s, go::BufferChannel<uint8_t> *ch,
	      Epoch *e)
    : ts_vec(tss), sock(s), wait_channel(ch), epoch(e) {}

protected:
  virtual void Run() {
    auto *channel = sock->input_channel();
    bool pb_eof = false;

    uint64_t commit_ts;
    int64_t skew_ts;
    while (true) {
      if (!channel->Read(&commit_ts, sizeof(uint64_t))) {
	pb_eof = true;
	break;
      }

      if (commit_ts == 0) {
	break;
      }

      if (!channel->Read(&skew_ts, sizeof(int64_t))) {
	pb_eof = true;
	break;
      }
      logger->debug("read ts {:x} {:x}", commit_ts, skew_ts);
      auto req = BaseRequest::CreateRequestFromChannel(channel, epoch);
      ts_vec->emplace_back(TxnTimeStamp{commit_ts, skew_ts, req});
    }
    std::sort(ts_vec->begin(), ts_vec->end());
    wait_channel->Write((uint8_t) (pb_eof ? 1 : 0));
  }
};

const int Epoch::kNrThreads;

Epoch::Epoch(std::vector<go::EpollSocket *> socks)
{
  ++gGlobalEpoch;
  InitBrks();

  PerfLog p;
  size_t tss_offset[kNrThreads];
  TxnIOReader *readers[kNrThreads];
  wait_channel = new go::BufferChannel<uint8_t>(kNrThreads);

  for (int i = 0; i < kNrThreads; i++) {
    auto sock = socks[i];
    tss_offset[i] = 0;

    readers[i] = new TxnIOReader(&tss[i], socks[i], wait_channel, this);
    readers[i]->StartOn(i + 1);
  }

  {
    bool eof = false;
    bool need_throw = false;
    for (int i = 0; i < kNrThreads; i++) {
      uint8_t ch = wait_channel->Read(eof);
      if (ch) need_throw = true;
    }
    if (need_throw) throw ParseBufferEOF();
  }
  p.Show("IO takes");

  p = PerfLog();
  logger->info("merge sorting...");
  int nr_total = 0;
  while (true) {
    TxnTimeStamp ts;
    int ts_idx = -1;
    for (int i = 0; i < kNrThreads; i++) {
      if (tss_offset[i] == tss[i].size()) continue;

      if (ts_idx == -1 || tss[i][tss_offset[i]] < ts) {
	ts = tss[i][tss_offset[i]];
	ts_idx = i;
      }
    }
    if (ts_idx == -1) break;
    tss_offset[ts_idx]++;
    nr_total++;

    auto t = ts.txn;
    t->set_serializable_id(++kGlobSID);
    t->set_wait_channel(wait_channel);
    t->set_count_down(&count_downs[ts_idx]);

    logger->debug("txn sid {} type 0x{:x} value csum 0x{:x} timestamp commit_ts {} skew_ts {}",
		  t->serializable_id(), t->type, t->value_checksum(), ts.commit_ts, ts.skew_ts);
  }
  logger->info("epoch contains {} txns", nr_total);
  p.Show("Sorting takes");
}

std::mutex Epoch::pool_mutex;
mem::LargePool<Epoch::kBrkSize> *Epoch::pool;

void Epoch::InitBrks()
{
  std::unique_lock<std::mutex> l(pool_mutex);
  for (int i = 0; i < kNrThreads; i++) {
    brks[i].addr = (uint8_t *) pool->Alloc();
    brks[i].offset = 0;
  }
}

void Epoch::DestroyBrks()
{
  logger->info("deleting epoch and its mem");
  std::unique_lock<std::mutex> l(pool_mutex);
  for (int i = 0; i < kNrThreads; i++) {
    pool->Free(brks[i].addr);
  }
}

void Epoch::Setup()
{
  logger->info("setting up epoch");
  auto p = PerfLog();

  for (int i = 0; i < kNrThreads; i++) {
    count_downs[i] = tss[i].size();
    auto sched = go::GetSchedulerFromPool(i + 1);
     for (auto &t: tss[i]) {
       sched->WakeUp(t.txn, true);
    }
    sched->Signal();
  }

  for (int i = 0; i < kNrThreads; i++) {
    bool eof = false;
    uint8_t ch = wait_channel->Read(eof);
  }
  p.Show("SetupReExec takes");
}

void Epoch::ReExec()
{
  logger->info("replaying");
  auto p = PerfLog();
  for (int i = 0; i < kNrThreads; i++) {
    count_downs[i] = tss[i].size();
    auto sched = go::GetSchedulerFromPool(i + 1);
    for (auto &t: tss[i]) {
      if (!t.txn->is_detached()) std::abort();
      t.txn->Reset();
      sched->WakeUp(t.txn, true);
    }
    sched->Signal();
  }
  for (int i = 0; i < kNrThreads; i++) {
    bool eof = false;
    uint8_t ch = wait_channel->Read(eof);
  }
  p.Show("ReExec takes");
}

uint64_t Epoch::CurrentEpochNumber()
{
  return gGlobalEpoch;
}

// #define VALIDATE_TXN_KEY 1

void Txn::Initialize(go::InputSocketChannel *channel, uint16_t key_pkt_len, Epoch *epoch)
{
  int cpu = go::Scheduler::CurrentThreadPoolId() - 1;
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

BaseRequest *BaseRequest::CreateRequestFromChannel(go::InputSocketChannel *channel, Epoch *epoch)
{
  uint8_t type = 0;
  __builtin_prefetch(epoch);
  channel->Read(&type, 1);
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
