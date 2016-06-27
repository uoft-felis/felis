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
#include "worker.h"

#include "json11/json11.hpp"

#include "goplusplus/gopp.h"
#include "goplusplus/epoll-channel.h"

#include "csum.h"

using util::Instance;

namespace dolly {

uint64_t Epoch::kGlobSID = 0ULL;

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

static uint64_t gGlobalEpoch;

Epoch::Epoch(std::vector<go::EpollSocket *> socks)
{
  ++gGlobalEpoch;
  int nr_threads = Worker::kNrThreads;
  PerfLog p;
  std::vector<bool> eop_bitmap;

  for (int i = 0; i < nr_threads; i++) {
    eop_bitmap.push_back(false);
  }

  std::vector<TxnTimeStamp> tss;

  while (true) {
    int empty_cnt = 0;
    for (int i = 0; i < nr_threads; i++) {
    again:
      if (eop_bitmap[i]) {
	empty_cnt++;
	continue;
      }

      auto *channel = socks[i]->input_channel();
      uint64_t commit_ts;
      int64_t skew_ts;

      if (!channel->Read(&commit_ts, sizeof(uint64_t)))
	throw ParseBufferEOF();

      if (commit_ts == 0) {
	eop_bitmap[i] = true;
	continue;
      }
      if (!channel->Read(&skew_ts, sizeof(int64_t)))
	throw ParseBufferEOF();

      logger->debug("read ts {:x} {:x}", commit_ts, skew_ts);
      auto req = BaseRequest::CreateRequestFromChannel(channel);
      tss.emplace_back(TxnTimeStamp{commit_ts, skew_ts, req});
      goto again;
    }
    if (empty_cnt == nr_threads)
      break;
  }
  p.Show("Reading txn takes");
  p = PerfLog();
  std::sort(tss.begin(), tss.end());

  for (auto &ts: tss) {
    ts.txn->set_serializable_id(++kGlobSID);
    logger->debug("txn sid {} type 0x{:x} value csum 0x{:x} timestamp commit_ts {} skew_ts {}",
		  ts.txn->serializable_id(), ts.txn->type, ts.txn->value_checksum(),
		  ts.commit_ts, ts.skew_ts);
    txns.push_back(ts.txn);
  }

  logger->info("epoch contains {} txns", tss.size());
  p.Show("Sorting txn takes");
}

void Epoch::Setup()
{
  // phase two: SetupReExec()
  int counter = 0;
  std::mutex m;
  std::condition_variable cv;

  auto p = PerfLog();
  for (auto t: txns) {
    int aff = t->CoreAffinity();
    Instance<WorkerManager>().GetWorker(aff).AddTask([t]() {
	t->SetupReExec();
      }, true);
  }
  for (int i = 0; i < Worker::kNrThreads; i++) {
    Instance<WorkerManager>().GetWorker(i).AddTask([&m, &cv, &counter]() {
	std::lock_guard<std::mutex> l(m);
	counter++;
	cv.notify_one();
      });
  }
  {
    std::unique_lock<std::mutex> l(m);
    while (counter < Worker::kNrThreads)
      cv.wait(l);
    counter = 0;
  }

  p.Show("SetupReExec takes");
}

void Epoch::ReExec()
{
  int counter = 0;
  std::mutex m;
  std::condition_variable cv;

  auto p = PerfLog();
  for (auto &t: txns) {
    int aff = t->CoreAffinity();
    Instance<WorkerManager>().GetWorker(aff).AddTask([t]() {
	t->Run();
      }, true);
  }
  for (int i = 0; i < Worker::kNrThreads; i++) {
    Instance<WorkerManager>().GetWorker(i).AddTask([&m, &cv, &counter]() {
	std::lock_guard<std::mutex> l(m);
	counter++;
	cv.notify_one();
      });
  }
  {
    std::unique_lock<std::mutex> l(m);
    while (counter < Worker::kNrThreads)
      cv.wait(l);
    counter = 0;
  }
  p.Show("ReExec takes");
}

uint64_t Epoch::CurrentEpochNumber()
{
  return gGlobalEpoch;
}

#define VALIDATE_TXN_KEY 1

void Txn::Initialize(go::InputSocketChannel *channel, uint16_t key_pkt_len)
{
  int cur = 0;
  uint32_t orig_key_crc = 0, orig_val_crc = 0;
  while (cur < key_pkt_len - 8) {
    uint16_t fid;
    uint8_t len;
    channel->Read(&fid, sizeof(uint16_t));
    channel->Read(&len, sizeof(uint8_t));
    assert(len > 0);
    // logger->debug("  key len {0:d} in table {0:d}", len, fid);
    TxnKey *k = (TxnKey *) malloc(sizeof(TxnKey) + len);
    k->fid = fid;
    k->str.len = len;
    uint8_t *ptr = (uint8_t *) k + sizeof(TxnKey);
    k->str.data = ptr;
    channel->Read(ptr, len);
    // logger->debug("  key data {}", (const char *) k->data);
    cur += sizeof(uint16_t) + sizeof(uint8_t) + k->str.len;
#ifdef VALIDATE_TXN_KEY
    update_crc32(k->str.data, k->str.len, &key_crc);
#endif

    keys.push_back(k);
  }

  channel->Read(&orig_key_crc, sizeof(uint32_t));
  channel->Read(&orig_val_crc, sizeof(uint32_t));
  cur += 8; // including the checksums

  assert(cur == key_pkt_len);
#ifdef VALIDATE_TXN_KEY
  assert(orig_key_crc == key_crc);
#endif

  value_crc = orig_val_crc;

  logger->debug("key csum {:x} matches", key_crc);
}

void Txn::SetupReExec()
{
  auto &mgr = Instance<RelationManager>();
  for (auto kptr : keys) {
    auto &relation = mgr.GetRelationOrCreate(kptr->fid);
    relation.SetupReExec(&kptr->str, sid);
  }
}

BaseRequest *BaseRequest::CreateRequestFromChannel(go::InputSocketChannel *channel)
{
  uint8_t type = 0;
  channel->Read(&type, 1);
  assert(type != 0);
  assert(type <= GetGlobalFactoryMap().rbegin()->first);
  logger->debug("txn req type {0:d}", type);
  auto req = GetGlobalFactoryMap().at(type)();
  req->type = type;
  uint16_t key_pkt_size;
  req->ParseFromChannel(channel);
  channel->Read(&key_pkt_size, sizeof(uint16_t));
  logger->debug("receiving keys, total len {}", key_pkt_size);
  req->Initialize(channel, key_pkt_size);

  return req;
}

std::map<std::string, void *> BaseRequest::support_handles;

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
