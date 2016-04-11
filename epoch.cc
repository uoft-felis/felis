#include <cassert>
#include <fstream>
#include <dlfcn.h>
#include "log.h"

#include "epoch.h"
#include "net-io.h"
#include "index.h"
#include "util.h"
#include "worker.h"

#include "json11/json11.hpp"

#include "csum.h"

using util::Instance;

namespace dolly {

uint64_t Epoch::kGlobSID = 0ULL;

struct TxnTimeStamp
{
  uint64_t commit_ts;
  int64_t skew_ts;

  uint64_t logical_commit_ts() const {
    return skew_ts == -1 ? commit_ts : skew_ts;
  }

  bool operator<(const TxnTimeStamp &rhs) const {
    if (logical_commit_ts() != rhs.logical_commit_ts()) {
      return logical_commit_ts() < rhs.logical_commit_ts();
    }
    return skew_ts < rhs.skew_ts;
  }
};

Epoch::Epoch(int *fds, ParseBuffer *buffers)
{
  int nr_threads = Worker::kNrThreads;
  PerfLog p;
  for (int i = 0; i < nr_threads; i++) {
    uint64_t tot_size = 0;
    ParseBuffer::FillDataFromFD(fds[i], &tot_size, sizeof(uint64_t));
    uint8_t *ptr = nullptr;
    if (tot_size != 0) {
      tot_size -= sizeof(uint64_t);
      ptr = (uint8_t *) malloc(tot_size);
      ParseBuffer::FillDataFromFD(fds[i], ptr, tot_size);
    }
    new (&buffers[i]) ParseBuffer(ptr, tot_size);
  }

  typedef std::tuple<TxnTimeStamp, int> HeapElement;
  std::vector<HeapElement> heap;
  std::greater<HeapElement> heap_comp;

  for (int i = 0; i < nr_threads; i++) {
    if (buffers[i].is_empty()) continue;

    TxnTimeStamp ts;
    buffers[i].Read(&ts.commit_ts, sizeof(uint64_t));
    buffers[i].Read(&ts.skew_ts, sizeof(int64_t));
    heap.push_back(std::make_tuple(ts, i));
  }

  std::make_heap(heap.begin(), heap.end(), heap_comp);

  while (!heap.empty()) {
    // then let's pop from the heap!
    std::pop_heap(heap.begin(), heap.end(), heap_comp);
    int chn = std::get<1>(heap.back());

    logger->debug("receiving request from channel {}", chn);
    BaseRequest *req = BaseRequest::CreateRequestFromBuffer(++kGlobSID, buffers[chn]);
    txns.push_back(req);

    if (!buffers[chn].is_empty()) {
      TxnTimeStamp ts;
      buffers[chn].Read(&ts.commit_ts, sizeof(uint64_t));
      buffers[chn].Read(&ts.skew_ts, sizeof(int64_t));
      heap.back() = std::make_tuple(ts, chn);
      std::push_heap(heap.begin(), heap.end(), heap_comp);
    } else {
      heap.pop_back();
    }
  }

  logger->info("epoch contains {} txns", txns.size());
  p.Show("Parsing from network takes");

  // phase two: SetupReExec()
  p = PerfLog();
  for (auto t: txns) {
    t->SetupReExec();
  }
  p.Show("SetupReExec takes");

  Instance<RelationManager>().LogStat();

  p = PerfLog();
  // phase three: re-run in parallel
  std::vector<std::future<void>> txn_futures;
  for (auto &t: txns) {
    txn_futures.push_back(
      Instance<WorkerManager>().SelectWorker().AddTask([t]() {
	t->Run();
	}));
  }
  for (auto &f: txn_futures) {
    f.wait();
  }
  p.Show("ReExec takes");
}

void Txn::Initialize(uint64_t id, ParseBuffer &buffer, uint16_t key_pkt_len)
{
  sid = id;
  int cur = 0;
  uint32_t orig_key_crc = 0, orig_val_crc = 0;
  while (cur < key_pkt_len - 8) {
    uint16_t fid;
    uint8_t len;
    buffer.Read(&fid, sizeof(uint16_t));
    buffer.Read(&len, sizeof(uint8_t));
    assert(len > 0);
    // logger->debug("  key len {0:d} in table {0:d}", len, fid);
    TxnKey *k = (TxnKey *) malloc(sizeof(TxnKey) + len);
    k->fid = fid;
    k->str.len = len;
    buffer.Read(k->str.data, k->str.len);
    // logger->debug("  key data {}", (const char *) k->data);
    cur += sizeof(uint16_t) + sizeof(uint8_t) + k->str.len;
    update_crc32(k->str.data, k->str.len, &key_crc);

    keys.push_back(k);
  }

  buffer.Read(&orig_key_crc, sizeof(uint32_t));
  buffer.Read(&orig_val_crc, sizeof(uint32_t));
  cur += 8; // including the checksums

  assert(cur == key_pkt_len);
  assert(orig_key_crc == key_crc);

  value_crc = orig_val_crc;

  logger->debug("key csum {:x} matches", key_crc);
}

void Txn::SetupReExec()
{
  auto &mgr = Instance<RelationManager>();
  for (auto kptr : keys) {
    auto &relation = mgr.GetRelationOrCreate(kptr->fid);
    // although we don't delete kptr->str, but since we need to move this data
    // into the database, we need IndexKey, rather than ConstIndexKey
    IndexKey index_key(&kptr->str);
    relation.SetupReExec(std::move(index_key), sid);
    index_key.k = nullptr;
  }
  mgr.LogStat();
  mgr.ClearStat();
}

BaseRequest *BaseRequest::CreateRequestFromBuffer(uint64_t sid, ParseBuffer &buffer)
{
  uint8_t type = 0;
  buffer.Read(&type, 1);
  assert(type != 0);
  assert(type <= GetGlobalFactoryMap().rbegin()->first);
  logger->debug("txn req type {0:d}", type);
  auto req = GetGlobalFactoryMap().at(type)();
  req->type = type;
  uint16_t key_pkt_size;
  req->ParseFromBuffer(buffer);
  buffer.Read(&key_pkt_size, sizeof(uint16_t));
  logger->debug("receiving keys, total len {}", key_pkt_size);
  req->Initialize(sid, buffer, key_pkt_size);

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
