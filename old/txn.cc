#include "txn.h"
#include "epoch.h"
#include "index.h"
#include "xxHash/xxhash.h"

#include "dolly_probes.h"

using util::Instance;

namespace dolly {

static const int64_t kEpsilonMax = 32768;

void TxnIOReader::Run()
{
  /* Dealing with EOF and end of epoch is easy
   * End of epoch: add another TxnIOReader, and return.
   * EOF: just return
   */
  auto *channel = sock->input_channel();

  uint64_t commit_ts;
  int64_t skew_ts;

  bool eof = false;
  int count_max = 0;
  uint64_t last_sid = 0;

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
    logger->debug("{} read ts {:x} {:x}", go::Scheduler::CurrentThreadPoolId(), commit_ts, skew_ts);

    auto req = BaseRequest::CreateRequestFromChannel(channel, epoch);

    if (req == nullptr) {
      logger->critical("WTF? {:x} {:x}", commit_ts, skew_ts);
      std::abort();
    }

    count_max++;
    // uint64_t sid = skew_ts == 0 ? commit_ts << 6 : (skew_ts << 6) - go::Scheduler::CurrentThreadPoolId();
#ifdef PROTO_OLD_SSI
    uint64_t sid = skew_ts == 0 ? commit_ts * 2 : (skew_ts * 2) - 1;
#else
    uint64_t sid = commit_ts * kEpsilonMax - skew_ts;
    if (skew_ts > 0) {
      DTRACE_PROBE1(dolly, commit_back_in_time, skew_ts);
    }
#endif

#if 0
    // for debugging
    if (req->key_buffer_size() == 0) {
      // but it doesn't seem to bother us?
      logger->alert("type {}, sid {}", req->type, sid);
    }
#endif
    req->set_serializable_id(sid);
    req->set_wait_barrier(barrier);
    req->set_counter(&fcnt);
    req->set_epoch(epoch);
    req->set_reuse_queue(reuse_q);

    req->PushToReuseQueue();
    last_sid = sid;
  }
  fcnt.max = count_max; // should never be 0 here
  logger->info("finished, socket ptr {} on {} cnt {} ioreader {}", (void *) sock,
	       go::Scheduler::CurrentThreadPoolId(), count_max, (void *) this);
  uint8_t ch = 0;
  if (eof)
    ch = 1;
  epoch->channel()->Write(&ch, 1);
}

void TxnRunner::Run()
{
  auto ent = queue->next;
  int tot = 0;
  bool done = false;
  while (ent != queue) {
    auto next = ent->next;

    ent->t->Reset();
    // ent->t->StartOn(go::Scheduler::CurrentThreadPoolId());
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
  if (collect_garbage) {
    Instance<DeletedGarbageHeads>().CollectGarbage(Epoch::CurrentEpochNumber());
  }
}

CommitBufferEntry::CommitBufferEntry(int id, const VarStr &k, VarStr *o)
    : epoch_nr(Epoch::CurrentEpochNumber()), fid(id), key(k), obj(o), handle(nullptr)
{}

CommitBufferEntry *CommitBufferEntry::Clone()
{
  auto *e = new CommitBufferEntry(fid, key, obj);
  e->epoch_nr = epoch_nr;
  e->handle = handle;
  return e;
}

void CommitBuffer::AppendPreWrite(int fid, const VarStr &key, VHandle *handle)
{
  writes[cur_write] = CommitBufferEntry(fid, key, nullptr);
  writes[cur_write].handle = handle;
  unsigned int h = Hash(fid, &key);
  writes[cur_write].ht_node.InsertAfter(&htable[h % kHashTableSize]);
  cur_write++;
}

unsigned long CommitBuffer::Hash(int fid, const VarStr *key)
{
  return XXH64(key->data, key->len, (unsigned long) fid);
}

CommitBufferEntry *CommitBuffer::GetEntry(int fid, const VarStr *k)
{
  __builtin_prefetch(htable);
  unsigned int h = Hash(fid, k);
  ListNode *head = &htable[h % kHashTableSize];
  ListNode *node = head->next;
  while (node != head) {
    auto entry = container_of(node, CommitBufferEntry, ht_node);
    if (entry->fid != fid)
      goto next;
    if (entry->key != *k)
      goto next;

    return entry;
 next:
    node = node->next;
  }
  return nullptr;
}

VarStr *CommitBuffer::Get(CommitBufferEntry *entry)
{
  if (!entry) return nullptr;
  unsigned int pos = entry - writes;
  if (pos < cur_write)
    return nullptr;
  return entry->obj;
}

void CommitBuffer::Put(int fid, const VarStr *key, VarStr *obj)
{
  auto *w = &writes[cur_write];
  if (w->fid == fid && w->key == *key) {
    cur_write++;
    goto done;
  }

  w = GetEntry(fid, key);
  if (!w) {
    logger->critical("missing writes!");
    std::abort();
  }

done:
  w->obj = obj;
}

void CommitBuffer::Commit(uint64_t sid, TxnValidator *validator)
{
  if (validator)
    validator->set_keys_ptr(tx->key_buffer());

  for (int i = 0; i < nr_writes; i++) {
    writes[i].handle->Prefetch();
  }

  for (int i = 0; i < nr_writes; i++) {
    auto *entry = &writes[i];
    bool is_garbage;

    if (validator)
      validator->CaptureWrite(*tx, entry->fid, &entry->key, entry->obj);

    entry->handle->WriteWithVersion(sid, entry->obj, is_garbage);

    if (!is_garbage) {
    } else {
      Instance<RelationManager>().GetRelationOrCreate(entry->fid).FakeDelete(&entry->key);
      Instance<DeletedGarbageHeads>().AttachGarbage(entry->Clone());
    }
  }

  if (validator)
    validator->Validate(*tx);

  delete writes;
}

mem::Pool *Txn::pools;

void Txn::InitPools()
{
  pools = (mem::Pool *) malloc(Epoch::kNrThreads * sizeof(mem::Pool));
  for (int i = 0; i < Epoch::kNrThreads; i++) {
    new (&pools[i]) mem::Pool(kTxnBrkSize, kPoolCap / kTxnBrkSize, i / mem::kNrCorePerNode);
  }
}

void Txn::PushToReuseQueue()
{
  node.t = this;
  auto *p = reuse_q->prev;
  for (int i = 0; p != reuse_q; p = p->prev, i++) {
    if (p->t->sid < sid)
      break;
  }
  node.Add(p);
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
      logger->info("thread {} done, waiting for barrier", go::Scheduler::CurrentThreadPoolId());
      barrier->Wait();
      auto runner = new TxnRunner(reuse_q);
      runner->set_collect_garbage(true);
      go::Scheduler::Current()->WakeUp(runner);
      // runner->StartOn(go::Scheduler::CurrentThreadPoolId());
      logger->info("{} has total {} txns", go::Scheduler::CurrentThreadPoolId(), fcnt->max);
    }
  } else {
    int cpu = go::Scheduler::CurrentThreadPoolId() - 1;
    void *p = pools[cpu].Alloc();
    {
      mem::Brk b(p, kTxnBrkSize);
      go::Scheduler::Current()->current_routine()->set_userdata(&b);

      RunTxn();
      go::Scheduler::Current()->current_routine()->set_userdata(nullptr);
    }
    pools[cpu].Free(p);

    if (++fcnt->count == fcnt->max) {
      // notify the driver thread, which only used to hold and free stuff...
      logger->info("all txn replayed done on thread {}", go::Scheduler::CurrentThreadPoolId());
      uint8_t ch = 0;
      epoch->channel()->Write(&ch, 1);
    }
  }
}


#ifdef CALVIN_REPLAY

void Txn::InitializeReadSet(go::TcpInputChannel *channel, uint32_t read_key_pkt_len, Epoch *epoch)
{
  int cpu = go::Scheduler::CurrentThreadPoolId() - 1;
  read_set_keys = (uint8_t *) epoch->AllocFromBrk(cpu, read_key_pkt_len);
  sz_read_set_key_buf = read_key_pkt_len;
  channel->Read(read_set_keys, read_key_pkt_len);
}

#endif

void Txn::Initialize(go::TcpInputChannel *channel, uint16_t key_pkt_len, Epoch *epoch)
{
  int cpu = go::Scheduler::CurrentThreadPoolId() - 1;
  uint8_t *buffer = (uint8_t *) epoch->AllocFromBrk(cpu, key_pkt_len);
  memset(buffer, 0xff, key_pkt_len);

  channel->Read(buffer, key_pkt_len);

  keys = buffer;
  sz_key_buf = key_pkt_len;

#ifdef VALIDATE_TXN_KEY
  sz_key_buf -= 4;
  uint32_t orig_key_crc;

  memcpy(&orig_key_crc, buffer + sz_key_buf, 4);
  // logger->info("orig key crc {} buffer {} sz_key_buf {}", orig_key_crc, (void *) buffer, sz_key_buf);

  uint8_t *p = buffer;
  while (p < buffer + sz_key_buf) {
    TxnKey *k = (TxnKey *) p;
    update_crc32(k->data, k->len, &key_crc);
    p += sizeof(TxnKey) + k->len + 4; // to skip the csum as well
  }
  if (orig_key_crc != key_crc) {
    logger->critical("Key crc doesn't match! {} should be {}", key_crc, orig_key_crc);
    std::abort();
  }
  logger->debug("key csum {:x} matches", key_crc);
#endif
}

void Txn::GenericSetupReExec(uint8_t *key_buffer, size_t key_len, bool is_read)
{
  auto &mgr = Instance<RelationManager>();
  uint8_t *p = key_buffer;
  size_t nr_tot = 0;

  while (p < key_buffer + key_len) {
    TxnKey *kptr = (TxnKey *) p;
    nr_tot++;

    p += sizeof(TxnKey) + kptr->len;
#ifdef VALIDATE_TXN
    p += 4; // skip the csum as well
#endif
  }

  VHandle **handles = nullptr;
  if (!is_read) {
    commit_buffer.InitPreWrites(nr_tot);
    handles = (VHandle **) alloca(nr_tot * sizeof(VHandle *));
  }

  p = key_buffer;
  int i = 0;
  while (p < key_buffer + key_len) {
    TxnKey *kptr = (TxnKey *) p;
    Relation *relation;
    VarStr var_str;
    var_str.len = kptr->len;
    var_str.data = kptr->data;
    relation = &mgr.GetRelationOrCreate(kptr->fid);
    auto *handle = relation->InsertOrCreate(&var_str);
    if (!is_read) {
      // non-calvin
      handles[i++] = handle;
      commit_buffer.AppendPreWrite(kptr->fid, var_str, handle);
    } else {
      // calvin's read-set
#ifdef CALVIN_REPLAY
      while (!handle->AppendNewAccess(serializable_id(), true))
        __builtin_ia32_pause();
#endif
    }

    p += sizeof(TxnKey) + kptr->len;
#ifdef VALIDATE_TXN
    p += 4; // skip the csum as well
#endif
  }

  // calvin's read-set
  if (is_read)
    return;

  commit_buffer.DonePreWrite();

  for (int i = 0; i < nr_tot; i++) {
    __builtin_prefetch(handles[i]);
  }

  size_t nr_left = nr_tot;
  while (nr_left > 0) {
    for (int i = 0; i < nr_tot; i++) {
      auto handle = handles[i];
      if (!handle) continue;
      if (!is_read && handle->AppendNewVersion(serializable_id())) {
        handles[i] = nullptr;
        nr_left--;
      }
    }
  }
}

void Txn::SetupReExec()
{
  logger->debug("setup sid {}", serializable_id());
#ifdef CALVIN_REPLAY
  GenericSetupReExec(read_set_keys, sz_read_set_key_buf, true);
#endif
  GenericSetupReExec(keys, sz_key_buf);
}


}
