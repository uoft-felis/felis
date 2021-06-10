#include "commit_buffer.h"
#include "epoch.h"
#include "xxHash/xxhash.h"
#include "vhandle.h"
#include "index_info.h"

namespace felis {

constexpr auto kPerTxnHashSize = 16;

CommitBuffer::CommitBuffer()
{
  ref_hashtable_size = EpochClient::g_txn_per_epoch * kPerTxnHashSize;
  ref_hashtable = (std::atomic<Entry *> *) mem::AllocMemory(
      mem::MemAllocType::GenericMemory, ref_hashtable_size * sizeof(Entry *));
  dup_hashtable_size = EpochClient::g_txn_per_epoch;
  dup_hashtable = (std::atomic<Entry *> *) mem::AllocMemory(
      mem::MemAllocType::GenericMemory, dup_hashtable_size * sizeof(Entry *));
  clear_refcnt = NodeConfiguration::g_nr_threads;

  for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
    auto lmt = EpochClient::g_txn_per_epoch * 6_K / NodeConfiguration::g_nr_threads;
    int numa_node = i / mem::kNrCorePerNode;
    entbrks[i] = mem::Brk::New(
        mem::AllocMemory(mem::MemAllocType::GenericMemory, lmt, numa_node), lmt);
    entbrks[i]->set_thread_safe(false);
  }
}

void CommitBuffer::Clear(int core_id)
{
  if (clear_refcnt.load() == 0)
    return;
  auto tot_threads = NodeConfiguration::g_nr_threads;
  long start, end;
  start = ref_hashtable_size * core_id / tot_threads;
  end = ref_hashtable_size * (core_id + 1) / tot_threads;
  memset(ref_hashtable + start, 0, (end - start) * sizeof(Entry *));

  start = dup_hashtable_size * core_id / tot_threads;
  end = dup_hashtable_size * (core_id + 1) / tot_threads;
  memset(dup_hashtable + start, 0, (end - start) * sizeof(Entry *));

  clear_refcnt.fetch_sub(1);
  entbrks[core_id]->Reset();
}

void CommitBuffer::Reset()
{
  clear_refcnt = NodeConfiguration::g_nr_threads;
}

void CommitBuffer::EnsureReady()
{
  while (clear_refcnt.load() != 0)
    _mm_pause();
}

bool CommitBuffer::AddRef(int core_id, IndexInfo *index_info, uint64_t sid)
{
  uint32_t short_sid = sid;
  uint32_t seq = ((1 << 24) - 1) & (((uint32_t) sid >> 8) - 1);
  uintptr_t r = ((uintptr_t) index_info) >> 6;
  uint32_t hash = seq * kPerTxnHashSize;
  hash += XXH32(&r, sizeof(uintptr_t), seq) % (kPerTxnHashSize - 1);
  hash %= ref_hashtable_size;
  EnsureReady();

  auto &brk = entbrks[core_id];
  std::atomic<Entry *> *pp = &ref_hashtable[hash];
  Entry *p = pp->load();

  Entry *tail;
  Entry *new_ent;

again:
  while (p) {
    if (p->short_sid == short_sid && p->index_info == index_info) {
      if ((tail = p->u.dup.load()))
        goto done;
      pp = &p->u.dup;
      break;
    }
    pp = &p->next;
    p = pp->load();
  }

  new_ent = new (brk->ptr() + brk->current_size()) Entry(index_info, seq);
  tail = nullptr;

  if (pp->compare_exchange_strong(tail, new_ent)) {
    if (p != nullptr) {
      // Inserting into the dup_hashtable. We use the sid as the hash. This should
      // work well when there is very few duplicate writes.
      Entry *last = dup_hashtable[seq].load();
      do {
        new_ent->next = last;
      } while (!dup_hashtable[seq % dup_hashtable_size].compare_exchange_strong(last, new_ent));
      // Set the row value to pending
      new_ent->u.value = (VarStr *) kPendingValue;
      // Increment because we are a new dup entry
      new_ent->wcnt.fetch_add(1);
    }

    brk->Alloc(sizeof(Entry));
    return (p != nullptr);
  }

  new_ent->~Entry();

  if (p == nullptr) {
    p = tail;
    goto again;
  }
done:
  tail->wcnt.fetch_add(1);
  return true;
}

CommitBuffer::Entry *CommitBuffer::LookupDuplicate(IndexInfo *index_info, uint64_t sid)
{
  uint32_t short_sid = sid;
  uint32_t seq = ((1 << 24) - 1) & (((uint32_t) sid >> 8) - 1);
  Entry *p = dup_hashtable[seq % dup_hashtable_size].load();

  while (p) {
    if (p->index_info == index_info && p->short_sid == short_sid)
      break;
    p = p->next.load();
  }
  return p;
}

}
