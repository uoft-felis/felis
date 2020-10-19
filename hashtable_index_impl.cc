#include <cstdlib>
#include <sys/mman.h>

#include "hashtable_index_impl.h"
#include "xxHash/xxhash.h"

namespace felis {

// Useless?
struct ThreadInfo {
  std::atomic<HashEntry *> free = nullptr; // free list has a list of pre-allocated entries

  HashEntry *AllocEntry();
  void FreeEntry(HashEntry *);
};

static void *AllocFromHugePage(size_t length)
{
  length = util::Align(length, 2 << 20);
  void *p = mmap(nullptr, length, PROT_READ | PROT_WRITE,
                 MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB,
                 -1, 0);
  if (p == MAP_FAILED) return nullptr;
  mlock(p, length);
  return p;
}

HashEntry *ThreadInfo::AllocEntry()
{
  while (true) {
    auto e = free.load();
    while (e) {
      if (free.compare_exchange_strong(e, e->next))
        return e;
    }

    static constexpr auto kAllocSize = 64 << 10;
    e = (HashEntry *) AllocFromHugePage(kAllocSize * sizeof(HashEntry));
    HashEntry *it;
    for (it = e; it < e + kAllocSize - 1; it++) {
      it->next = it + 1;
    }

    HashEntry *tail = nullptr;
    do {
      it->next = tail;
    } while (!free.compare_exchange_strong(tail, e));
  }
}

void ThreadInfo::FreeEntry(HashEntry *e)
{
  auto head = free.load();
  do {
    e->next = head;
  } while (!free.compare_exchange_strong(head, e));
}

static thread_local ThreadInfo *local_ti = nullptr;


VHandle *HashEntry::value() const
{
  return (VHandle *) ((uint8_t *) this - 96);
}

static HashEntry *kNextForUninitialized = (HashEntry *) 0;
static HashEntry *kNextForInitializing = (HashEntry *) 0xdeadbeef00000000;
static HashEntry *kNextForEnd = (HashEntry *) 0xEDEDEDEDEDEDEDED;

HashtableIndex::HashtableIndex(std::tuple<HashFunc, size_t, bool> conf)
    : Table()
{
  hash = std::get<0>(conf);
  nr_buckets = std::get<1>(conf);
  enable_inline = std::get<2>(conf);

  // Instead pre-allocate the table from the beginning, we'll use fine on-demand
  // paging for the bucket. In this way, the insertion CPU will allocate the
  // page from its local NUMA zone. As long as the hash function can generate
  // NUMA friendly hash function, we can make sure all pages are accessed from
  // local NUMA zone.
  auto nrpg = ((nr_buckets * row_size() - 1) >> 12) + 1;
  table = (uint8_t *)
          mmap(nullptr, nrpg << 12, PROT_READ | PROT_WRITE,
               MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  printf("addr %p %p\n", table, table + (nrpg << 12));
}

static constexpr size_t kOffset = 96;

VHandle *HashtableIndex::SearchOrCreate(const VarStr *k, bool *created)
{
  auto idx = hash(k) % nr_buckets;
  HashEntry *first = (HashEntry *) (table + idx * row_size() + kOffset);

  if (first->next == kNextForUninitialized) {
    HashEntry *old = kNextForUninitialized;
    if (first->next.compare_exchange_strong(old, kNextForInitializing)) {
      first->key = HashEntry::Convert(k);
      auto row = first->value();
      new (row) SortedArrayVHandle();
      row->capacity = 1;
      first->next = kNextForEnd;
      *created = true;
      return row;
    }
  }
  while (first->next == kNextForInitializing) _mm_pause();

  HashEntry *p = first, *newentry = nullptr;
  std::atomic<HashEntry *> *parent = nullptr;
  auto x = HashEntry::Convert(k);
  VHandle *row = nullptr;

  do {
    while (p != kNextForEnd) {
      if (p->Compare(x)) {
        if (row) delete row;
        *created = false;
        return p->value();
      }
      parent = &p->next;
      p = parent->load();
    }

    if (newentry == nullptr) {
      row = NewRow();
      row->capacity = 1;
      newentry = (HashEntry *) ((uint8_t *) row + 96);
      newentry->key = x;
      newentry->next = kNextForEnd;
    }

  } while (!parent->compare_exchange_strong(p, newentry));
  *created = true;
  return row;
}

VHandle *HashtableIndex::SearchOrCreate(const VarStr *k)
{
  bool unused = false;
  return SearchOrCreate(k, &unused);
}

VHandle *HashtableIndex::Search(const VarStr *k)
{
  auto idx = hash(k) % nr_buckets;
  auto p = (HashEntry *) (table + idx * row_size() + kOffset);
  auto x = HashEntry::Convert(k);
  unsigned int cnt = 0;

  if (p->next == kNextForUninitialized) return nullptr;

  while (p->next == kNextForInitializing) _mm_pause();

  while (p != kNextForEnd) {
    cnt++;
    if (p->Compare(x)) {
      // if (cnt > 1) printf("table id %d\n", relation_id()); std::abort();
      return p->value();
    }
    p = p->next.load();
  }
  return nullptr;
}

uint32_t DefaultHash(const VarStr *k)
{
  return XXH32(k->data, k->len, 0xdeadbeef);
}

}
