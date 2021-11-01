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


IndexInfo *HashEntry::value() const
{
  return (IndexInfo *) ((uint8_t *) this - 40);
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
  // shirley todo: hashtable index?
  // shirley: hash table index allocates an array of elements (each element is a vhandle? 4 cache lines?)
  // then the first row that hashes to an element will use this element in the array (in DRAM)
  // as its vhandle. When future rows hash to the same element (conflict), we will allocate
  // "external" elements through the NewRow which actually calls vhandle::new_inline (or new)
  // and will allocate from PMEM.
  // the index itself is inlined in the vhandle, it's 32 bytes starting from byte 96.
  // shirley: modify it so hash index is inlined to index_info starting from byte 40.
  table = (uint8_t *)
          mmap(nullptr, nrpg << 12, PROT_READ | PROT_WRITE,
               MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  printf("addr %p %p\n", table, table + (nrpg << 12));
}

static constexpr size_t kOffset = 40;

IndexInfo *HashtableIndex::SearchOrCreate(const VarStrView &k, bool *created)
{
  auto idx = hash(k) % nr_buckets;
  HashEntry *first = (HashEntry *) (table + idx * row_size() + kOffset);

  // if element is uninitialized, use the inlined space immediately 
  // this doesn't allocate from pmem because
  if (first->next == kNextForUninitialized) {
    HashEntry *old = kNextForUninitialized;
    if (first->next.compare_exchange_strong(old, kNextForInitializing)) {
      first->key = HashEntry::Convert(k);
      auto row = first->value();

      new (row) IndexInfo();

      // new (row) SortedArrayVHandle();
      // row->capacity = 1; //shirley: removed this bc we already set it in constructor
      // shirley: need to initialize our fields here as well, because here just
      // calls vhandle constructor without initializing required fields as in
      // vhandle NewInline. shirley: we use inline for all tables!
      //row->inline_used = 0;
      
      first->next = kNextForEnd;
      *created = true;
      return row;
    }
  }
  while (first->next == kNextForInitializing) _mm_pause();

  HashEntry *p = first, *newentry = nullptr;
  std::atomic<HashEntry *> *parent = nullptr;
  auto x = HashEntry::Convert(k);
  IndexInfo *row = nullptr;

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
      //shirley: don't let it set capacity to 1, we set it in vhandle constructor already
      // row->capacity = 1;
      newentry = (HashEntry *) ((uint8_t *) row + 40);
      newentry->key = x;
      newentry->next = kNextForEnd;
    }

  } while (!parent->compare_exchange_strong(p, newentry));
  *created = true;
  return row;
}

IndexInfo *HashtableIndex::SearchOrCreate(const VarStrView &k)
{
  bool unused = false;
  return SearchOrCreate(k, &unused);
}

IndexInfo *HashtableIndex::Search(const VarStrView &k)
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

uint32_t DefaultHash(const VarStrView &k)
{
  return XXH32(k.data(), k.length(), 0xdeadbeef);
}

}
