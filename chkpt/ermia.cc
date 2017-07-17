#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "ermia_size_encode.h"

#ifndef STANDALONE
#include "index.h"
#endif

namespace dolly {
namespace chkpt {
namespace ermia {

struct ErmiaTableDesc {
  std::string name;
  uint32_t fid;
  uint32_t max_oid;
};

struct ErmiaHeader {
  uint32_t nr_tables;
  ErmiaTableDesc *tables;
};

struct ErmiaObjectHeader {
  uint64_t pdest, next, clsn, epoch;
};

struct ErmiaTupleHeader {
  uint64_t bitmap[256 / 64]; // 4 words
  uint64_t sstamp, xstamp, preader, s2;
  uint32_t size;
  uint32_t __pad__;
  uint64_t pvalue_garbage;
};

struct ErmiaEntry {
  static const size_t kMaxKeyLength = 32767;
  static const size_t kMaxDataLength = 32767;

  uint32_t max_oid; // for iterator
  uint32_t oid;
  ErmiaObjectHeader object_hdr;
  ErmiaTupleHeader tuple_hdr;
  uint32_t key_size;
  unsigned char *key_data;
  uint64_t value_storage_size;
  unsigned char *value_data;

  bool operator==(const ErmiaEntry &rhs) {
    return oid == rhs.oid;
  }
};

template <template<typename> class Iterator>
struct ErmiaTableData {
  uint32_t max_oid;
  uint32_t fid;

  Iterator<ErmiaEntry> entries;
public:
  Iterator<ErmiaEntry> begin() const {
    return std::move(entries);
  }
  Iterator<ErmiaEntry> end() const {
    ErmiaEntry end_entry;
    end_entry.max_oid = max_oid;
    end_entry.oid = max_oid;
    return Iterator<ErmiaEntry>(end_entry);
  }

  void set_max_oid(uint32_t maxoid) {
    max_oid = maxoid;
    entries->max_oid = maxoid;
  }
};

template <typename T>
class ReadIterator {
  FILE *fp;
  T rs;
public:
  ReadIterator() {}
  ReadIterator(const T &o) : rs(o) {}

  void set_file_handle(FILE *f) { fp = f; }

  ReadIterator<T> &operator++() {
    ReadStruct(fp, rs);
    return *this;
  }

  T &operator*() {
    return rs;
  }

  T *operator->() {
    return &rs;
  }

  bool operator==(const ReadIterator<T> &rhs) {
    return rs == rhs.rs;
  }

  bool operator!=(const ReadIterator<T> &rhs) {
    return !(rs == rhs.rs);
  }
};

#define ABORT_IF(x) if (x) std::abort()

template <typename T>
void ReadStruct(FILE *fp, T &rs)
{
  ABORT_IF(!fread(&rs, sizeof(T), 1, fp));
}

template <typename T>
void WriteStruct(FILE *fp, const T &o)
{
  fwrite(&o, sizeof(T), 1, fp);
}

template <>
void ReadStruct(FILE *fp, std::string &rs)
{
  size_t s;
  ABORT_IF(!fread(&s, sizeof(size_t), 1, fp));
  char str[s + 1];
  ABORT_IF(!fread(str, s, 1, fp));
  str[s] = 0;
  rs = std::string(str);
}

template <>
void WriteStruct(FILE *fp, const std::string &o)
{
  size_t s = o.length();
  fwrite(&s, sizeof(size_t), 1, fp);
  fwrite(o.c_str(), s, 1, fp);
}

template <>
void ReadStruct(FILE *fp, ErmiaTableDesc &rs)
{
  ReadStruct(fp, rs.name);
  ReadStruct(fp, rs.fid);
  ReadStruct(fp, rs.max_oid);
}

template <>
void WriteStruct(FILE *fp, const ErmiaTableDesc &o)
{
  WriteStruct(fp, o.name);
  WriteStruct(fp, o.fid);
  WriteStruct(fp, o.max_oid);
}

template <>
void ReadStruct(FILE *fp, ErmiaHeader &rs)
{
  ReadStruct(fp, rs.nr_tables);
  rs.tables = new ErmiaTableDesc[rs.nr_tables];
  for (int i = 0; i < rs.nr_tables; i++) {
    ReadStruct(fp, rs.tables[i]);
  }
}

template <>
void WriteStruct(FILE *fp, const ErmiaHeader &o)
{
  WriteStruct(fp, o.nr_tables);
  for (int i = 0; i < o.nr_tables; i++) {
    WriteStruct(fp, o.tables[i]);
  }
}


template <>
void ReadStruct(FILE *fp, ErmiaEntry &rs)
{
  uint8_t vscode;
  ReadStruct(fp, rs.oid);

  if (rs.oid == rs.max_oid) return;

  ReadStruct(fp, rs.key_size);
  free(rs.key_data);
  rs.key_data = (unsigned char *) malloc(rs.key_size);
  ABORT_IF(!fread(rs.key_data, rs.key_size, 1, fp));
  ReadStruct(fp, vscode);
  ReadStruct(fp, rs.object_hdr);
  ReadStruct(fp, rs.tuple_hdr);

  rs.value_storage_size = DecodeSizeAligned(vscode);
  size_t offset = sizeof(ErmiaObjectHeader) + sizeof(ErmiaTupleHeader);

  ABORT_IF(rs.value_storage_size < rs.tuple_hdr.size + offset);
  free(rs.value_data);
  rs.value_data = (unsigned char *) malloc(rs.value_storage_size - offset);
  ABORT_IF(!fread(rs.value_data, rs.value_storage_size - offset, 1, fp));
}

template <>
void WriteStruct(FILE *fp, const ErmiaEntry &rs)
{
  WriteStruct(fp, rs.oid);

  if (rs.oid == rs.max_oid) return;

  WriteStruct(fp, rs.key_size);
  ABORT_IF(!fwrite(rs.key_data, rs.key_size, 1, fp));
  size_t offset = sizeof(ErmiaObjectHeader) + sizeof(ErmiaTupleHeader);
  size_t storage_size = offset + rs.tuple_hdr.size;
  uint8_t vscode = EncodeSizeAligned(storage_size);
  WriteStruct(fp, vscode);
  WriteStruct(fp, rs.object_hdr);
  WriteStruct(fp, rs.tuple_hdr);

  ABORT_IF(!fwrite(rs.value_data, storage_size - offset, 1, fp));
}

template <template<typename> class Iterator>
void ReadStruct(FILE *fp, ErmiaTableData<Iterator> &rs)
{
  uint32_t max_oid;
  ReadStruct(fp, max_oid);
  ReadStruct(fp, rs.fid);
  rs.set_max_oid(max_oid);
  rs.entries.set_file_handle(fp);
  ReadStruct(fp, *rs.entries);
}

template <template<typename> class Iterator>
void WriteStruct(FILE *fp, const ErmiaTableData<Iterator> &rs)
{
  WriteStruct(fp, rs.max_oid);
  WriteStruct(fp, rs.fid);
  for (ErmiaEntry entry: rs) {
    WriteStruct(fp, entry);
  }
  WriteStruct(fp, *rs.end());
}

#ifndef STANDALONE

template <typename T>
class TableDataIterator {

};

template <>
class TableDataIterator<ErmiaEntry> {
  size_t key_size;
  std::shared_ptr<uint8_t> zero_key;
  dolly::VarStr zero_var_str;
  std::shared_ptr<CommitBuffer> dummy;
  std::shared_ptr<dolly::Relation::Iterator> dolly_iter;
  ErmiaEntry current_entry;
  bool adapted;
  uint32_t current_oid;
  FILE *out_file;
public:
  TableDataIterator(dolly::Relation &rel, FILE *fp);
  TableDataIterator(const ErmiaEntry &entry);

  ErmiaEntry &operator*();
  bool operator!=(const TableDataIterator<ErmiaEntry> &rhs) const;
  TableDataIterator<ErmiaEntry> &operator++();
};

class ErmiaCheckpoint : public dolly::Checkpoint {
public:
  void Export() override final;
  void ExportRelation(FILE *fp, Relation &rel);
};

TableDataIterator<ErmiaEntry>::TableDataIterator(const ErmiaEntry &entry)
  : dummy(nullptr), dolly_iter(nullptr), current_entry(entry), adapted(true), out_file(nullptr)
{
}

TableDataIterator<ErmiaEntry>::TableDataIterator(dolly::Relation &rel, FILE *fp)
  : key_size(rel.key_length()),
    zero_key((uint8_t *) calloc(key_size, 1)),
    zero_var_str{(uint16_t) key_size, 0, zero_key.get()},
    dummy(new CommitBuffer(nullptr)),
    dolly_iter(new dolly::Relation::Iterator(rel.SearchIterator(&zero_var_str, std::numeric_limits<int64_t>::max(), *dummy))),
    adapted(false),
    current_oid(0),
    out_file(fp)
{
  memset(&current_entry, 0, sizeof(ErmiaEntry));
  current_entry.max_oid = rel.nr_unique_keys();
  current_entry.object_hdr.clsn = (1 << 16) | 0x1000;
}

bool TableDataIterator<ErmiaEntry>::operator!=(const TableDataIterator<ErmiaEntry> &rhs) const
{
  if (!dolly_iter && !rhs.dolly_iter) return false;
  if (!dolly_iter && !rhs.dolly_iter->IsValid()) return false;
  if (!dolly_iter->IsValid() && !rhs.dolly_iter) return false;
  return true;
}

ErmiaEntry &TableDataIterator<ErmiaEntry>::operator*()
{
  if (!adapted) {
    off_t pos = ftello(out_file);
    current_entry.object_hdr.pdest = (pos << 16) | 0x5000;
    current_entry.oid = current_oid;
    current_entry.key_size = key_size;
    // memcpy(current_entry.key_data, dolly_iter->key().data, key_size);
    current_entry.key_data = (unsigned char *) dolly_iter->key().data;
    current_entry.tuple_hdr.size = dolly_iter->object()->len;
    // memcpy(current_entry.value_data, dolly_iter->object()->data, dolly_iter->object()->len);
    current_entry.value_data = (unsigned char *) dolly_iter->object()->data;
  }
  return current_entry;
}

TableDataIterator<ErmiaEntry> &TableDataIterator<ErmiaEntry>::operator++()
{
  do {
    dolly_iter->Next();
  } while (dolly_iter->IsValid() && dolly_iter->object() == nullptr);
  adapted = false;
  current_oid++;
  return *this;
}

void ErmiaCheckpoint::ExportRelation(FILE *fp, dolly::Relation &rel)
{
  printf("Exporting relations FID %d\n", rel.relation_id());

  TableDataIterator<ErmiaEntry> entries(rel, fp);

  ErmiaTableData<TableDataIterator> table {
    (uint32_t) rel.nr_unique_keys(), (uint32_t) rel.relation_id(), std::move(entries)
  };

  WriteStruct(fp, table);
}

void ErmiaCheckpoint::Export()
{
  auto &mgr = util::Instance<RelationManager>();
  const auto &mapping = mgr.relation_mapping();
  ErmiaHeader hdr;
  hdr.nr_tables = mapping.size();
  hdr.tables = new ErmiaTableDesc[hdr.nr_tables];
  int i = 0;
  for (const auto &pair: mapping) {
    auto& desc = hdr.tables[i++];
    desc.fid = pair.second;
    desc.name = pair.first;
    desc.max_oid = mgr.GetRelationOrCreate(desc.fid).nr_unique_keys();
  }
  puts("Writing ERMIA checkpoint header");

  char *buffer = (char *) malloc(64 << 20);
  FILE *fp = fopen("ermia.chkpt", "w");
  setvbuf(fp, buffer, _IOFBF, 64 << 20);

  WriteStruct(fp, hdr);
  for (i = 0; i < hdr.nr_tables; i++) {
    ExportRelation(fp, mgr.GetRelationOrCreate(hdr.tables[i].fid));
  }
  fclose(fp);
}

#endif

}
}
}

#ifndef STANDALONE

extern "C" dolly::Checkpoint* InitializeChkpt()
{
  return new dolly::chkpt::ermia::ErmiaCheckpoint();
}

#else

using namespace dolly::chkpt::ermia;

int main(int argc, char *argv[])
{
  if (argc != 2) {
    fprintf(stderr, "%s <oac-file>\n", argv[0]);
    std::exit(-1);
  }
  FILE *fp = fopen(argv[1], "r");
  if (!fp) {
    perror("fopen");
    return -1;
  }

  ErmiaHeader hdr;
  ReadStruct(fp, hdr);

  puts("Dumping ERMIA checkpoint file\n");
  puts("=======Header========");
  for (int i = 0; i < hdr.nr_tables; i++) {
    printf("Table: %s %u max_oid %u\n", hdr.tables[i].name.c_str(),
	   hdr.tables[i].fid, hdr.tables[i].max_oid);
  }
  puts("");

  for (int i = 0; i < hdr.nr_tables; i++) {
    printf("\nTable %s...\n", hdr.tables[i].name.c_str());
    ErmiaTableData<ReadIterator> table_data;
    ReadStruct(fp, table_data);
    for (auto entry: table_data) {
      printf("OID: %u clsn %ld\n", entry.oid, entry.object_hdr.clsn);
    }
  }

  fclose(fp);
  return 0;
}

#endif
