// -*- C++ -*-
#ifndef INDEX_COMMON_H
#define INDEX_COMMON_H

#include <cstdlib>
#include <type_traits>
#include <memory>
#include <mutex>
#include <atomic>

#include "mem.h"
#include "log.h"
#include "util/objects.h"
#include "util/linklist.h"

#include "vhandle.h"
#include "index_info.h"
#include "node_config.h"
#include "shipping.h"

namespace felis {

using util::ListNode;

class Checkpoint {
  static std::map<std::string, Checkpoint *> impl;
 public:
  static void RegisterCheckpointFormat(std::string fmt, Checkpoint *pimpl) { impl[fmt] = pimpl; }
  static Checkpoint *checkpoint_impl(std::string fmt) { return impl[fmt]; }
  virtual void Export() = 0;
};

class Table {
 public:
  static constexpr size_t kAutoIncrementZones = 2048;

 protected:
  int id;
  bool read_only;
  size_t key_len;
  std::atomic_uint64_t *auto_increment_cnt;
  bool enable_inline;
 public:
  Table() : id(-1), read_only(false) {
    auto_increment_cnt = new std::atomic_uint64_t[kAutoIncrementZones];
  }

  void set_id(int relation_id) { id = relation_id; }
  int relation_id() { return id; }

  void set_key_length(size_t l) { key_len = l; }
  size_t key_length() const { return key_len; }

  void set_read_only(bool v) { read_only = v; }
  bool is_read_only() const { return read_only; }

  bool is_enable_inline() const { return enable_inline; }

  // In a distributed environment, we may need to generate a AutoIncrement key
  // on one node and insert on another. In order to prevent conflict, we need to
  // attach our node id at th end of the key.
  uint64_t AutoIncrement(int zone = 0) {
    auto &conf = util::Instance<NodeConfiguration>();
    auto ts = auto_increment_cnt[zone].fetch_add(1);
    return (ts << 8) | (conf.node_id() & 0x00FF);
  }

  uint64_t GetCurrentAutoIncrement(int zone = 0) {
    auto &conf = util::Instance<NodeConfiguration>();
    auto ts = auto_increment_cnt[zone].load();
    return (ts << 8) | (conf.node_id() & 0x00FF);
  }

  void ResetAutoIncrement(int zone = 0, uint64_t ts = 0) {
    abort_if(zone >= kAutoIncrementZones, "zone {} overflows", zone);
    auto_increment_cnt[zone] = ts;
  }

  void PersistAutoIncrement(uint64_t *pmemaddr) {
    for (unsigned int i = 0; i < 171; i++) {
      pmemaddr[i] = auto_increment_cnt[i].load();
    }
  }

  void RecoverAutoIncrement(uint64_t *pmemaddr) {
    for (unsigned int i = 0; i < 171; i++) {
      auto_increment_cnt[i].store(pmemaddr[i]);
    }
  }

  // typedef struct IndexInfo {
  //   VHandle *vhandle;
  //   uint64_t *versions;
  //   util::MCSSpinLock lock;
  // } IndexInfo;

  class Iterator {
    friend class Table;
   protected:
    VarStrView end_key;
    VarStrView cur_key;
    // VHandle *vhandle;

    IndexInfo *index_info;
    // shirley note: when returning idx_info, return the pointer to it, not just a copy
    // shirley note: or else the threads are not actually seeing anyone's changes to lock/versions
    // shirley note: do we want to just store *idx_info here (but more mem usage)? Or do we want to
    // shirley note: store the idx_info but return the address of it (is it harder to implement)?
    // IndexInfo *idx_info;

  public:
    virtual void Next() = 0;
    virtual bool IsValid() const = 0;

    const VarStrView &key() const { return cur_key; }
    // const VHandle *row() const { return vhandle; }
    const IndexInfo *row() const { return index_info; }
    // VHandle *row() { return vhandle; }
    IndexInfo *row() { return index_info; }
  };
 protected:
  void set_iterator_end_key(Iterator *it, const VarStrView &end) {
    it->end_key = end;
  }
 public:

  // IndexBackend will implement these
  virtual IndexInfo *SearchOrCreate(const VarStrView &k, bool *created) { return nullptr; }
  virtual IndexInfo *SearchOrCreate(const VarStrView &k) { return nullptr; }
  virtual IndexInfo *Search(const VarStrView &k) { return nullptr; }
  virtual IndexInfo *RecoverySearchOrCreate(const VarStrView &k, void *vhandle) { return nullptr; }
  virtual Table::Iterator *IndexSearchIterator(const VarStrView &start) {
    return nullptr;
  }
  virtual Table::Iterator *IndexSearchIterator(const VarStrView &start, const VarStrView &end) {
    return nullptr;
  }
  virtual Table::Iterator *IndexReverseIterator(const VarStrView &start) {
    return nullptr;
  }
  virtual Table::Iterator *IndexReverseIterator(const VarStrView &start, const VarStrView &end) {
    return nullptr;
  }
  virtual std::vector<IndexInfo *> SearchRange(const VarStrView &start, const VarStrView &end) {
    return std::vector<IndexInfo *>();
  }

  virtual void IndexMerge() {
    return;
  }

  virtual void IndexLog() {
    return;
  }

  IndexInfo *NewRow(void *vhandle = nullptr);
  size_t row_size() const {
    return IndexInfo::kIndexInfoSize;
    // if (is_enable_inline()) return VHandle::kInlinedSize;
    // else return VHandle::kSize;
  }
};

class TableManager {
  template <typename T> friend T &util::Instance() noexcept;
  TableManager() {}
 public:
  static constexpr int kMaxNrRelations = 1024;

  template <typename TableSpec>
  typename TableSpec::IndexBackend &Get() {
    static_assert(std::is_base_of<Table, typename TableSpec::IndexBackend>::value);

    return *static_cast<typename TableSpec::IndexBackend *>(
        tables[static_cast<int>(TableSpec::kTable)]);
  }

  Table *GetTable(int idx) {
    return tables[idx];
  }

  template <typename TableSpec, typename ...TableSpecs>
  void Create() {
    static_assert(std::is_base_of<Table, typename TableSpec::IndexBackend>::value);

    auto table = new typename TableSpec::IndexBackend(TableSpec::kIndexArgs);
    table->set_id(static_cast<int>(TableSpec::kTable));

    tables[static_cast<int>(TableSpec::kTable)] = table;

    if constexpr(sizeof...(TableSpecs) > 0) Create<TableSpecs...>();
  }

 protected:
  std::array<Table *, kMaxNrRelations> tables;
};

void InitVersion(felis::IndexInfo *, int key_0, int key_1, int key_2, int key_3, int table_id, VarStr *);

}

#endif /* INDEX_COMMON_H */
