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

  class Iterator {
    friend class Table;
   protected:
    VarStrView end_key;
    VarStrView cur_key;
    VHandle *vhandle;

   public:
    virtual void Next() = 0;
    virtual bool IsValid() const = 0;

    const VarStrView &key() const { return cur_key; }
    const VHandle *row() const { return vhandle; }
    VHandle *row() { return vhandle; }
  };
 protected:
  void set_iterator_end_key(Iterator *it, const VarStrView &end) {
    it->end_key = end;
  }
 public:

  // IndexBackend will implement these
  virtual VHandle *SearchOrCreate(const VarStrView &k, bool *created) { return nullptr; }
  virtual VHandle *SearchOrCreate(const VarStrView &k) { return nullptr; }
  virtual VHandle *Search(const VarStrView &k) { return nullptr; }
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

  VHandle *NewRow();
  size_t row_size() const {
    if (is_enable_inline()) return VHandle::kInlinedSize;
    else return VHandle::kSize;
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

void InitVersion(felis::VHandle *, VarStr *);

}

#endif /* INDEX_COMMON_H */
