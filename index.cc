#include <fstream>
#include <streambuf>
#include "index.h"
#include "epoch.h"
#include "util.h"
#include "json11/json11.hpp"

using util::Instance;

// export global variables
namespace util {

static dolly::RelationManager mgr;

template <>
dolly::RelationManager &Instance()
{
  return mgr;
}

}

namespace dolly {

std::atomic<unsigned long> TxnValidator::tot_validated;

bool BaseIndexKey::operator<(const BaseIndexKey &rhs) const
{
  if (rhs.k == nullptr) return false;
  if (k == nullptr) return true;
  int r = memcmp(k->data, rhs.k->data, std::min(k->len, rhs.k->len));
  return r < 0 || (r == 0 && k->len < rhs.k->len);
}

bool BaseIndexKey::operator==(const BaseIndexKey &rhs) const
{
  if (rhs.k == nullptr && k == nullptr) return true;
  else if (rhs.k == nullptr || k == nullptr) return false;

  return k->len == rhs.k->len && memcmp(k->data, rhs.k->data, k->len) == 0;
}

void BaseRelation::LogStat() const
{
  logger->debug("NewKeyCnt: {}", stat.new_key_cnt);
  logger->debug("KeyCnt: {}", stat.key_cnt);
}

RelationManagerBase::RelationManagerBase()
{
  std::string err;
  std::ifstream fin("relation_map.json");

  // Wow...I thought I will never encounter most-vexing parse for the rest of my
  // life....and today, I encountered two of them on Clang!
  // - Mike
  std::string conf_text {
    std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>() };

  json11::Json conf_doc = json11::Json::parse(conf_text, err);
  if (!err.empty()) {
    logger->critical(err);
    logger->critical("Cannot read relation id map configuration!");
    std::abort();
  }

  auto json_map = conf_doc.object_items();
  for (auto it = json_map.begin(); it != json_map.end(); ++it) {
    // assume it->second is int!
    // TODO: validations!
    relation_id_map[it->first] = it->second.int_value();
    logger->info("relation name {} map to id {}", it->first,
		 relation_id_map[it->first]);
  }
}

void TxnValidator::CaptureWrite(const dolly::BaseIndexKey &k, VarStr *obj)
{
  update_crc32(k.k->data, k.k->len, &key_crc);

  if (obj != nullptr) {
    // size_t dummy_size = obj->len;
    // update_crc32(&dummy_size, sizeof(size_t), &value_crc);
    update_crc32(obj->data, obj->len, &value_crc);
    data.push_back(std::vector<uint8_t>{obj->data, obj->data + obj->len});
    value_size += obj->len;
  }
}

void TxnValidator::Validate(const Txn &tx)
{
  assert(tx.key_checksum() == key_crc);
  if (tx.value_checksum() != value_crc) {
    logger->alert("value csum mismatch, type {:d}", tx.type);
    logger->alert("tx csum 0x{:x}, result csum 0x{:x}. Dumping:",
		  tx.value_checksum(), value_crc);
    for (auto line_data: data) {
      std::string str;
      for (auto ch: line_data) {
	char buf[1024];
	snprintf(buf, 1024, "0x%.2x ", ch);
	str.append(buf);
      }
      logger->alert(str);
    }
    std::abort();
  }
  logger->info("Valid! Total {} txns data size {} bytes",
	       tot_validated.fetch_add(1), value_size);
}

void CommitBuffer::Put(int fid, dolly::IndexKey &&key, VarStr *obj)
{
  std::tuple<int, BaseIndexKey> tup(fid, std::move(key));

  auto it = buf.find(tup);
  if (it != buf.end()) {
    it->second.first++;
    it->second.second = obj;
    write_seq.push_back(it);
  } else {
    auto result = buf.emplace(std::move(tup), std::move(std::make_pair(1, obj)));
    write_seq.push_back(result.first);
  }
}

VarStr *CommitBuffer::Get(int fid, const ConstIndexKey &k)
{
  auto tup = std::tuple<int, BaseIndexKey>(fid, k.k);
  auto it = buf.find(tup);
  if (it == buf.end()) {
    return nullptr;
  }
  return it->second.second;
}

void CommitBuffer::Commit(uint64_t sid, TxnValidator *validator)
{
  for (auto &it: write_seq) {
    int refcnt = --it->second.first;
    if (refcnt > 0) continue;
    assert(refcnt == 0);

    auto &tup = it->first;
    const BaseIndexKey &base_k = std::get<1>(tup);
    int fid = std::get<0>(tup);

    IndexKey ikey(base_k.k);
    VarStr *obj = it->second.second;

    if (validator != nullptr)
      validator->CaptureWrite(ikey, obj);
    Instance<RelationManager>().GetRelationOrCreate(fid)
      .CommitPut(std::move(ikey), sid, obj);
  }

  if (validator)
    validator->Validate(*tx);
}

}
