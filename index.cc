#include "index.h"
#include "epoch.h"

namespace db_backup {

bool IndexKey::operator<(const IndexKey &rhs) const
{
  int r = memcmp(k->data, rhs.k->data,
		 std::min(k->len, rhs.k->len));
  return r < 0 || (r == 0 && k->len < rhs.k->len);
}

bool IndexKey::operator==(const IndexKey &rhs) const
{
  return k->len == rhs.k->len
    && memcmp(k->data, rhs.k->data, k->len) == 0;
}

void BaseRelation::LogStat() const
{
  logger->info("NewKeyCnt: {}", stat.new_key_cnt);
  logger->info("KeyCnt: {}", stat.key_cnt);
}

}
