// -*- mode: c++ -*-

#ifndef YCSB_TABLE_DECL_H
#define YCSB_TABLE_DECL_H

#ifndef OFFSET
#define OFFSET 1000
#endif

#include "sqltypes.h"

namespace sql {

FIELD(uint64_t, k);
KEYS(YcsbKey);

static constexpr int kYcsbRecordSize = 1000;

FIELD(sql::inline_str_16<kYcsbRecordSize>, v);
VALUES(YcsbValue);

}

#endif
