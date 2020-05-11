// -*- mode: c++ -*-

#ifndef YCSB_TABLE_DECL_H
#define YCSB_TABLE_DECL_H

#include "sqltypes.h"

namespace sql {

FIELD(uint64_t, k);
KEYS(YcsbKey);

static constexpr int kYcsbRecordSize = 1000;

FIELD(sql::inline_str_16<kYcsbRecordSize>, v);
VALUES(YcsbValue);

}

#endif
