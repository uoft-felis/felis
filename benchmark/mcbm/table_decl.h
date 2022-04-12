// -*- mode: c++ -*-

#ifndef MCBM_TABLE_DECL_H
#define MCBM_TABLE_DECL_H

#ifndef OFFSET
#define OFFSET 3000
#endif

#include "sqltypes.h"

namespace sql {

FIELD(uint64_t, k);
KEYS(McbmKey);


FIELD(uint64_t, v);
VALUES(McbmValue);

}

#endif
