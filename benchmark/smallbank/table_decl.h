// -*- mode: c++ -*-

#ifndef SMALLBANK_TABLE_DECL_H
#define SMALLBANK_TABLE_DECL_H

#ifndef OFFSET
#define OFFSET 2000
#endif

#include "sqltypes.h"

namespace sql {

FIELD(uint64_t, AccountName);
KEYS(AccountKey);

FIELD(uint64_t, CustomerID);
VALUES(AccountValue);

FIELD(uint64_t, CustomerIDSv);
KEYS(SavingKey);

FIELD(int64_t, BalanceSv);
VALUES(SavingValue);

FIELD(uint64_t, CustomerIDCk);
KEYS(CheckingKey);

FIELD(int64_t, BalanceCk);
VALUES(CheckingValue);
}

#endif
