#include <gtest/gtest.h>

#include "sqltypes.h"

namespace felis {

using sql::Tuple;

class TupleTest : public testing::Test {
};

TEST_F(TupleTest, Simple) {
  void *buf = alloca(4096);
  Tuple<int> t(10);
  VarStr *s = t.EncodeFromAlloca(buf);
  t.Decode(s);
  ASSERT_EQ(t._<0>(), 10);
}

}
