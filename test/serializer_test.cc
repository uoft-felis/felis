#include <gtest/gtest.h>

#include "sqltypes.h"
#include "benchmark/tpcc/tpcc.h"

namespace felis {

using sql::Tuple;

class TupleTest : public testing::Test {
 protected:
  void *buf = nullptr;
  virtual void SetUp() final override {
    buf = malloc(4096);
  }
  virtual void TearDown() final override {
    free(buf);
  }
};

TEST_F(TupleTest, Simple) {
  Tuple<int> t(10);
  VarStr *s = t.EncodeFromAlloca(buf);
  Tuple<int> n;
  n.Decode(s);
  ASSERT_EQ(n._<0>(), 10);
}

TEST_F(TupleTest, Nested) {
  Tuple<Tuple<int, int>> t(Tuple<int, int>(23, 24));
  VarStr *s = t.EncodeFromAlloca(buf);
  Tuple<Tuple<int, int>> n;
  n.Decode(s);

  int a, b;
  n._<0>().Unpack(a, b);

  ASSERT_EQ(a, 23);
  ASSERT_EQ(b, 24);
}

TEST_F(TupleTest, NestedDBRow) {
  Tuple<tpcc::Customer::Key> t(tpcc::Customer::Key::New(10, 1000, 12));
  VarStr *s = t.EncodeFromAlloca(buf);
  Tuple<tpcc::Customer::Key> n;
  n.Decode(s);
  const auto &k = n._<0>();
  ASSERT_EQ(k.c_w_id, 10);
  ASSERT_EQ(k.c_d_id, 1000);
  ASSERT_EQ(k.c_id, 12);
}

}
