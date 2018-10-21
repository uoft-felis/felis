#include "util.h"
#include "promise.h"

#include <vector>
#include <queue>
#include <gtest/gtest.h>

namespace felis {

struct TestTransport : public PromiseRoutineTransportService {
  void TransportPromiseRoutine(PromiseRoutine *routine) override final;
};

struct TestServer {
  std::vector<void *> packets;
  void Run();
};

static std::vector<TestServer> servers;

void InitTestingServer()
{
  servers.clear();
  servers.push_back(TestServer());
  servers.push_back(TestServer());
}

void TestTransport::TransportPromiseRoutine(PromiseRoutine *routine)
{
  TestServer *target = &servers[routine->node_id];
  uint64_t buffer_size = routine->TreeSize();
  uint8_t *buffer = (uint8_t *) malloc(8 + buffer_size);

  // printf("%s to %d\n", __FUNCTION__, routine->node_id);

  memcpy(buffer, &buffer_size, 8);
  routine->EncodeTree(buffer + 8);
  target->packets.push_back(buffer);
}

void TestServer::Run()
{
  std::queue<PromiseRoutine *> run_queue;
  for (auto p: packets) {
    uint8_t *buffer = (uint8_t *) p;
    uint64_t promise_size = *(uint64_t *) p;

    auto pool = PromiseRoutinePool::Create(promise_size);
    memcpy(pool->mem, buffer + 8, promise_size);
    auto r = PromiseRoutine::CreateFromBufferedPool(pool);
    free(p);
    run_queue.push(r);
  }
  packets.clear();
  // printf("%p runqueue: %lu\n", this, run_queue.size());
  while (!run_queue.empty()) {
    auto r = run_queue.front();
    r->callback(r);
    run_queue.pop();
  }
}

using sql::Tuple;

namespace {

class PromiseTest : public testing::Test {
 public:
  virtual void SetUp() final override {
    InitTestingServer();
  }
};

TEST_F(PromiseTest, Simple)
{
  int state = 1;
  {
    PromiseProc _;
    auto p = (uintptr_t) &state;
    _ >> T(p, 1, [](auto ctx, auto _) -> Optional<Tuple<int>> {
        int *state = (int *) ctx;
        EXPECT_EQ(*state, 1);
        (*state)++;
        return Tuple<int>(1);
      }) >> T(p, 0, [](auto ctx, auto last) -> Optional<VoidValue> {
          int *state = (int *) ctx;
          EXPECT_EQ(*state, 2);
          (*state)++;
          return nullopt;
        });
  }

  EXPECT_EQ(servers[0].packets.size(), 0);
  EXPECT_EQ(servers[1].packets.size(), 1);

  servers[1].Run();
  EXPECT_EQ(state, 2);

  EXPECT_EQ(servers[0].packets.size(), 1);
  EXPECT_EQ(servers[1].packets.size(), 0);

  servers[0].Run();
  EXPECT_EQ(state, 3);

  EXPECT_EQ(servers[0].packets.size(), 0);
  EXPECT_EQ(servers[1].packets.size(), 0);
}

TEST_F(PromiseTest, Combine)
{
  CombinerState<Tuple<int>, Tuple<int>> state;
  auto state_ptr = &state;
  int capture = 0;
  auto p = (uintptr_t) &capture;
  {
    PromiseProc _;
    auto a =
        (_ >> T(p, 1, [](auto ctx, auto _) -> Optional<Tuple<int>> {
            auto capture = (int *) ctx;
            (*capture)++;
            return Tuple<int>(1);
          })).promise();
    auto b =
        (_ >> T(p, 0, [](auto ctx, auto _) -> Optional<Tuple<int>> {
            auto capture = (int *) ctx;
            (*capture)++;
            return Tuple<int>(2);
          })).promise();
    auto c = Combine(0, state_ptr, a, b).stream();
    c >> T(p, 0, [](auto ctx, auto _) -> Optional<VoidValue> {
        auto capture = (int *) ctx;
        EXPECT_EQ(*capture, 2);
        (*capture)++;
        return nullopt;
      });
  }

  servers[1].Run();
  servers[0].Run();

  servers[0].Run();
  servers[0].Run();
  EXPECT_EQ(capture, 3);
}

}

}

namespace util {

using namespace felis;

template <>
PromiseRoutineTransportService &Impl()
{
  return Instance<TestTransport>();
}

}
