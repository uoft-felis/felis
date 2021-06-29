#include "piece_cc.h"
#include "sqltypes.h"

#include <vector>
#include <queue>
#include <gtest/gtest.h>

namespace felis {

struct TestTransport : public PromiseRoutineTransportService {
  virtual void TransportPromiseRoutine(PieceRoutine *routine) override final;
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

void TestTransport::TransportPromiseRoutine(PieceRoutine *routine)
{
  TestServer *target = &servers[routine->node_id];
  uint64_t buffer_size = routine->NodeSize();
  uint8_t *buffer = (uint8_t *) malloc(8 + buffer_size);

  // printf("%s to %d\n", __FUNCTION__, routine->node_id);

  memcpy(buffer, &buffer_size, 8);
  routine->EncodeNode(buffer + 8);
  target->packets.push_back(buffer);
}

void TestServer::Run()
{
  std::queue<PieceRoutine *> run_queue;
  for (auto p: packets) {
    uint8_t *buffer = (uint8_t *) p;
    uint64_t size = *(uint64_t *) p;
    auto r = PieceRoutine::CreateFromCapture(size);
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
  int *p = &state;
  {
    PieceCollection root;
    root.AttachRoutine(Tuple<int *>(p), 1, [](auto ctx) {
        int *state;
        ctx.Unpack(state);

        EXPECT_EQ(*state, 1);
        (*state)++;
    });

    root.AttachRoutine(Tuple<int *>(p), 0, [](auto ctx) {
      int *state;
      ctx.Unpack(state);

      EXPECT_EQ(*state, 2);
      (*state)++;
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
