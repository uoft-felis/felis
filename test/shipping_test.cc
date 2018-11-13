#include "gopp/channels.h"
#include "shipping.h"
#include "util.h"
#include <gtest/gtest.h>
#include <random>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

namespace felis {

namespace {

class TestObject {
  ShippingHandle handle;

public:
  int value;
  TestObject(int value) : value(value) {}
  TestObject() : value(0) {}

  void SetValue(int value) { this->value = value; }

  ShippingHandle *shipping_handle() { return &handle; }
  int EncodeIOVec(struct iovec *vec, int max_nr_vec) {
    if (max_nr_vec < 1)
      return 0;
    vec->iov_base = &value;
    vec->iov_len = 4;

    encoded_len = 4;
    return 1;
  }

  uint64_t encoded_len;

  void DecodeIOVec(struct iovec *vec) {
    EXPECT_EQ(vec->iov_len, 4);
    memcpy(&value, vec->iov_base, 4);
  }
};

class ShippingTest : public testing::Test {
public:
};

TEST_F(ShippingTest, SimplePipeTest)
{
  int srv = socket(AF_INET, SOCK_STREAM, 0);
  int enable = 1;
  setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

  go::TcpSocket::CommonInetAddr addr, claddr;
  socklen_t socklen;
  go::TcpSocket::FillSockAddr(addr, socklen, AF_INET, "127.0.0.1", 41345);
  ASSERT_EQ(bind(srv, addr.sockaddr(), socklen), 0);
  ASSERT_EQ(listen(srv, 1), 0);

  // Run!
  auto t = std::thread([] {
    auto s = Shipment<TestObject>("127.0.0.1", 41345);
    for (int i = 0; i < 2 << 17; i++) {
      s.AddObject(new TestObject(i + 10));
    }
    while (!s.RunSend())
      ;
  });

  int fd = accept(srv, addr.sockaddr(), &socklen);
  ASSERT_GT(fd, 0);

  auto s = ShipmentReceiver<TestObject>(fd);
  int i = 0;
  while (true) {
    TestObject o;
    if (!s.Receive(&o)) {
      break;
    }
    ASSERT_EQ(o.value, i + 10);
    i++;
  }
  t.join();
}

void ThreadSend(TestObject obj[])
{
  auto s = Shipment<TestObject>("127.0.0.1", 41346);
  for (int i = 0; i < 2 << 17; i++) {
    s.AddObject(&obj[i]);
  }
  while (!s.RunSend());
}

void ThreadUpdate(TestObject obj[])
{
  auto s = Shipment<TestObject>("127.0.0.1", 41346);
  std::random_device rd;
  std::default_random_engine gen = std::default_random_engine(rd());
  std::uniform_int_distribution<int> dis(1, 2 << 17);

  for (int i = 0; i < 2 << 15; i++) {
    int idx = dis(gen);
    obj[idx].SetValue(i);
    if (obj[idx].shipping_handle()->MarkDirty()) {
      s.AddObject(&obj[i]);
    }
  }
}

TEST_F(ShippingTest, MutableTest)
{
  int srv = socket(AF_INET, SOCK_STREAM, 0);
  int enable = 1;
  setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

  go::TcpSocket::CommonInetAddr addr, claddr;
  socklen_t socklen;
  go::TcpSocket::FillSockAddr(addr, socklen, AF_INET, "127.0.0.1", 41346);
  ASSERT_EQ(bind(srv, addr.sockaddr(), socklen), 0);
  ASSERT_EQ(listen(srv, 1), 0);

  TestObject obj[2 << 17];
  std::thread t(ThreadSend, obj);
  std::thread t1(ThreadUpdate, obj);

  int fd = accept(srv, addr.sockaddr(), &socklen);
  ASSERT_GT(fd, 0);

  auto s = ShipmentReceiver<TestObject>(fd);

  while (true) {
    TestObject o;
    if (!s.Receive(&o)) {
      break;
    }
  }
  t1.join();
  t.join();
}

}

}
