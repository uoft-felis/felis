#include "shipping.h"
#include "util.h"
#include <gtest/gtest.h>
#include <unistd.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <thread>

namespace felis {

namespace {

class TestObject {
  ShippingHandle handle;
  int value;
 public:
  TestObject(int value) : value(value) {}
  TestObject() : value(0) {}

  ShippingHandle *shipping_handle() { return &handle; }
  void EncodeIOVec(struct iovec *vec) {
    vec->iov_base = &value;
    vec->iov_len = 4;
  }
  void DecodeIOVec(struct iovec *vec) {
    EXPECT_EQ(vec->iov_len, 4);
    memcpy(vec->iov_base, &value, 4);
  }
};

class ShippingTest : public testing::Test {
 public:
  virtual void TearDown() final override {
    unlink("/tmp/test.sock");
  }
};

TEST_F(ShippingTest, SimplePipeTest) {
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  sockaddr_un addr;
  addr.sun_family = AF_UNIX;
  strcpy(addr.sun_path, "/tmp/test.sock");
  ASSERT_EQ(bind(fd, (sockaddr *) &addr, sizeof(sockaddr_un)), 0);
  ASSERT_EQ(listen(fd, 1024), 0);

  // Run!
  auto t = std::thread(
      [] {
        sockaddr_un addr;
        addr.sun_family = AF_UNIX;
        strcpy(addr.sun_path, "/tmp/test.sock");
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        ASSERT_EQ(connect(fd, (sockaddr *) &addr, sizeof(sockaddr_un)), 0);
        auto s = Shipment<TestObject>(fd);
        for (int i = 0; i < 2 << 20; i++) {
          s.AddShipment(new TestObject(i + 10));
        }
        while (!s.RunSend());
      });

  sockaddr_un claddr;
  socklen_t cllen;
  int read_end = accept(fd, (sockaddr *) &claddr, &cllen);
  auto s = Shipment<TestObject>(read_end);
  while (true) {
    TestObject o;
    if (!s.Receive(&o)) {
      break;
    }
  }
  t.join();
}

}

}
