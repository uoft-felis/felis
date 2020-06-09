#include <thread>
#include <cstring>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include "console.h"
#include "module.h"
#include "util/objects.h"
#include "json11/json11.hpp"

namespace felis {

//
// The console protocol is just a json object with \0 char at the end.
//
// This means that the protocol does not support pipelining and the console
// WebUI has to send request one by one.
//
// Currently, there is no point to support concurrent requests. In fact, it
// might be cleaner to support just one connection to avoid races.
class ConsoleClient {
  static constexpr int kMaxBufferSize = (32 << 10) - 8;
  int fd;
  int len;
  char buffer[kMaxBufferSize];
 public:
  ConsoleClient(int fd) : fd(fd), len(0) {}
  void ReadRequests();
  bool WriteResponse(std::string);
};

void ConsoleClient::ReadRequests()
{
  while (true) {
    int rs = read(fd, buffer + len, kMaxBufferSize - len);
    if (rs < 0) {
      perror("read");
      goto done;
    } else if (rs == 0) {
      goto done;
    }

    len += rs;
    if (len >= kMaxBufferSize)
      goto done;

    if (buffer[len - 1] == 0) {
      std::string err;
      json11::Json j = json11::Json::parse(buffer, err);
      if (err.empty()) {
        auto res = util::Instance<Console>().HandleAPI(j);
        if (!WriteResponse(res))
          goto done;
      } else {
        fprintf(stderr, "Cannot parse request %s\n", err.c_str());
      }
      len = 0;
    }
  }
done:
  close(fd);
}

bool ConsoleClient::WriteResponse(std::string response)
{
  const char *p = response.c_str();
  size_t total = response.length() + 1;
  size_t l = 0;
  while (l < total) {
    int rs = write(fd, p + l, total - l);
    if (rs < 0) {
      if (errno == EINTR)
        continue;
      else
        return false;
    }
    l += rs;
  }
  return true;
}

class ConfModule : public Module<CoreModule> {
 public:
  static std::string kHost;
  static unsigned short kPort;
  ConfModule() {
    info = {
      .name = "config",
      .description = "Configuration and monitoring interface for the controller WebUI",
    };
    required = true;
  }
  void Init() final override {
    if (kHost == "") {
      fputs("Please specify the controller host and port\n", stderr);
      std::abort();
    }
    InitConsoleClient(kHost, kPort);
    auto &console = util::Instance<Console>();
    console.WaitForServerStatus(Console::ServerStatus::Configuring);
  }
 private:
  void InitConsoleClient(std::string host, unsigned short port);
};

std::string ConfModule::kHost = "";
unsigned short ConfModule::kPort = 3144;

void ConfModule::InitConsoleClient(std::string host, unsigned short port)
{
  printf("Console Client connecting to controller %s on port %u\n",
         host.c_str(), port);
  struct sockaddr_in addr;

  memset(&addr, 0, sizeof(struct sockaddr_in));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(host.c_str());
  addr.sin_port = htons(port);

  int s = socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0) {
    goto fail;
  }

  if (connect(s, (struct sockaddr *) &addr, sizeof(struct sockaddr_in)) < 0) {
    goto fail;
  }

  {
    auto t = std::thread(
        [=]() {
            auto client = new ConsoleClient(s);
            client->ReadRequests();
        });
    t.detach();
  }

  return;
fail:
  if (s >= 0)
    close(s);
  perror(__FUNCTION__);
  std::abort();
}

static ConfModule console_module;

void ParseControllerAddress(std::string arg)
{
  auto pos = arg.find(":");
  if (pos != arg.npos) {
    ConfModule::kHost = arg.substr(0, pos);
    ConfModule::kPort = std::stoi(arg.substr(pos + 1));
  } else {
    ConfModule::kHost = arg;
  }
}

}
