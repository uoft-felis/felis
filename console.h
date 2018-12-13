#ifndef CONSOLE_H
#define CONSOLE_H

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <string>
#include <map>
#include <functional>
#include "util.h"
#include "json11/json11.hpp"

namespace felis {

class Console {
 public:
  enum ServerStatus {
    Booting, Configuring, Listening, Connecting, Running, Exiting,
  };
 private:
  std::mutex mutex;
  std::condition_variable cond;
  ServerStatus server_status = ServerStatus::Booting;
  std::string node_name;
  std::map<std::string, std::function<json11::Json (Console *, json11::Json)>> handlers;

  json11::Json conf;

  template <typename T> friend T &util::Instance();
 public:
  Console();
  Console(const Console &rhs) = delete;

  void set_server_node_name(std::string name) { node_name = name; }
  std::string server_node_name() const { return node_name; }
  void UpdateServerStatus(ServerStatus status) {
    std::unique_lock<std::mutex> _(mutex);
    server_status = status;
    cond.notify_all();
  }
  void WaitForServerStatus(ServerStatus status) {
    std::unique_lock<std::mutex> _(mutex);
    while (server_status != status) {
      cond.wait(_);
    }
  }

  json11::Json FindConfigSection(const char *section_name) const {
    auto &map = conf.object_items();
    auto it = map.find(section_name);
    if (it == map.end()) {
      fprintf(stderr, "Configuration section %s cannot be found. "
              "Check controller server's configuration\n", section_name);
      std::abort();
    }
    return it->second;
  }

  std::string HandleAPI(json11::Json j) {
    return HandleJsonAPI(j).dump();
  }
  json11::Json HandleJsonAPI(json11::Json j);
 private:
  json11::Json HandleStatusChange(json11::Json j);
  json11::Json HandleGetStatus(json11::Json j);

  json11::Json JsonResponse() {
    return json11::Json::object({
        {"type", "OK"},
        {"host", node_name},
      });
  }
  json11::Json JsonResponse(json11::Json::object props) {
    auto hdr = JsonResponse().object_items();
    props.insert(hdr.begin(), hdr.end());
    return props;
  }
};

}

#endif /* CONSOLE_H */
