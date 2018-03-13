#ifndef CONSOLE_H
#define CONSOLE_H

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <string>
#include "util.h"

namespace dolly {

class Console {
  std::mutex mutex;
  std::condition_variable cond;
  std::string server_status;

  static Console *instance;
  template <typename T> friend T &util::Instance();

 public:
  void UpdateServerStatus(std::string status) {
    std::unique_lock<std::mutex> _(mutex);
    server_status = status;
    cond.notify_all();
  }
  void WaitForServerStatus(std::string status) {
    std::unique_lock<std::mutex> _(mutex);
    while (server_status != status) {
      cond.wait(_);
    }
  }

  std::string HandleAPI(std::string uri);
};

}

#endif /* CONSOLE_H */
