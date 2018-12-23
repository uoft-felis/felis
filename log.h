// -*- c++ -*-

#ifndef LOG_H
#define LOG_H

#include <memory>
#include "spdlog/spdlog.h"

extern std::shared_ptr<spdlog::logger> logger;

void InitializeLogger();

class PerfLog {
  struct timeval tv;
  bool is_started;
  uint32_t duration;
 public:
  PerfLog();
  void Start();
  void End();
  void Show(const char *msg);
  void Show(std::string str) { Show(str.c_str()); }
  void Clear();
};

#define abort_if(cond, ...)                     \
  if (cond) {                                   \
    logger->critical(__VA_ARGS__);              \
    std::abort();                               \
  }

#define TBD()                                                           \
  do {                                                                  \
    auto p = __PRETTY_FUNCTION__;                                       \
    logger->critical("TBD: Implement {}", p);                           \
    abort();                                                            \
  } while (0)                                                           \

#endif /* LOG_H */
