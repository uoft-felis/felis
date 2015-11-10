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
  void Clear();
};

#endif /* LOG_H */
