// -*- c++ -*-

#ifndef LOG_H
#define LOG_H

#include <memory>
#include <mutex>
#include "spdlog/spdlog.h"

extern std::unique_ptr<spdlog::logger> logger;

void InitializeLogger(const std::string &hostname);

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

  uint32_t duration_ms() const { return duration; }
};

#define abort_if(cond, ...)                     \
  if (__builtin_expect(cond, 0)) {              \
    logger->critical(__VA_ARGS__);              \
    std::abort();                               \
  }

#define TBD()                                                           \
  do {                                                                  \
    logger->critical("TBD: Implement {}", __PRETTY_FUNCTION__);         \
    abort();                                                            \
  } while (0)                                                           \

#define REMINDER(msg)                                                   \
  static std::once_flag __call_once_flag;                               \
  std::call_once(                                                       \
      __call_once_flag,                                                 \
      []() {                                                            \
        auto p = __PRETTY_FUNCTION__;                                   \
        logger->critical("TODO: In {}:{}, {} {}", __FILE__, __LINE__,   \
                         __PRETTY_FUNCTION__, msg);                     \
      })                                                                \

// We don't use logger->trace because it's even lower than debug. We would like
// to turn on trace for only some tags.
static bool is_trace_enabled(std::string_view msg = "")
{
  return (msg.length() > 0 && msg.at(0) == '\x7f');
}

template <typename ...T>
static void trace(std::string_view fmt, T... args)
{
  if (is_trace_enabled(fmt)) {
    logger->info(fmt.substr(1).data(), args...);
  }
}

// Trace tags
#define TRACE_EXEC_ROUTINE // "\x7f" "Trace ExecRoutine: "
#define TRACE_GC // "\x7f" "Trace GC: "

#endif /* LOG_H */
