#include <sys/time.h>
#include "log.h"

#ifdef NDEBUG
// std::shared_ptr<spdlog::logger> logger = spdlog::rotating_logger_mt("file_logger", "log", 1048576 * 5, 3, true);
std::shared_ptr<spdlog::logger> logger = spdlog::stdout_logger_mt("console", true);
#else
std::shared_ptr<spdlog::logger> logger = spdlog::daily_logger_mt("daily_logger", "debug/log", 2, 30);
// std::shared_ptr<spdlog::logger> logger = spdlog::stdout_logger_mt("console", true);
#endif

void InitializeLogger()
{
#ifdef NDEBUG
  spdlog::set_level(spdlog::level::info);
#else
  spdlog::set_level(spdlog::level::debug);
#endif
  spdlog::set_pattern("[%H:%M:%S] [thread %t] %v");
}

PerfLog::PerfLog()
  : is_started(false), duration(0)
{
  Start();
}

void PerfLog::Start()
{
  if (!is_started) {
    is_started = true;
    gettimeofday(&tv, NULL);
  }
}

void PerfLog::End()
{
  if (is_started) {
    is_started = false;
    struct timeval newtv;
    gettimeofday(&newtv, NULL);
    duration += (newtv.tv_sec - tv.tv_sec) * 1000
      + (newtv.tv_usec - tv.tv_usec) / 1000;
  }
}

void PerfLog::Show(const char *msg)
{
  if (is_started) End();
  logger->info("{} {}ms", msg, duration);
}

void PerfLog::Clear()
{
  if (!is_started) duration = 0;
}
