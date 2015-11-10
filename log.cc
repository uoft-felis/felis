#include <sys/time.h>
#include "log.h"

std::shared_ptr<spdlog::logger> logger = spdlog::stdout_logger_mt("console");

void InitializeLogger()
{
  spdlog::set_level(spdlog::level::info); // or info
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
