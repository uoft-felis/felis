#include "probe.h"

static void AtExit()
{
  DTRACE_PROBE0(general, process_exit);
}

static void __attribute__((constructor)) StartProbing()
{
  std::atexit(AtExit);
}
