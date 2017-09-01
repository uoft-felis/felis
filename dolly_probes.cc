#include <cstdlib>
#include <cstdio>

#include "dolly_probes.h"

static agg::Agg<agg::Sum> tot_wait;

static thread_local struct {
  agg::Agg<agg::Sum>::Value nr_wait = tot_wait;
} stat;

PROBE(dolly, wait_jiffies, long jiffies) {
  stat.nr_wait << jiffies;
}

PROBE(general, process_exit, void)
{
  printf("wait_jiffies: %lu\n", tot_wait.sum);
}
