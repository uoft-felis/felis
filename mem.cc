#include <cassert>
#include "util.h"
#include "mem.h"
#include "goplusplus/gopp.h"

namespace mem {

static ThreadLocalRegion *regions;
static size_t nr_regions;

void InitThreadLocalRegions(int tot)
{
  nr_regions = tot;
  regions = new ThreadLocalRegion[tot];
}

ThreadLocalRegion &GetThreadLocalRegion(int idx)
{
  assert(idx < nr_regions && idx >= 0);
  return regions[idx];
}

static __thread int gAffinity = -1;


void SetThreadLocalAllocAffinity(int h)
{
  gAffinity = h;
}

int CurrentAllocAffinity()
{
  if (gAffinity != -1) return gAffinity;
  else return go::Scheduler::CurrentThreadPoolId() - 1;
}

}
