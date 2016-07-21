#include <cassert>
#include "util.h"
#include "mem.h"

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

}
