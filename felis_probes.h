// -*- mode: c++ -*-
#ifndef FELIS_PROBES_H
#define FELIS_PROBES_H

#include "probe.h"

DEFINE_PROBE(felis, versions_per_epoch_on_gc);
DEFINE_PROBE(felis, wait_jiffies);
DEFINE_PROBE(felis, version_read);
DEFINE_PROBE(felis, commit_back_in_time);
DEFINE_PROBE(felis, deleted_gc_per_core);
DEFINE_PROBE(felis, chkpt_scan);
DEFINE_PROBE(felis, blocking_version_read);
DEFINE_PROBE(felis, linklist_search_read);
DEFINE_PROBE(felis, linklist_search_write);
DEFINE_PROBE(felis, local_index_cache);
DEFINE_PROBE(felis, index_get);

#define PROBE_ENBALED
#include "felis_probes.cc"
#undef PROBE_ENBALED

#endif /* FELIS_PROBES_H */
