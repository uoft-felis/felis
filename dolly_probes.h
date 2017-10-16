// -*- mode: c++ -*-
#ifndef DOLLY_PROBES_H
#define DOLLY_PROBES_H

#include "probe.h"

DEFINE_PROBE(dolly, versions_per_epoch_on_gc);
DEFINE_PROBE(dolly, wait_jiffies);
DEFINE_PROBE(dolly, version_read);
DEFINE_PROBE(dolly, commit_back_in_time);
DEFINE_PROBE(dolly, deleted_gc_per_core);
DEFINE_PROBE(dolly, chkpt_scan);
DEFINE_PROBE(dolly, blocking_version_read);
DEFINE_PROBE(dolly, linklist_search_read);
DEFINE_PROBE(dolly, linklist_search_write);
DEFINE_PROBE(dolly, local_index_cache);
DEFINE_PROBE(dolly, index_get);

#define PROBE_ENBALED
#include "dolly_probes.cc"
#undef PROBE_ENBALED

#endif /* DOLLY_PROBES_H */
