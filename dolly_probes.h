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

// depends on the probes impl
PROBE(dolly, wait_jiffies, long jiffies);
PROBE(general, process_exit, void);

#endif /* DOLLY_PROBES_H */
