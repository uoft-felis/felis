#include "masstree/build/config.h"
#include "masstree/kvthread.hh"

volatile mrcu_epoch_type active_epoch;
volatile mrcu_epoch_type globalepoch = 1;

kvtimestamp_t initial_timestamp;
kvepoch_t global_log_epoch;
