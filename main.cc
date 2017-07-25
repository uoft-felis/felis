#include <unistd.h>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "epoch.h"
#include "index.h"
#include "client.h"
#include "module.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

#include "log.h"

void show_usage(const char *progname)
{
  printf("Usage: %s [-t core_count] [-d]\n\n", progname);
  puts("\t-t\tnumber of cores used");
  puts("\t-d\tdump only");
  puts("\t-r\treplay from trace files");

  std::exit(-1);
}

int main(int argc, char *argv[])
{
  int opt;
  bool replay_from_file = false;
  int timer_skip = 30;
  bool chkpt = false;
  std::string chkpt_format;
  std::string workload_name;

  InitializeLogger();

  while ((opt = getopt(argc, argv, "rw:s:c:")) != -1) {
    switch (opt) {
      case 'r':
        replay_from_file = true;
        break;
      case 'w':
        workload_name = std::string(optarg);
        break;
      case 's':
        timer_skip = atoi(optarg);
        break;
      case 'c':
        chkpt = true;
        chkpt_format = std::string(optarg);
        break;
      default:
        show_usage(argv[0]);
        break;
    }
  }

  if (workload_name == "") {
    show_usage(argv[0]);
    return -1;
  }
  {
    std::ofstream pid_fout("/tmp/dolly.pid");
    pid_fout << (unsigned long) getpid();
  }

  logger->info("Running {} workload", workload_name);

  dolly::Epoch::InitPools();

  mem::InitThreadLocalRegions(dolly::Epoch::kNrThreads);
  for (int i = 0; i < dolly::Epoch::kNrThreads; i++) {
    auto &r = mem::GetThreadLocalRegion(i);
    r.ApplyFromConf("mem.json");
    logger->info("setting up regions {}", i);
    r.InitPools(i / mem::kNrCorePerNode);
  }

  dolly::VHandle::InitPools();
  dolly::Txn::InitPools();

  logger->info("memory ready");

  logger->info("setting up co-routine thread pool");
  go::InitThreadPool(dolly::Epoch::kNrThreads);

  logger->info("loading base dataset");
  dolly::Module<dolly::WorkloadModule>::InitAllModules();

  // FIXME:
  // goto chkpt;
  {
    auto epoch_ch = new go::BufferChannel(sizeof(void *));
    std::mutex m;
    m.lock();

    auto epoch_fetcher = new dolly::ClientFetcher(epoch_ch, workload_name);
    auto epoch_executor = new dolly::ClientExecutor(epoch_ch, &m, epoch_fetcher);

    epoch_fetcher->set_replay_from_file(replay_from_file);
    if (timer_skip > 0)
      epoch_fetcher->set_timer_skip_epoch(timer_skip);

    go::GetSchedulerFromPool(1)->WakeUp(epoch_fetcher);
    go::GetSchedulerFromPool(2)->WakeUp(epoch_executor);

    m.lock(); // waits
    go::WaitThreadPool();
  }
  // chkpt:
  // go::WaitThreadPool();
  // everybody quits. time to dump a checkpoint
  if (chkpt) {
    PerfLog p;
    dolly::Module<dolly::ExportModule>::InitAllModules();
    auto p_chkpt_impl = dolly::Checkpoint::checkpoint_impl(chkpt_format);
    p_chkpt_impl->Export();
    p.Show("checkpoint takes");
  }

  return 0;
}
