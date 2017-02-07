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
#include "gopp/gopp.h"
#include "gopp/epoll-channel.h"

#include "log.h"

void DoDumpNetwork(int fds[])
{
  int nfds = dolly::Epoch::kNrThreads;
  std::ofstream **fout = new std::ofstream*[nfds];
  char *buffer = new char[4096];

  for (int i = 0; i < nfds; i++) {
    fout[i] = new std::ofstream(std::string("dolly-net.") + std::to_string(i) + ".dump");
  }

  logger->info("Dumping the network packets into dolly-net.*.dump");

  int max_fd = -1;
  fd_set fset;
  fd_set eof_fset;
  FD_ZERO(&fset);
  FD_ZERO(&eof_fset);

  // set sockets to async and watch for reading
  for (int i = 0; i < nfds; i++) {
    int opt = fcntl(fds[i], F_GETFL);
    fcntl(fds[i], F_SETFL, opt | O_NONBLOCK);
    FD_SET(fds[i], &fset);
    max_fd = std::max(max_fd, fds[i] + 1);
  }
  int eof_count = 0;
  while (true) {
    int res = select(max_fd, &fset, NULL, NULL, NULL);
    if (errno == EINTR || res == 0) continue;

    if (res < 0) {
      perror("select");
      std::abort();
    }
    logger->debug("found {} connections readable", res);
    max_fd = -1;
    for (int i = 0; i < nfds; i++) {
      int fd = fds[i];
      if (!FD_ISSET(fd, &fset)) {
	if (!FD_ISSET(fd, &eof_fset)) {
	  FD_SET(fd, &fset);
	  max_fd = std::max(max_fd, fd + 1);
	}
	continue;
      }
      int rsize = 0;
      int rcount = 0;
      int tot_rsize = 0;
      while (++rcount < 160 && (rsize = read(fd, buffer, 4096)) > 0) {
	fout[i]->write(buffer, rsize);
	tot_rsize += rsize;
      }
      if (rsize < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
	perror("read");
	std::abort();
      }
      if (rsize == 0) {
	logger->debug("connection {} EOF", i);
	FD_CLR(fd, &fset);
	FD_SET(fd, &eof_fset);
	eof_count++;
      } else {
	logger->debug("connection {} recv {} bytes", i, tot_rsize);
	max_fd = std::max(max_fd, fd + 1);
      }
    }
    if (eof_count == nfds)
      break;
  }
  puts("Network dump all done!");
  delete [] buffer;
  for (int i = 0; i < nfds; i++)
    delete fout[i];
  delete [] fout;
}

void show_usage(const char *progname)
{
  printf("Usage: %s [-t core_count] [-d]\n\n", progname);
  puts("\t-t\tnumber of cores used");
  puts("\t-d\tdump only");
  puts("\t-r\treplay from trace files");

  std::exit(-1);
}

// static const char *kDummyWebServerHost = "127.0.0.1";
static const char *kDummyWebServerHost = "142.150.234.186";
static const int kDummyWebServerPort = 8000;

int main(int argc, char *argv[])
{
  int opt;
  bool dump_only = false;
  bool replay_from_file = false;
  int timer_skip = 30;
  bool chkpt = false;
  std::string chkpt_format;
  std::string workload_name;

  InitializeLogger();

  while ((opt = getopt(argc, argv, "drw:s:c:")) != -1) {
    switch (opt) {
    case 'd':
      dump_only = true;
      break;
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

  if (workload_name == "" && !dump_only) {
    show_usage(argv[0]);
    return -1;
  }
  logger->info("Running {} workload", workload_name);

  int fd = dolly::SetupServer();
  int peer_fd = -1;

  socklen_t addrlen = sizeof(struct sockaddr_in);
  struct sockaddr_in addr;

  // we assume each core is creating a connection
  int *peer_fds = new int[dolly::Epoch::kNrThreads];

  if (replay_from_file) {
    memset(&addr, 0, addrlen);
    addr.sin_addr.s_addr = inet_addr(kDummyWebServerHost);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(kDummyWebServerPort);
  }

  for (int i = 0; i < dolly::Epoch::kNrThreads; i++) {
    if (replay_from_file) {
      peer_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (connect(peer_fd, (struct sockaddr *) &addr, addrlen) < 0) {
	perror("connect");
	std::abort();
      }
    } else {
      if ((peer_fd = accept(fd, (struct sockaddr *) &addr, &addrlen)) < 0) {
	perror("accept");
	std::abort();
      }
    }
    peer_fds[i] = peer_fd;
  }
  logger->info("replica received {} connections\n", dolly::Epoch::kNrThreads);
  logger->info("replica ready!");

  if (dump_only) {
    DoDumpNetwork(peer_fds);
    std::exit(0);
  }

  const int tot_nodes = dolly::Epoch::kNrThreads / mem::kNrCorePerNode;
  logger->info("setting up memory pools and regions. {} NUMA nodes in total", tot_nodes);

  dolly::Epoch::pools = (dolly::Epoch::BrkPool *)
    malloc(sizeof(dolly::Epoch::BrkPool) * tot_nodes);
  for (int nid = 0; nid < tot_nodes; nid++) {
    new (&dolly::Epoch::pools[nid])
      dolly::Epoch::BrkPool(dolly::Epoch::kBrkSize, 2 * mem::kNrCorePerNode, nid);
  }
  mem::InitThreadLocalRegions(dolly::Epoch::kNrThreads);
  for (int i = 0; i < dolly::Epoch::kNrThreads; i++) {
    auto &r = mem::GetThreadLocalRegion(i);
    r.ApplyFromConf("mem.json");
    logger->info("setting up regions {}", i);
    r.InitPools(i / mem::kNrCorePerNode);
  }

  dolly::SortedArrayVHandle::InitPools();

  logger->info("memory ready");

  logger->info("setting up co-routine thread pool");
  go::InitThreadPool(dolly::Epoch::kNrThreads);

  logger->info("loading base dataset");
  dolly::BaseRequest::LoadWorkloadSupportFromConf();

  // FIXME:
  // goto chkpt;
  {
    auto epoch_ch = new go::BufferChannel<dolly::Epoch *>(0);
    std::mutex m;
    m.lock();

    auto epoch_fetcher = new dolly::ClientFetcher(peer_fds, epoch_ch, workload_name);
    auto epoch_executor = new dolly::ClientExecutor(epoch_ch, &m, epoch_fetcher);

    epoch_fetcher->set_replay_from_file(replay_from_file);
    if (timer_skip > 0)
      epoch_fetcher->set_timer_skip_epoch(timer_skip);

    go::CreateGlobalEpoll();

    auto t = std::thread([]{ go::GlobalEpoll()->EventLoop(); });
    t.detach();

    epoch_fetcher->StartOn(1);
    epoch_executor->StartOn(2);

    m.lock(); // waits
    go::WaitThreadPool();
  }
// chkpt:
  // go::WaitThreadPool();
  // everybody quits. time to dump a checkpoint
  if (chkpt) {
    auto p_chkpt_impl = dolly::Checkpoint::LoadCheckpointImpl(chkpt_format);
    p_chkpt_impl->Export();
  }

  delete [] peer_fds;

  return 0;
}
