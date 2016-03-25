#include <unistd.h>
#include <cstdio>
#include <iostream>
#include "net-io.h"
#include "epoch.h"
#include "worker.h"

#include "log.h"

int main(int argc, char *argv[])
{
  InitializeLogger();

  int fd = dolly::SetupServer();
  int peer_fd = -1;

  dolly::BaseRequest::LoadWorkloadSupportFromConf();

  socklen_t addrlen = sizeof(struct sockaddr_in);
  struct sockaddr_in addr;

  // we assume each core is creating a connection
  int *peer_fds = new int[dolly::Worker::kNrThreads];
  // per-core buffer
  dolly::ParseBuffer *buffers = new dolly::ParseBuffer[dolly::Worker::kNrThreads];

  for (int i = 0; i < dolly::Worker::kNrThreads; i++) {
    if ((peer_fd = accept(fd, (struct sockaddr *) &addr, &addrlen)) < 0) {
      perror("accept");
      std::abort();
    }
    peer_fds[i] = peer_fd;
  }
  logger->info("replica received {} connections\n", dolly::Worker::kNrThreads);
  logger->info("replica ready!");

  while (true) {
    dolly::Epoch epoch(peer_fds, buffers);
    logger->info("received a complete epoch");
  }

  delete [] peer_fds;

  return 0;
}
