#include <unistd.h>
#include <cstdio>
#include <iostream>
#include "net-io.h"
#include "epoch.h"

#include "log.h"

int main(int argc, char *argv[])
{
  InitializeLogger();

  int fd = db_backup::SetupServer();
  int peer_fd = -1;

  db_backup::BaseRequest::LoadWorkloadSupportFromConf();

  socklen_t addrlen = sizeof(struct sockaddr_in);
  struct sockaddr_in addr;

  if ((peer_fd = accept(fd, (struct sockaddr *) &addr, &addrlen)) < 0) {
    perror("accept");
    std::abort();
  }

  logger->info("replica ready!");

  // TODO: initialize DB
  while (true) {
    db_backup::Epoch epoch(peer_fd);
    logger->info("received a complete epoch");
  }

  return 0;
}
