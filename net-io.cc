#include <cstdlib>
#include <unistd.h>
#include <string.h>
#include <cstdio>
#include <iostream>

#include "net-io.h"

namespace db_backup {

void ParseBuffer::FillDataFromFD(int fd, void *p, size_t size)
{
  uint8_t *ptr = (uint8_t *) p;
  size_t cur = 0;
  while (cur < size) {
    int len = read(fd, ptr + cur, size - cur);
    if (len < 0) {
      perror("read");
      continue;
    }
    cur += len;
  }
}

static const char *kBackupServerListenAddr = "0.0.0.0";
static const int kBackupServerPort = 1777;

int SetupServer()
{
  struct sockaddr_in serv;
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  memset(&serv, 0, sizeof(sockaddr_in));

  serv.sin_family = AF_INET;
  serv.sin_addr.s_addr = inet_addr(kBackupServerListenAddr);
  serv.sin_port = htons(kBackupServerPort);

  if (bind(fd, (struct sockaddr *) &serv, sizeof(struct sockaddr)) < 0) {
    perror("bind");
    goto error;
  }
  if (listen(fd, 1) < 0) {
    perror("listen");
    goto error;
  }
  std::cerr << "listening on port " << kBackupServerPort << std::endl;
  return fd;
error:
  close(fd);
  return -1;
}

}
