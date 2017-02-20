#include <cstdlib>
#include <unistd.h>
#include <string.h>
#include <cstdio>
#include <iostream>

#include "net-io.h"

namespace dolly {

void ReadFrom(int fd, void *p, size_t size)
{
  uint8_t *ptr = (uint8_t *) p;
  size_t cur = 0;
  while (cur < size) {
    int len = read(fd, ptr + cur, size - cur);
    if (len < 0) {
      perror("read");
      continue;
    } else if (len == 0) {
      throw ParseBufferEOF();
    }
    cur += len;
  }
}


}
