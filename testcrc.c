#include <stdio.h>
#include "csum.h"

int main(int argc, char *argv[])
{
  const char *p = "AB", *q = "BA";
  int a = INITIAL_CRC32_VALUE, b = INITIAL_CRC32_VALUE;
  update_crc32(p, 2, &a);
  update_crc32(q, 2, &b);

  printf("a %d b %d\n", a, b);
  return 0;
}
