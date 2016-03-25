// -*- c++ -*-

#ifndef NET_IO_H
#define NET_IO_H

#include <cstdlib>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <sys/types.h>
#include <cstdint>
#include <cstring>

namespace dolly {

class ParseBuffer {
  uint8_t *ptr;
  size_t offset;
  size_t size;
public:
  static void FillDataFromFD(int fd, void *ptr, size_t size);

  ParseBuffer(uint8_t *data, size_t sz) : ptr(data), offset(0), size(sz) {}
  ParseBuffer() : ptr(nullptr), offset(0), size(0) {}

  const uint8_t *data() const { return ptr + offset; }
  const bool is_empty() const { return offset == size; }

  void Advance(size_t sz) {
    offset += sz;
    if (offset > size) abort();
  }

  void Read(void *p, size_t sz) {
    if (offset + sz > size) abort();
    memcpy(p, ptr + offset, sz);
    Advance(sz);
  }

};

int SetupServer();

}

#endif /* NET_IO_H */
