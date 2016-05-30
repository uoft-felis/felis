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
#include <exception>

namespace dolly {

class ParseBufferEOF : public std::exception {
public:
  virtual const char *what() const noexcept {
    return "ParseBuffer EOF";
  }
};

void ReadFrom(int fd, void *p, size_t size);

// later on this should be replaced with a CSP style channel
class InputChannel {
  int fd;
public:
  InputChannel(int file_desc) : fd(file_desc) {}

  void Read(void *p, size_t size) { ReadFrom(fd, p, size); }
};
int SetupServer();

}

#endif /* NET_IO_H */
