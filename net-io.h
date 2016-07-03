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

int SetupServer();

}

#endif /* NET_IO_H */
