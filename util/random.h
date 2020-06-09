// -*- c++ -*-
#ifndef UTIL_RANDOM_H
#define UTIL_RANDOM_H

#include <cstdint>
#include <string>

namespace util {

// not thread-safe
//
// taken from java:
//   http://developer.classpath.org/doc/java/util/Random-source.html
class FastRandom {
 public:
  FastRandom(unsigned long seed)
      : seed(0)
  {
    set_seed0(seed);
  }

  unsigned long next() {
    return ((unsigned long) next(32) << 32) + next(32);
  }

  uint32_t next_u32() {
    return next(32);
  }

  uint16_t next_u16() {
    return next(16);
  }

  /** [0.0, 1.0) */
  double next_uniform() {
    return (((unsigned long) next(26) << 27) + next(27)) / (double) (1L << 53);
  }

  char next_char() {
    return next(8) % 256;
  }

  char next_readable_char() {
    static const char readables[] = "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    return readables[next(6)];
  }

  std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++)
      s[i] = next_char();
    return s;
  }

  std::string next_readable_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++)
      s[i] = next_readable_char();
    return s;
  }

  unsigned long get_seed() {
    return seed;
  }

  void set_seed(unsigned long seed) {
    this->seed = seed;
  }

 private:
  void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return (unsigned long) (seed >> (48 - bits));
  }

  unsigned long seed;
};

template <typename T>
class XORRandom {
  T state;
 public:
  XORRandom(T state) : state(state) {}
  T Next() {
    auto x = state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    state = x;
    return x;
  }

  T NextRange(T begin, T end) {
    return Next() % (end - begin) + begin;
  }
};

class XORRandom32 : public XORRandom<uint32_t> {
 public:
  XORRandom32() : XORRandom<uint32_t>(0x25F16D1D) {}
};

class XORRandom64 : public XORRandom<uint64_t> {
 public:
  XORRandom64() : XORRandom<uint64_t>(0x2545F4914F6CDD1D) {}
};

}

#endif
