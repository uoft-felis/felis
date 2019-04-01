#ifndef LITERALS_H
#define LITERALS_H

#include <cstdlib>

// public namespace!

constexpr unsigned long long operator"" _M(unsigned long long N) { return N << 20; }
constexpr unsigned long long operator"" _K(unsigned long long N) { return N << 10; }

#endif
