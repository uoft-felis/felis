// -*- c++ -*-

#ifndef UTIL_H
#define UTIL_H

#include <sys/types.h>
#include <functional>
#include <string>
#include <cassert>
#include <atomic>
#include <memory>
#include <unistd.h>
#include <sched.h>
#include <pthread.h>

#include <optional>

#include "gopp/gopp.h"

#define CACHE_ALIGNED __attribute__((aligned(CACHE_LINE_SIZE)))
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define __XCONCAT2(a, b) a ## b
#define CACHE_PADOUT                                                    \
  char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHE_LINE_SIZE)))

#ifndef likely
#define likely(x) __builtin_expect((x),1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x),0)
#endif

#ifndef container_of
#define container_of(ptr, type, member) ({			\
      (type *)((char *)ptr - offsetof(type, member)); })
#endif

namespace util {

// padded, aligned primitives
template <typename T, bool Pedantic = true>
class CacheAligned {
 public:

  template <class... Args>
  CacheAligned(Args &&... args)
      : elem(std::forward<Args>(args)...)
  {
    if (Pedantic)
      assert(((uintptr_t)this % CACHE_LINE_SIZE) == 0);
  }

  T elem;
  CACHE_PADOUT;

  // syntactic sugar- can treat like a pointer
  inline T & operator*() { return elem; }
  inline const T & operator*() const { return elem; }
  inline T * operator->() { return &elem; }
  inline const T * operator->() const { return &elem; }

 private:
  inline void
  __cl_asserter() const
  {
    static_assert((sizeof(*this) % CACHE_LINE_SIZE) == 0, "xx");
  }
} CACHE_ALIGNED;

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

// link list headers. STL is too slow
template <typename T>
struct GenericListNode {
  GenericListNode<T> *prev, *next;

  void InsertAfter(GenericListNode<T> *parent) {
    prev = parent;
    next = parent->next;
    parent->next->prev = this;
    parent->next = this;
  }

  void Remove() {
    prev->next = next;
    next->prev = prev;
    prev = next = nullptr; // detached
  }

  void Initialize() {
    prev = next = this;
  }

  bool empty() const { return next == this; }

  T *object() { return static_cast<T *>(this); }
};

using ListNode = GenericListNode<void>;

// Type Safe Embeded LinkListNode. E is a enum, serves as a variable name.
template <typename T, int E> struct TypedListNode;

template <typename T>
struct TypedListNodeWrapper {
  template <int E>
  static TypedListNode<T, E> *ToListNode(T *obj) {
    return obj;
  }
};

template <typename T, int E>
struct TypedListNode : public TypedListNodeWrapper<T>,
                       public GenericListNode<TypedListNode<T, E>> {
  T *object() { return static_cast<T *>(this); }
};

class SpinLock {
  std::atomic_bool lock = false;
 public:
  void Lock() {
    bool locked = false;
    while (!lock.compare_exchange_strong(locked, true)) {
      locked = false;
      __builtin_ia32_pause();
    }
  }
  void Acquire() { Lock(); }

  void Unlock() {
    lock.store(false);
  }
  void Release() { Unlock(); }
};

template <typename T>
class Guard {
  T &t;
 public:
  Guard(T &t) : t(t) { t.Acquire(); }
  ~Guard() { t.Release(); }
};

template <class... M>
class MixIn : public M... {};

// instance of a global object. So that we don't need the ugly extern.
template <class O> O &Instance() noexcept;

template <class O>
struct InstanceInit {
  static constexpr bool kHasInstance = false;
  InstanceInit() {
    Instance<O>();
  }
};

template <class O> O &Instance() noexcept
{
  if constexpr (InstanceInit<O>::kHasInstance) {
    return *(InstanceInit<O>::instance);
  } else {
    static O o;
    return o;
  }
}

// Interface implementation. The real implementation usually is in iface.cc
template <class IFace> IFace &Impl() noexcept;

#define IMPL(IFace, ImplClass) \
  template<> IFace &Impl() noexcept { return Instance<ImplClass>(); }

template <typename T, typename ...Args>
class BaseFactory {
 public:
  typedef std::vector<std::function<T *(Args...)>> Table;
 protected:
  static Table table;
  static void AddToTable(std::function<T *(Args...)> f) {
    table.push_back(f);
  }
 public:
  static T *Create(int n, Args... args) {
    return table[n](args...);
  }
};

template <typename T, typename ...Args>
typename BaseFactory<T, Args...>::Table BaseFactory<T, Args...>::table;

template <typename T, int LastEnum, typename ...Args>
class Factory : public Factory<T, LastEnum - 1, Args...> {
  typedef Factory<T, LastEnum - 1, Args...> Super;
 public:
  static void Initialize() {
    Super::Initialize();
    Super::AddToTable([](Args... args) {
        return Factory<T, LastEnum - 1, Args...>::Construct(args...);
      });
  }
  static T *Construct(Args ...args);
};

template <typename T, typename ...Args>
class Factory<T, 0, Args...> : public BaseFactory<T, Args...> {
 public:
  static void Initialize() {}
  static T *Construct(Args ...args);
};

// CPU pinning
static inline void PinToCPU(std::vector<int> cpus)
{
  // linux only
  cpu_set_t set;
  CPU_ZERO(&set);
  for (auto cpu : cpus) {
    CPU_SET(cpu % sysconf(_SC_NPROCESSORS_CONF), &set);
  }
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &set);
  pthread_yield();
}

static inline void PinToCPU(int cpu) { PinToCPU(std::vector<int>{cpu}); }

static inline size_t Align(size_t x, size_t a = 16)
{
  return a * ((x - 1) / a + 1);
}

// Typesafe get from a variadic arguments
template <int Index, typename U, typename ...Args>
struct GetArg : public GetArg<Index - 1, Args...> {
  GetArg(const U &first, const Args&... rest) : GetArg<Index - 1, Args...>(rest...) {}
};

template <typename U, typename ...Args>
struct GetArg<0, U, Args...> {
  U value;
  GetArg(const U &value, const Args&... drop) : value(value) {}
};

template <typename ValueType> using Optional = std::optional<ValueType>;
template <typename ValueType> using Ref = std::reference_wrapper<ValueType>;
template <typename ValueType> using OwnPtr = std::unique_ptr<ValueType>;
}

#endif /* UTIL_H */
