#ifndef UTIL_FACTORY_H
#define UTIL_FACTORY_H
#include <vector>

namespace util {

template <typename T, typename ...Args>
class BaseFactory {
 public:
  // Don't use std::function. Complation speed is very slow.
  struct Callable { virtual T *operator()(Args...) = 0; };
  template <typename F> struct GenericCallable : public Callable {
    F f;
    GenericCallable(F f) : f(f) {}
    T *operator()(Args... args) override final {
      return f(args...);
    }
  };
  typedef std::vector<Callable *> Table;
 protected:
  static Table table;
  template <typename F>
  static void AddToTable(F f) {
    table.push_back(new GenericCallable<F>(f));
  }
 public:
  static T *Create(int n, Args... args) {
    return (*table[n])(args...);
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

}

#endif
