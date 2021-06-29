#ifndef UTIL_FACTORY_H
#define UTIL_FACTORY_H
#include <utility>

namespace util {

template <typename EnumType, EnumType Enum> struct FactoryTag {};

template <typename T, typename EnumType, EnumType LastEnum, typename ...Args>
class Factory {
  struct Callable { virtual T *operator()(Args...) = 0; };
  template <EnumType Enum>
  struct Construct final : public Callable {
    T *operator()(Args ...args) {
      return new typename FactoryTag<EnumType, Enum>::Type(args...);
    }
  };

  static inline Callable *table[int(LastEnum)];
  template <std::size_t ...Ints>
  static void InitializeTable(std::integer_sequence<std::size_t, Ints...> seq) {
    ((table[Ints] = new Construct<EnumType(Ints)>()), ...);
  }
 public:
  static void Initialize() {
    InitializeTable(std::make_index_sequence<int(LastEnum)>());
  }
  static T *Create(EnumType n, Args... args) {
    return table[int(n)]->operator()(args...);
  }
};

}

#endif
