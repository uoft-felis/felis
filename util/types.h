#ifndef UTIL_TYPES_H
#define UTIL_TYPES_H

#include <optional>

namespace util {
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

#endif
