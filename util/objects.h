#ifndef UTIL_OBJECTS_H
#define UTIL_OBJECTS_H

namespace util {

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

}

#endif
