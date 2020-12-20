#ifndef VARSTR_H
#define VARSTR_H

#include <cstdlib>
#include <string>
#include <cstring>

#include "mem.h"
#include "spdlog/fmt/fmt.h"

#include "felis_probes.h"

namespace felis {

class VarStrView final {
  static void *operator new(size_t s) { std::abort(); } // No, you cannot new this class.
  static void operator delete(void *) { std::abort(); } // No, you cannot delete this class.
  uint64_t w;
 public:
  VarStrView() : w(0) {}
  VarStrView(uint16_t len, const uint8_t *data) {
    w = ((uintptr_t) data << 16) | len;
  }

  uint16_t length() const { return 0x00ff & w; }
  uint8_t *data() const { return (uint8_t *) (w >> 16); }

  uint8_t operator[](size_t idx) const {
    return *(data() + idx);
  }

  bool operator<(const VarStrView &rhs) const {
    return length() == rhs.length() ?
        memcmp(data(), rhs.data(), length()) < 0 : length() < rhs.length();
  }

  bool operator==(const VarStrView &rhs) const {
    if (length() != rhs.length()) return false;
    return memcmp(data(), rhs.data(), length()) == 0;
  }

  bool operator!=(const VarStrView &rhs) const {
    return !(*this == rhs);
  }

  std::string ToHex() const {
    fmt::memory_buffer buf;
    for (int i = 0; i < length(); i++) {
      fmt::format_to(buf, "{:x} ", data()[i]);
    }
    return std::string(buf.data(), buf.size());
  }

  template <typename T>
  const T ToType() const {
    T instance;
    instance.DecodeView(*this);
    return instance;
  }
};

// serve as the key/value of database
class VarStr final {
  uint16_t len;
  int16_t region_id;
  VarStr() {}
 public:
  VarStr(const VarStr &rhs) = delete;
  VarStr(VarStr &&rhs) = delete;

  static size_t NewSize(uint16_t length) { return sizeof(VarStr) + length; }

  static VarStr *New(uint16_t length) {
    int region_id = mem::ParallelPool::CurrentAffinity();
    VarStr *ins = (VarStr *) mem::GetDataRegion(true).Alloc(NewSize(length));
    //shirley: probe
    //probes::RegionPoolVarstr{(long long)(length + sizeof(VarStr))}();
    ins->len = length;
    ins->region_id = region_id;
    // ins->p = (uint8_t *) ins + sizeof(VarStr);
    return ins;
  }

  static void operator delete(void *ptr) {
    if (ptr == nullptr) return;
    VarStr *ins = (VarStr *) ptr;
    // shirley: probe
    // probes::RegionPoolVarstr{(-1 * (long long)(sizeof(VarStr) +
    // ins->len))}();
    mem::GetDataRegion(true).Free(ptr, ins->region_id, sizeof(VarStr) + ins->len);
  }

  static VarStr *FromPtr(void *ptr, uint16_t length) {
    VarStr *str = static_cast<VarStr *>(ptr);
    str->len = length;
    str->region_id = -5206; // something peculiar, making you realize it's allocated adhoc.
    // str->p = (uint8_t *) ptr;
    return str;
  }

  const uint8_t *data() const {
    return (const uint8_t *) this + sizeof(VarStr);
  }

  uint8_t *data() {
    return (uint8_t *) this + sizeof(VarStr);
  }

  uint16_t length() const { return len; }

  template <typename T>
  const T ToType() const {
    T instance;
    instance.Decode(this);
    return instance;
  }

  std::string ToHex() const {
    return ToView().ToHex();
  }

  VarStrView ToView() const {
    return VarStrView(len, data());
  }
};

}

#endif
