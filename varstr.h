#ifndef VARSTR_H
#define VARSTR_H

#include <cstdlib>
#include <string>
#include <cstring>

#include "mem.h"
#include "spdlog/fmt/fmt.h"

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
struct VarStr {

  static size_t NewSize(uint16_t length) { return sizeof(VarStr) + length; }

  static VarStr *New(uint16_t length) {
    int region_id = mem::ParallelPool::CurrentAffinity();
    VarStr *ins = (VarStr *) mem::GetDataRegion().Alloc(NewSize(length));
    ins->len = length;
    ins->region_id = region_id;
    ins->data = (uint8_t *) ins + sizeof(VarStr);
    return ins;
  }

  static void operator delete(void *ptr) {
    if (ptr == nullptr) return;
    VarStr *ins = (VarStr *) ptr;
    if (__builtin_expect(ins->data == (uint8_t *) ptr + sizeof(VarStr), 1)) {
      mem::GetDataRegion().Free(ptr, ins->region_id, sizeof(VarStr) + ins->len);
    } else {
      // Don't know who's gonna do that. Looks like it's a free from stack?!
      std::abort();
    }
  }

  static VarStr *FromPtr(void *ptr, uint16_t length) {
    VarStr *str = static_cast<VarStr *>(ptr);
    str->len = length;
    str->region_id = -5206; // something peculiar, making you realize it's allocated adhoc.
    str->data = (uint8_t *) str + sizeof(VarStr);
    return str;
  }

  uint16_t len;
  int region_id;
  const uint8_t *data;

  VarStr() : len(0), region_id(0), data(nullptr) {}
  VarStr(uint16_t len, int region_id, const uint8_t *data) : len(len), region_id(region_id), data(data) {}

  template <typename T>
  const T ToType() const {
    T instance;
    instance.Decode(this);
    return instance;
  }

  std::string ToHex() const {
    fmt::memory_buffer buf;
    for (int i = 0; i < len; i++) {
      fmt::format_to(buf, "{:x} ", data[i]);
    }
    return std::string(buf.data(), buf.size());
  }

  VarStrView ToView() const {
    return VarStrView(len, data);
  }
};

}

#endif
