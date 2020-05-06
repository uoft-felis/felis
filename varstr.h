#ifndef VARSTR_H
#define VARSTR_H

#include <cstdlib>
#include <string>
#include <cstring>

#include "mem.h"
#include "util.h"
#include "spdlog/fmt/fmt.h"

namespace felis {

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
  uint8_t inherit;
  int region_id;
  const uint8_t *data;

  VarStr() : len(0), inherit(0), region_id(0), data(nullptr) {}
  VarStr(uint16_t len, int region_id, const uint8_t *data) : len(len), inherit(0), region_id(region_id), data(data) {}

  bool operator<(const VarStr &rhs) const {
    if (data == nullptr) return true;
    else if (rhs.data == nullptr) return false;

    return len == rhs.len ? memcmp(data, rhs.data, len) < 0 : len < rhs.len;
  }

  bool operator==(const VarStr &rhs) const {
    if (len != rhs.len) return false;
    return memcmp(data, rhs.data, len) == 0;
  }

  bool operator!=(const VarStr &rhs) const {
    return !(*this == rhs);
  }

  VarStr *InspectBaseInheritPointer() const {
    if (inherit > 1) std::abort();
    if (inherit && len >= sizeof(VarStr *)) return (VarStr *) (*(uintptr_t *) data);
    return nullptr;
  }

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
};

}

#endif
