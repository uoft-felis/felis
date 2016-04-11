// -*- c++ -*-

#ifndef SQLTYPES_H
#define SQLTYPES_H

#include <iostream>
#include <string>
#include <cstring>
#include <cassert>

namespace sql {

// types that looks like a built-in C++ type, let's keep them all in lower-case!
// copied from Silo.

// equivalent to VARCHAR(N)
template <typename IntSizeType, unsigned int N>
class inline_str_base {
public:
  inline_str_base() : sz(0) {}

  inline_str_base(const char *s) {
    assign(s);
  }

  inline_str_base(const char *s, size_t n) {
    assign(s, n);
  }

  inline_str_base(const std::string &s) {
    assign(s);
  }

  inline_str_base(const inline_str_base &that) : sz(that.sz) {
    memcpy(&buf[0], &that.buf[0], sz);
  }

  inline_str_base &
  operator=(const inline_str_base &that) {
    if (this == &that)
      return *this;
    sz = that.sz;
    memcpy(&buf[0], &that.buf[0], sz);
    return *this;
  }

  size_t
  max_size() const {
    return N;
  }

  const char * c_str() const {
    buf[sz] = 0;
    return &buf[0];
  }

  inline std::string str(bool zeropad = false) const {
    if (zeropad) {
      assert(N >= sz);
      std::string r(N, 0);
      memcpy((char *) r.data(), &buf[0], sz);
      return r;
    } else {
      return std::string(&buf[0], sz);
    }
  }

  const char *data() const {
    return &buf[0];
  }

  size_t size() const {
    return sz;
  }

  void assign(const char *s) {
    assign(s, strlen(s));
  }

  void assign(const char *s, size_t n) {
    assert(n <= N);
    memcpy(&buf[0], s, n);
    sz = n;
  }

  void assign(const std::string &s) {
    assign(s.data(), s.size());
  }

  void resize(size_t n, char c = 0) {
    assert(n <= N);
    if (n > sz)
      memset(&buf[sz], c, n - sz);
    sz = n;
  }

  void resize_junk(size_t n) {
    assert(n <= N);
    sz = n;
  }

  bool operator==(const inline_str_base &other) const {
    return memcmp(buf, other.buf, sz) == 0;
  }

  bool operator!=(const inline_str_base &other) const {
    return !operator==(other);
  }

private:
  IntSizeType sz;
  mutable char buf[N + 1];
} __attribute__((packed));

template <typename IntSizeType, unsigned int N>
inline std::ostream &
operator<<(std::ostream &o, const inline_str_base<IntSizeType, N> &s)
{
  o << std::string(s.data(), s.size());
  return o;
}

template <unsigned int N>
class inline_str_8 : public inline_str_base<uint8_t, N> {
  typedef inline_str_base<uint8_t, N> super_type;
public:
  inline_str_8() : super_type() {}
  inline_str_8(const char *s) : super_type(s) {}
  inline_str_8(const char *s, size_t n) : super_type(s, n) {}
  inline_str_8(const std::string &s) : super_type(s) {}
} __attribute__((packed));

template <unsigned int N>
class inline_str_16 : public inline_str_base<uint16_t, N> {
  typedef inline_str_base<uint16_t, N> super_type;
public:
  inline_str_16() : super_type() {}
  inline_str_16(const char *s) : super_type(s) {}
  inline_str_16(const char *s, size_t n) : super_type(s, n) {}
  inline_str_16(const std::string &s) : super_type(s) {}
} __attribute__((packed));

// equiavlent to CHAR(N)
template <unsigned int N, char FillChar = ' '>
class inline_str_fixed {
public:
  inline_str_fixed() {
    memset(&buf[0], FillChar, N);
  }

  inline_str_fixed(const char *s) {
    assign(s, strlen(s));
  }

  inline_str_fixed(const char *s, size_t n) {
    assign(s, n);
  }

  inline_str_fixed(const std::string &s) {
    assign(s.data(), s.size());
  }

  inline_str_fixed(const inline_str_fixed &that) {
    memcpy(&buf[0], &that.buf[0], N);
  }

  inline_str_fixed &
  operator=(const inline_str_fixed &that) {
    if (this == &that)
      return *this;
    memcpy(&buf[0], &that.buf[0], N);
    return *this;
  }

  std::string str() const {
    return std::string(&buf[0], N);
  }

  const char * data() const {
    return &buf[0];
  }

  size_t size() const {
    return N;
  }

  void assign(const char *s) {
    assign(s, strlen(s));
  }

  inline void assign(const char *s, size_t n) {
    assert(n <= N);
    memcpy(&buf[0], s, n);
    if ((N - n) > 0) // to suppress compiler warning
      memset(&buf[n], FillChar, N - n); // pad with spaces
  }

  void assign(const std::string &s) {
    assign(s.data(), s.size());
  }

  bool operator==(const inline_str_fixed &other) const {
    return memcmp(buf, other.buf, N) == 0;
  }

  bool operator!=(const inline_str_fixed &other) const {
    return !operator==(other);
  }

private:
  char buf[N];
} __attribute__((packed));

template <unsigned int N, char FillChar>
inline std::ostream &
operator<<(std::ostream &o, const inline_str_fixed<N, FillChar> &s)
{
  o << std::string(s.data(), s.size());
  return o;
}

// Schemas Wrapper.
// Because schemas is a POJO, we need to add less than operator to this POJO.
// However, with a combined key, we have to compare field by field. Moreover, on
// x86 architecture, memcmp() won't work for integer types at all.

template <typename T, typename ...Targs>
struct Compare {
  // less than: -1. equal: 0. larger than: 1
  static inline int CompareTo(const uint8_t *p, const uint8_t *q) {
    int ret = Compare<T>::CompareTo(p, q);
    if (ret != 0)
      return ret;
    else
      return Compare<Targs...>::LessThan(p + sizeof(T), q + sizeof(T));
  }
};

template <typename T>
struct Compare<T> {
  static inline int CompareTo(const uint8_t *p, const uint8_t *q) {
    const T *a = (const T *) p;
    const T *b = (const T *) q;
    if (*a == *b)
      return 0;
    else if (*a < *b)
      return -1;
    else
      return 1;
  }
};

template <typename T, typename ...Targs>
struct Serializer {
  static size_t EncodeSize(const uint8_t *ptr) {
    return Serializer<T>::EncodeSize(ptr)
      + Serializer<Targs...>::EncodeSize(ptr + sizeof(T));
  }
  static constexpr size_t DecodeSize() {
    return sizeof(T) + Serializer<Targs...>::DecodeSize();
  }
  static void Encode(uint8_t *buf, const uint8_t *ptr) {
    Serializer<T>::Encode(buf, ptr);
    Serializer<Targs...>::Encode(buf + Serializer<T>::EncodeSize(ptr), ptr + sizeof(T));
  }
  static void DecodeFrom(uint8_t *ptr, const uint8_t *buf) {
    Serializer<T>::DecodeFrom(ptr, buf);
    Serializer<Targs...>::DecodeFrom(ptr + sizeof(T), buf + Serializer<T>::EncodeSize(ptr));
  }
};

template <typename T>
struct BasicSerializer {
  static size_t EncodeSize(const uint8_t *ptr) { return sizeof(T); }
  static constexpr size_t DecodeSize() { return sizeof(T); }

  static void Encode(uint8_t *buf, const uint8_t *ptr) {
    memcpy(buf, ptr, EncodeSize(ptr));
  }
  static void DecodeFrom(uint8_t *ptr, const uint8_t *buf) {
    memcpy(ptr, buf, sizeof(T));
  }
};

template <typename SizeType, unsigned int N>
struct Serializer<inline_str_base<SizeType, N>>
  : public BasicSerializer<inline_str_base<SizeType, N>> {
  typedef inline_str_base<SizeType, N> ObjectType;
  static size_t EncodeSize(const uint8_t *ptr) {
    auto p = (const ObjectType *) ptr;
    return sizeof(SizeType) + p->size();
  }
  static void Encode(uint8_t *buf, const uint8_t *ptr) {
    auto p = (const ObjectType *) ptr;
    SizeType sz = p->size();
    Serializer<SizeType>::Encode(buf, (const uint8_t *) &sz);
    memcpy(buf + Serializer<SizeType>::EncodeSize((const uint8_t *) &sz),
	   p->data(), p->size());
  }
  static void DecodeFrom(uint8_t *ptr, const uint8_t *buf) {
    auto p = (ObjectType *) ptr;
    SizeType sz;
    Serializer<SizeType>::DecodeFrom((uint8_t *) &sz, buf);
    p->assign((const char *) buf + Serializer<SizeType>::EncodeSize((const uint8_t *) &sz), sz);
  }
};

template<>
struct Serializer<uint16_t> : public BasicSerializer<uint16_t> {
  static void Encode(uint8_t *buf, const uint8_t *ptr) {
    uint16_t be = htobe16(*(const uint16_t *) ptr); // has to be BE!
    memcpy(buf, &be, EncodeSize(ptr));
  }
  static void DecodeFrom(uint8_t *ptr, const uint8_t *buf) {
    uint16_t h = be16toh(* (const uint16_t *) buf);
    memcpy(ptr, &h, sizeof(uint16_t));
  }
};

template <>
struct Serializer<uint32_t> : public BasicSerializer<uint32_t> {
  static void Encode(uint8_t *buf, const uint8_t *ptr) {
    uint32_t be = htobe32(*(const uint32_t *) ptr); // has to be BE!
    memcpy(buf, &be, EncodeSize(ptr));
  }
  static void DecodeFrom(uint8_t *ptr, const uint8_t *buf) {
    uint32_t h = be32toh(* (const uint32_t *) buf);
    memcpy(ptr, &h, sizeof(uint32_t));
  }
};

// host integer for serializer. Won't be matched to uintxx_t
template <typename T>
struct hint {
  T val;
  operator int16_t() const { return val; }
  operator int32_t() const { return val; }
  operator int64_t() const { return val; }

  operator uint16_t() const { return val; }
  operator uint32_t() const { return val; }
  operator uint64_t() const { return val; }
} __attribute__((packed));

struct hint16 : public hint<uint16_t> {
  hint16() { val = 0; }
  hint16(const int16_t& rhs) { val = rhs; }
  // hint16(const uint16_t& rhs) { val = rhs; }
} __attribute__((packed));

struct hint32 : public hint<uint32_t> {
  hint32() { val = 0; }
  hint32(const int32_t& rhs) { val = rhs; }
  hint32(const uint32_t& rhs) { val = rhs; }
} __attribute__((packed));

template <typename T>
struct Serializer<T> : public BasicSerializer<T> {};

/*
template <unsigned int N>
struct Serializer<sql::inline_str_fixed<N>> {
  static size_t EncodeSize() { return N; }
  static void Encode(uint8_t *buf, const uint8_t *ptr) {
    memcpy(buf, ptr, EncodeSize());
  }
  static void DecodeFrom(uint8_t *ptr, const uint8_t *buf) {
    memcpy(ptr, buf, N);
  }
};
*/

// serve as the key of database
struct VarStr {

  static size_t NewSize(uint16_t length) { return sizeof(VarStr) + length; }

  static VarStr *New(uint16_t length) {
    VarStr *ins = (VarStr *) malloc(NewSize(length));
    ins->len = length;
    return ins;
  }

  static VarStr *FromAlloca(void *ptr, uint16_t length) {
    VarStr *str = static_cast<VarStr *>(ptr);
    str->len = length;
    return str;
  }

  uint16_t len;
  uint8_t data[];
};

template <typename T, typename ...Targs>
class Schemas : public T,
		public Compare<Targs...>,
		public Serializer<Targs...> {
public:
  using T::T;
  Schemas(Targs... args) : T {args...} {}
  Schemas() : T{} {}

  bool operator<(const T &rhs) const {
    return Compare<Targs...>::CompareTo((const uint8_t *) this,
					(const uint8_t *) &rhs) < 0;
  }

  bool operator==(const T &rhs) const {
    return Compare<Targs...>::CompareTo((const uint8_t *) this,
					(const uint8_t *) &rhs) == 0;
  }

  void Encode(uint8_t *buf) const {
    Serializer<Targs...>::Encode(buf, (const uint8_t *) this);
  }

  VarStr *Encode(VarStr *str) const {
    Encode(str->data);
    return str;
  }

  VarStr *Encode() const { return Encode(VarStr::New(EncodeSize())); }

  size_t EncodeSize() const {
    return Serializer<Targs...>::EncodeSize((const uint8_t *) this);
  }

  void DecodeFrom(const uint8_t *buf) {
    Serializer<Targs...>::DecodeFrom((uint8_t *) this, buf);
  }

  void DecodeFrom(const VarStr *str) {
    DecodeFrom(str->data);
  }
};


}

#endif /* SQLTYPES_H */
