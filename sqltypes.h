// -*- C++ -*-

#ifndef SQLTYPES_H
#define SQLTYPES_H

#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>
#include <cstring>
#include <cassert>

#include "mem.h"
#include "util.h"

namespace sql {

// serve as the key of database
struct VarStr {

  static size_t NewSize(uint16_t length) { return sizeof(VarStr) + length; }

  static VarStr *New(uint16_t length) {
    int region_id = mem::CurrentAllocAffinity();
    VarStr *ins = (VarStr *) mem::GetThreadLocalRegion(region_id).Alloc(NewSize(length));
    ins->len = length;
    ins->region_id = region_id;
    ins->data = (uint8_t *) ins + sizeof(VarStr);
    return ins;
  }

  static void operator delete(void *ptr) {
    if (ptr == nullptr) return;
    VarStr *ins = (VarStr *) ptr;
    auto &r = mem::GetThreadLocalRegion(ins->region_id);
    if (__builtin_expect(ins->data == (uint8_t *) ptr + sizeof(VarStr), 1)) {
      r.Free(ptr, sizeof(VarStr) + ins->len);
    } else {
      r.Free(ptr, sizeof(VarStr)); // don't know who's gonna do that
    }
  }

  static VarStr *FromAlloca(void *ptr, uint16_t length) {
    VarStr *str = static_cast<VarStr *>(ptr);
    str->len = length;
    str->data = (uint8_t *) str + sizeof(VarStr);
    return str;
  }

  uint16_t len;
  int region_id;
  const uint8_t *data;

  VarStr() : len(0), region_id(0), data(nullptr) {}
  VarStr(uint16_t len, int region_id, uint8_t *data) : len(len), region_id(region_id), data(data) {}

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

  template <typename T>
  const T ToType() const {
    T instance;
    instance.Decode(this);
    return instance;
  }

  std::string ToHex() const {
    char buf[8];
    std::stringstream ss;
    for (int i = 0; i < len; i++) {
      snprintf(buf, 8, "%x ", data[i]);
      ss << buf;
    }
    return ss.str();
  }
};


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
};

template <unsigned int N>
using inline_str_8 = inline_str_base<uint8_t, N>;

template <unsigned int N>
using inline_str_16 = inline_str_base<uint16_t, N>;

// equiavlent to CHAR(N)
template <unsigned int N, char FillChar = ' '>
class Char {
 public:
  Char() {
    memset(&buf[0], FillChar, N);
  }

  Char(const char *s) {
    assign(s, strlen(s));
  }

  Char(const char *s, size_t n) {
    assign(s, n);
  }

  Char(const std::string &s) {
    assign(s.data(), s.size());
  }

  Char(const Char &that) {
    memcpy(&buf[0], &that.buf[0], N);
  }

  Char &
  operator=(const Char &that) {
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

  bool operator==(const Char &other) const {
    return memcmp(buf, other.buf, N) == 0;
  }

  bool operator!=(const Char &other) const {
    return !operator==(other);
  }

 private:
  char buf[N];
};

// Schemas Wrapper.
// Because schemas is a POJO, we need to add less than operator to this POJO.
// However, with a combined key, we have to compare field by field. Moreover, on
// x86 architecture, memcmp() won't work for integer types at all.

template <typename T>
struct Serializer {
  static size_t EncodeSize(const T *ptr) { return sizeof(T); }

  static void EncodeTo(uint8_t *buf, const T *ptr) {
    memcpy(buf, ptr, EncodeSize(ptr));
  }
  static void DecodeFrom(T *ptr, const uint8_t *buf) {
    memcpy(ptr, buf, sizeof(T));
  }
};

template <typename SizeType, unsigned int N>
struct Serializer<inline_str_base<SizeType, N>> {
  typedef inline_str_base<SizeType, N> ObjectType;
  static size_t EncodeSize(const ObjectType *p) {
    return sizeof(SizeType) + p->size();
  }
  static void EncodeTo(uint8_t *buf, const ObjectType *p) {
    SizeType sz = p->size();
    Serializer<SizeType>::EncodeTo(buf, (const SizeType *) &sz);
    memcpy(buf + Serializer<SizeType>::EncodeSize((const SizeType *) &sz),
	   p->data(), p->size());
  }
  static void DecodeFrom(ObjectType *p, const uint8_t *buf) {
    SizeType sz;
    Serializer<SizeType>::DecodeFrom((SizeType *) &sz, buf);
    p->assign((const char *) buf + Serializer<SizeType>::EncodeSize((const SizeType *) &sz), sz);
  }
};

template <typename T>
struct KeySerializer : public Serializer<T> {};

template <typename T>
struct ValueSerializer : public Serializer<T> {};

template<>
struct KeySerializer<uint16_t> : public Serializer<uint16_t> {
  static void EncodeTo(uint8_t *buf, const uint16_t *ptr) {
    uint16_t be = htobe16(*ptr); // has to be BE!
    memcpy(buf, &be, EncodeSize(ptr));
  }
  static void DecodeFrom(uint16_t *ptr, const uint8_t *buf) {
    uint16_t h = be16toh(*buf);
    *ptr = h;
  }
};

template <>
struct KeySerializer<uint32_t> : public Serializer<uint32_t> {
  static void EncodeTo(uint8_t *buf, const uint32_t *ptr) {
    uint32_t be = htobe32(*ptr); // has to be BE!
    memcpy(buf, &be, EncodeSize(ptr));
  }
  static void DecodeFrom(uint32_t *ptr, const uint8_t *buf) {
    uint32_t h = be32toh(*(const uint32_t *) buf);
    *ptr = h;
  }
};

template <typename Base>
class Object : public Base {
 public:
  using Base::Base;

  Object(const Base &b) : Base(b) {}

  VarStr *Encode() const { return EncodeVarStr(VarStr::New(this->EncodeSize())); }

  VarStr *EncodeFromAlloca(void *base_ptr) const {
    return EncodeVarStr(VarStr::FromAlloca(base_ptr, this->EncodeSize()));
  }

  VarStr *EncodeFromRoutine() const {
    void *base_ptr = mem::AllocFromRoutine(VarStr::NewSize(this->EncodeSize()));
    return EncodeVarStr(VarStr::FromAlloca(base_ptr, this->EncodeSize()));
  }
  void Decode(const VarStr *str) {
    this->DecodeFrom(str->data);
  }
 private:
  VarStr *EncodeVarStr(VarStr *str) const {
    this->EncodeTo((uint8_t *) str + sizeof(VarStr));
    return str;
  }
};

template <typename Base>
struct Serializer<Object<Base>> {
  static size_t EncodeSize(const Object<Base> *ptr) {
    return ptr->EncodeSize();
  }
  static void EncodeTo(uint8_t *buf, const Object<Base> *ptr) {
    ptr->EncodeTo(buf);
  }
  static void DecodeFrom(Object<Base> *ptr, const uint8_t *buf) {
    ptr->DecodeFrom(buf);
  }
};

template <int N> class FieldValue {};

template <template <typename> class FieldSerializer, int N>
class Field : public Field<FieldSerializer, N - 1>, public FieldValue<N> {
  typedef Field<FieldSerializer, N - 1> PreviousFields;
  typedef typename FieldValue<N>::Type ImplType;
  typedef FieldSerializer<ImplType> Impl;
  ImplType *pointer() { return FieldValue<N>::ptr(); }
  const ImplType *pointer() const { return FieldValue<N>::ptr(); }

 protected:
  static constexpr int kFieldOffset = PreviousFields::kFieldOffset + 1;
  Field() {}

  template <int K>
  using FieldType = Field<FieldSerializer, K>;

 public:
  static constexpr int kOffset = N;


  template <typename T>
  struct FieldBuilder : public FieldValue<N>::template Builder<typename Field<FieldSerializer, N + 1>::template FieldBuilder<T>, T> {};

  size_t EncodeSize() const {
    return PreviousFields::EncodeSize() + Impl::EncodeSize(pointer());
  }
  uint8_t *EncodeTo(uint8_t *buf) const {
    buf = PreviousFields::EncodeTo(buf);
    Impl::EncodeTo(buf, pointer());
    return buf + Impl::EncodeSize(pointer());
  }
  const uint8_t *DecodeFrom(const uint8_t *buf) {
    buf = PreviousFields::DecodeFrom(buf);
    Impl::DecodeFrom(pointer(), buf);
    return buf + Impl::EncodeSize(pointer());
  }
};

// Gap Fields. For example, 0 is the first gap fields

template <template <typename> class FieldSerializer>
class GapField {
 protected:
  static constexpr int kFieldOffset = -1;

 public:
  template <typename T>
  struct FieldBuilder {
    T *obj;
    Object<T> Done() { return *obj; }
    void Init() {}
  };

  size_t EncodeSize() const { return 0; }
  uint8_t *EncodeTo(uint8_t *buf) const { return buf; }
  const uint8_t *DecodeFrom(const uint8_t *buf) { return buf; }
};

// First Gap
template <template <typename> class FieldSerializer>
class Field<FieldSerializer, __COUNTER__> : public GapField<FieldSerializer> {};

#define FIELD(field_type, field_name)                                   \
  template <> struct FieldValue<__COUNTER__> { \
    field_type field_name; typedef field_type Type;                     \
    Type *ptr() { return &field_name;}                                  \
    const Type *ptr() const { return &field_name;}                      \
    template <typename NextBuilder, typename T> struct Builder {        \
      T *obj;                                                           \
      NextBuilder _(field_type field_name) { this->obj->field_name = field_name; NextBuilder b; b.obj = obj; return b;} \
      template <typename ...Args> void Init(field_type field_name, Args... args) { _(field_name).Init(args...); } \
    };                                                                  \
  };                                                                    \

#define DBOBJ(name, serializer)                                         \
  using name = sql::Schemas<sql::Field<serializer, __COUNTER__ - 1>>;   \
  template <template <typename> class FieldSerializer>                  \
  class Field<FieldSerializer, name::kOffset + 1> : public GapField<FieldSerializer> {}; \

#define KEYS(name) DBOBJ(name, KeySerializer)
#define VALUES(name) DBOBJ(name, ValueSerializer)

// Serializable tuples. Tuples are different than fields, because their
// members are anonymous.

template <int N, template <typename ...> class TupleField, typename T, typename ...Types>
struct TupleFieldType {
  typedef typename TupleFieldType<N - 1, TupleField, Types...>::Type Type;
  typedef typename TupleFieldType<N - 1, TupleField, Types...>::ValueType ValueType;
};

template <template <typename ...> class TupleField, typename T, typename ...Types>
struct TupleFieldType<0, TupleField, T, Types...> {
  typedef TupleField<T, Types...> Type;
  typedef T ValueType;
};

template <typename T, typename ...Types>
struct TupleField : public TupleField<Types...> {
  T value;
  typedef TupleField<Types...> ParentTupleFields;
  TupleField() : ParentTupleFields() {}
  TupleField(const T &v, const Types&... args) : value(v), ParentTupleFields(args...) {}

  void Unpack(T &v, Types&... args) const {
    v = value;
    ParentTupleFields::Unpack(args...);
  }

  size_t EncodeSize() const {
    return ParentTupleFields::EncodeSize() + Serializer<T>::EncodeSize(&value);
  }
  uint8_t *EncodeTo(uint8_t *buf) const {
    buf = ParentTupleFields::EncodeTo(buf);
    Serializer<T>::EncodeTo(buf, &value);
    return buf + Serializer<T>::EncodeSize(&value);
  }
  const uint8_t *DecodeFrom(const uint8_t *buf) {
    buf = ParentTupleFields::DecodeFrom(buf);
    Serializer<T>::DecodeFrom(&value, buf);
    return buf + Serializer<T>::EncodeSize(&value);
  }
};

template <typename T>
struct TupleField<T> {
  T value;
  TupleField() {}
  TupleField(const T &v) : value(v) {}

  void Unpack(T &v) const { v = value; }

  size_t EncodeSize() const {
    return Serializer<T>::EncodeSize(&value);
  }
  uint8_t *EncodeTo(uint8_t *buf) const {
    Serializer<T>::EncodeTo(buf, &value);
    return buf + Serializer<T>::EncodeSize(&value);
  }
  const uint8_t *DecodeFrom(const uint8_t *buf) {
    Serializer<T>::DecodeFrom(&value, buf);
    return buf + Serializer<T>::EncodeSize(&value);
  }
};

template <typename ...Types>
class TupleImpl : public TupleField<Types...> {
 public:
  using TupleField<Types...>::TupleField;

  TupleImpl(const TupleField<Types...> &rhs) : TupleField<Types...>(rhs) {}

  template <int N>
  typename TupleFieldType<N, TupleField, Types...>::ValueType _() const {
    return ((const typename TupleFieldType<N, TupleField, Types...>::Type *) this)->value;
  }
};


template <typename AllFields>
class SchemasImpl : public AllFields {
 public:
  // Concept:
  // void EncodeTo(uint8_t *buf) const;
  // size_t EncodeSize() const;
  // void DecodeFrom(const uint8_t *buf);

  SchemasImpl() {}

  using ThisType = SchemasImpl<AllFields>;
  using FirstBuilder = typename AllFields::template FieldType<AllFields::kOffset - AllFields::kFieldOffset>::template FieldBuilder<ThisType>;

  template <typename ...Args>
  static Object<ThisType> New(Args... args) {
    ThisType o;
    o.Build().Init(args...);
    return o;
  }

  FirstBuilder Build() {
    FirstBuilder b;
    b.obj = this;
    return b;
  }
};

template <typename AllFields> using Schemas = Object<SchemasImpl<AllFields>>;
template <typename ...Types> using Tuple = Object<TupleImpl<Types...>>;

}

namespace felis {

using VarStr = sql::VarStr;

}

#endif /* SQLTYPES_H */
