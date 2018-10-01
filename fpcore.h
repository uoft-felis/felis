#ifndef FPCORE_H
#define FPCORE_H

#include <cstdlib>
#include <cstring>
#include <tuple>
#include <vector>
#include <map>
#include <functional>
#include <type_traits>
#include "gopp/gopp.h"
#include "util.h"

namespace felis {

static inline void *Allocate(size_t s)
{
#if 0
  auto r = go::Scheduler::Current()->current_routine();
  uint8_t *p = (uint8_t *) r->userdata();
  s = util::Align(s, 8);
  r->set_userdata(p + s);
  return p;
#endif
  return malloc(s);
}

// some immutable data structures

// Why we need these immutable data structures?
// 1. Enforce Functional Programming
// 2. Most of data structures are pretty small, it makes sense to do a complete copy for cache friendliness.

template <typename ...T>
using Tuple = std::tuple<T...>;

template <typename Impl, typename T>
class Sequence {
 public:
  using Type = T;
  using SequenceType = Impl;

  SequenceType sequence() const { return *(Impl *) this; }
};

template <typename T>
class Lazy : public Sequence<Lazy<T>, T> {
  T o;
  bool valid;
 public:
  Lazy(const T &rhs) : o(rhs), valid(true) {}

  bool IsValid() const {
    return valid;
  }
  void Next() {
    valid = false;
  }
  T Deref() {
    return o;
  }
};

template <typename T>
class Vector {
  template <class> friend class TransientModifier;

  T* a;
  size_t l;
 public:
  Vector(T *a, size_t l) : a(a), l(l) {}
  T operator[](int idx) { return a[idx]; }
  size_t length() const { return l; }

  class Iterator : public Sequence<Iterator, T> {
    size_t pos;
    const Vector<T> *v;
   public:
    Iterator(size_t pos, const Vector<T> *v) : pos(pos), v(v) {}

    bool IsValid() const {
      return pos < v->l;
    }
    void Next() {
      pos++;
    }
    T Deref() {
      return v->a[pos];
    }
  };

  Iterator sequence() const { return Iterator(0, this); }

  using Type = T;
  using SequenceType = Iterator;
};

template <typename T>
Vector<T> MakeVector(T *arr, size_t length) { return Vector<T>(arr, length); }

template <typename A>
Vector<typename A::Type> MakeVector(A seq)
{
  std::vector<typename A::Type> result;
  while (seq.IsValid()) {
    result.emplace_back(seq.Deref());
    seq.Next();
  }
  auto mem = (typename A::Type *) Allocate(sizeof(typename A::Type) * result.size());
  memcpy(mem, &result[0], result.size() * sizeof(typename A::Type));
  return MakeVector(mem, result.size());
}

template <typename K, typename V>
class Dictionary {
  std::map<K, V> m;
  template <typename KSeq, typename VSeq>
  friend Dictionary<typename KSeq::Type, typename VSeq::Type> MakeDict(KSeq, VSeq);
  Dictionary() {}
 public:
  class Iterator : public Sequence<Iterator, Tuple<K, V>> {
    Dictionary<K, V> *d;
    typename std::map<K, V>::iterator it;
   public:
    Iterator(Dictionary<K, V> *d, typename std::map<K, V>::iterator it) : d(d), it(it) {}

    bool IsValid() const {
      return it != d->m.end();
    }
    void Next() {
      ++it;
    }
    typename Iterator::Type Deref() {
      return std::make_tuple(it->first, it->second);
    }
  };

  using Type = Tuple<K, V>;
  using SequenceType = Iterator;

  SequenceType sequence() { return Iterator(this, m.begin()); }
};

template <typename KSeq, typename VSeq>
Dictionary<typename KSeq::Type, typename VSeq::Type> MakeDict(KSeq kseq, VSeq vseq)
{
  Dictionary<typename KSeq::Type, typename VSeq::Type> d;
  while (kseq.IsValid() && vseq.IsValid()) {
    d.m[kseq.Deref()] = vseq.Deref();
    kseq.Next();
    vseq.Next();
  }
  return d;
}

// transients

template <typename T>
class TransientModifier {
  T *obj;
 public:
  using Type = T*;
 protected:
  TransientModifier(Type t) : obj(t) {}

  void Clone() {
    auto p = (T *) Allocate(sizeof(T));
    memcpy(p, obj, sizeof(T));
    obj = p;
  }
  Type persistent_value() {
    return obj;
  }
 public:
  // modifiers
  T* operator->() {
    return obj;
  }
};

template <typename T>
class TransientModifier<Vector<T>> {
  Vector<T> vec;
 public:
  using Type = Vector<T>;
 protected:
  TransientModifier(Type vec) : vec(vec) {}

  size_t capacity() {
    size_t sz = (1 << (sizeof(long) * 8 - __builtin_clzl(vec.l))) * sizeof(T);
    return util::Align(sz, 8);
  }
  void Clone() {
    auto p = (T *) Allocate(capacity());
    memcpy(p, vec.a, sizeof(T) * vec.l);
    vec.a = p;
  }
  Type persistent_value() {
    return vec;
  }

 public:
  T &operator[](size_t idx) {
    return vec.a[idx];
  }
  void Insert(const T& obj, size_t pos) {
    size_t cap = capacity();
    vec.l++;
    if (vec.l > cap) {
      auto p = (T *) Allocate(capacity());
      memcpy(p, vec.a, sizeof(T) * pos);
      memcpy(p + pos + 1, vec.a + pos, sizeof(T) * (vec.l - pos - 1));
      vec.a = p;
    } else {
      memmove(vec.a + pos + 1, vec.a + pos, sizeof(T) * (vec.l - pos - 1));
    }
    vec.a[pos] = obj;
  }
  void Remove(size_t pos) {
    size_t cap = capacity();
    vec.l--;
    if (capacity() < cap) {
      auto p = (T *) Allocate(capacity());
      memcpy(p, vec.a, sizeof(T) * pos);
      memcpy(p + pos, vec.a + pos + 1, sizeof(T) * (vec.l - pos));
      vec.a = p;
    } else {
      memmove(vec.a + pos, vec.a + pos + 1, sizeof(T) * (vec.l - pos));
    }
  }
};

template <typename T>
class Transient : public TransientModifier<T> {
  bool need_copy;
 public:
  Transient(typename TransientModifier<T>::Type o)
      : TransientModifier<T>(o), need_copy(true) {}
  TransientModifier<T> &Update() {
    if (need_copy) {
      this->Clone();
      need_copy = false;
    }
    return *this;
  }
  typename TransientModifier<T>::Type Persist() {
    need_copy = true;
    return this->persistent_value();
  }
};

// lazy sequence functions

template <typename A, typename B>
class ZipImpl : public Sequence<ZipImpl<A, B>, Tuple<typename A::Type, typename B::Type>> {
  typename A::SequenceType seqa;
  typename B::SequenceType seqb;
 public:
  ZipImpl(const A &a, const B &b) : seqa(a.sequence()), seqb(b.sequence()) {}

  bool IsValid() const {
    return seqa.IsValid() && seqb.IsValid();
  }
  void Next() {
    seqa.Next();
    seqb.Next();
  }
  typename ZipImpl<A, B>::Type Deref() {
    return std::make_tuple(seqa.Deref(), seqb.Deref());
  }
};

template <typename A, typename B>
ZipImpl<A, B> Zip(const A &a, const B &b)
{
  return ZipImpl<A, B>(a, b);
}

template <typename A, typename F>
class MapImpl : public Sequence<MapImpl<A, F>, typename std::result_of<F(typename A::Type)>::type> {
  typename A::SequenceType seq;
  F func;
 public:
  MapImpl(const A &a, F f) : seq(a.sequence()), func(f) {}

  bool IsValid() const {
    return seq.IsValid();
  }
  void Next() {
    seq.Next();
  }
  typename MapImpl<A, F>::Type Deref() {
    return func(seq.Deref());
  }
};

template <typename A, typename F>
MapImpl<A, F> Map(const A &a, F func)
{
  return MapImpl<A, F>(a, func);
}

}

#endif /* FPCORE_H */
