#ifndef UTIL_LINKLIST_H
#define UTIL_LINKLIST_H

namespace util {

// link list headers. STL is too slow
template <typename T>
struct GenericListNode {
  GenericListNode<T> *prev, *next;

  void InsertAfter(GenericListNode<T> *parent) {
    prev = parent;
    next = parent->next;
    parent->next->prev = this;
    parent->next = this;
  }

  void Remove() {
    prev->next = next;
    next->prev = prev;
    prev = next = nullptr; // detached
  }

  bool is_detached() {
    return prev == next && next == nullptr;
  }

  void Initialize() {
    prev = next = this;
  }

  bool empty() const { return next == this; }

  T *object() { return static_cast<T *>(this); }
};

using ListNode = GenericListNode<void>;

// Type Safe Embeded LinkListNode. E is a enum, serves as a variable name.
template <typename T, int E> struct TypedListNode;

template <typename T>
struct TypedListNodeWrapper {
  template <int E>
  static TypedListNode<T, E> *ToListNode(T *obj) {
    return obj;
  }
};

template <typename T, int E>
struct TypedListNode : public TypedListNodeWrapper<T>,
                       public GenericListNode<TypedListNode<T, E>> {
  T *object() { return static_cast<T *>(this); }
};

}

#endif
