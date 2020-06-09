#ifndef UTIL_LOCKS_H
#define UTIL_LOCKS_H
#include <atomic>

namespace util {

// Locks
class SpinLock {
  std::atomic_bool lock;
 public:
  SpinLock();
  void Lock();
  void Acquire() { Lock(); }

  bool TryLock();

  void Unlock();
  void Release() { Unlock(); }
};

class MCSSpinLock {
 public:
  struct QNode {
    std::atomic_bool done = false;
    std::atomic<QNode *> next = nullptr;
  };
 private:
  std::atomic<QNode *> tail;
 public:
  MCSSpinLock();
  bool IsLocked();
  bool TryLock(QNode *qnode);

  void Lock(QNode *qnode);
  void Acquire(QNode *qnode) { Lock(qnode); }

  void Unlock(QNode *qnode);
  void Release(QNode *qnode) { Unlock(qnode); }
};

template <typename T>
class Guard {
  T &t;
 public:
  Guard(T &t) : t(t) { t.Acquire(); }
  ~Guard() { t.Release(); }
};

}

#endif
