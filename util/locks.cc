#include <immintrin.h>
#include "locks.h"

namespace util {

SpinLock::SpinLock() : lock(false) {}

void SpinLock::Lock()
{
  bool locked = false;
  while (!lock.compare_exchange_strong(locked, true)) {
    locked = false;
    _mm_pause();
  }
}

bool SpinLock::TryLock()
{
  bool locked = false;
  return lock.compare_exchange_strong(locked, true);
}

void SpinLock::Unlock()
{
  lock.store(false);
}

MCSSpinLock::MCSSpinLock() : tail(nullptr) {}

bool MCSSpinLock::IsLocked()
{
  return tail.load() != nullptr;
}

bool MCSSpinLock::TryLock(QNode *qnode)
{
  QNode *old = nullptr;
  return tail.compare_exchange_strong(old, qnode);
}

void MCSSpinLock::Lock(QNode *qnode)
{
  auto last = tail.exchange(qnode);
  if (last) {
    last->next = qnode;
    while (!qnode->done) _mm_pause();
  }
}

void MCSSpinLock::Unlock(QNode *qnode)
{
  if (!qnode->next) {
    auto owner = qnode;
    if (tail.compare_exchange_strong(owner, nullptr)) return;
    while (!qnode->next.load()) _mm_pause();
  }
  qnode->next.load()->done = true;
}

}
