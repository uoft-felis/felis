#ifndef _VHANDLE_SYNC_H
#define _VHANDLE_SYNC_H

#include "vhandle.h"

namespace felis {

// Spinner Slots Because we're using a coroutine library, so our coroutines are
// not preemptive.  When a coroutine needs to wait for some condition, it just
// spins on this per-cpu spinner slot rather than the condition.
//
// This also requires the condition notifier be able to aware of that. What we
// can do for our sorted versioning is to to store a bitmap in the magic number
// count. This limits us to *63 cores* at maximum though. Exceeding this limit
// might lead us to use share spinner slots.

class SpinnerSlot : public VHandleSyncService {
 public:
  static constexpr int kNrSpinners = 32;
 private:
  struct {
    volatile long done;
    long __padding__[7];
  } slots[kNrSpinners];
 public:
  SpinnerSlot() { memset(slots, 0, 64 * kNrSpinners); }

  bool Spin(uint64_t sid, uint64_t ver, ulong &wait_cnt);
  void Notify(uint64_t bitmap) final override;
  bool IsPendingVal(uintptr_t val)  final override {
    return (val >> 32) == (kPendingValue >> 32);
  }
  void WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver, void *handle) final override;
  void OfferData(volatile uintptr_t *addr, uintptr_t obj) final override;
};

}

#endif
