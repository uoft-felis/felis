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

struct SpinnerSlotData;

class SpinnerSlot : public VHandleSyncService {
  SpinnerSlotData *buffer;
 public:
  SpinnerSlot();

  SpinnerSlotData *slot(int idx);

  void ClearWaitCountStats() final override;
  long GetWaitCountStat(int core) final override;
  bool Spin(uint64_t sid, uint64_t ver, ulong &wait_cnt, volatile uintptr_t *ptr);
  void Notify(uint64_t bitmap);
  bool IsPendingVal(uintptr_t val) final override {
    return ((val & ~kReadBitMask) >> 32) == (kPendingValue >> 32);
  }
  void WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver, void *handle) final override;
  void OfferData(volatile uintptr_t *addr, uintptr_t obj) final override;
};

struct SimpleSyncData;

class SimpleSync : public VHandleSyncService {
  SimpleSyncData *buffer;
 public:
  SimpleSync();

  void ClearWaitCountStats() final override;
  long GetWaitCountStat(int core) final override;
  void WaitForData(volatile uintptr_t *addr, uint64_t sid, uint64_t ver, void *handle) final override;
  void OfferData(volatile uintptr_t *addr, uintptr_t obj) final override;
  bool IsPendingVal(uintptr_t val) final override {
    return val == kPendingValue;
  }
};

}

namespace util {

template <>
struct InstanceInit<felis::SpinnerSlot> {
  static constexpr bool kHasInstance = true;
  static inline felis::SpinnerSlot *instance;
  InstanceInit() { instance = new felis::SpinnerSlot(); }
};

template <>
struct InstanceInit<felis::SimpleSync> {
  static constexpr bool kHasInstance = true;
  static inline felis::SimpleSync *instance;
  InstanceInit() { instance = new felis::SimpleSync(); }
};

}

#endif
