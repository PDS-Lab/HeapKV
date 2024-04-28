#pragma once

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <ctime>
#include <limits>
#include <type_traits>

#include "linux/futex.h"
#include "rocksdb/rocksdb_namespace.h"
#include "sys/syscall.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

template <typename T>
constexpr static inline uint8_t TailingZero(T x) {
  if constexpr (std::is_same_v<T, uint64_t>) {
    return x == 0 ? 64 : __builtin_ctzll(x);
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    return x == 0 ? 32 : __builtin_ctz(x);
  } else if constexpr (std::is_same_v<T, uint16_t>) {
    return x == 0 ? 16 : __builtin_ctz(x);
  } else if constexpr (std::is_same_v<T, uint8_t>) {
    return x == 0 ? 8 : __builtin_ctz(x);
  } else {
    static_assert(std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t> ||
                      std::is_same_v<T, uint16_t> || std::is_same_v<T, uint8_t>,
                  "T must be uint64_t, uint32_t, uint16_t or uint8_t");
  }
}

template <typename T>
constexpr static inline uint8_t LeadingZero(T x) {
  if constexpr (std::is_same_v<T, uint64_t>) {
    return x == 0 ? 64 : __builtin_clzll(x);
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    return x == 0 ? 32 : __builtin_clz(x);
  } else if constexpr (std::is_same_v<T, uint16_t>) {
    return x == 0 ? 16 : __builtin_clz(x) - 16;
  } else if constexpr (std::is_same_v<T, uint8_t>) {
    return x == 0 ? 8 : __builtin_clz(x) - 24;
  } else {
    static_assert(std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t> ||
                      std::is_same_v<T, uint16_t> || std::is_same_v<T, uint8_t>,
                  "T must be uint64_t, uint32_t, uint16_t or uint8_t");
  }
}

constexpr static inline uint64_t align_up(uint64_t x, uint64_t align) {
  return (x + align - 1) & ~(align - 1);
}

constexpr static inline uint64_t align_down(uint64_t x, uint64_t align) {
  return x & ~(align - 1);
}

constexpr static inline bool is_aligned(uint64_t val, uint64_t align) {
  return (val & (align - 1)) == 0;
}

static inline auto futex(uint32_t *uaddr, int futex_op, uint32_t val,
                         const timespec *timeout = nullptr,
                         uint32_t *uaddr2 = nullptr, uint32_t val3 = 0)
    -> long {
  return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}

static inline auto futex_wait(uint32_t *uaddr, uint32_t val, bool is_private,
                              const timespec *timeout = nullptr) -> long {
  return futex(uaddr, is_private ? FUTEX_WAIT_PRIVATE : FUTEX_WAIT, val,
               timeout);
}

static inline auto futex_wake(uint32_t *uaddr, uint32_t val, bool is_private)
    -> long {
  return futex(uaddr, is_private ? FUTEX_WAKE_PRIVATE : FUTEX_WAKE, val);
}

static constexpr uint32_t PARKED = std::numeric_limits<uint32_t>::max();
static constexpr uint32_t EMPTY = 0;
static constexpr uint32_t NOTIFIED = 1;

class FutexParker {
 private:
  bool is_private_;
  std::atomic_uint32_t park_futex_{EMPTY};

 public:
  explicit FutexParker(bool is_private) : is_private_(is_private) {}

  void park() {
    if (park_futex_.fetch_sub(1, std::memory_order_acquire) == NOTIFIED) {
      return;
    }
    while (true) {
      uint32_t notified = NOTIFIED;
      futex_wait(reinterpret_cast<uint32_t *>(&park_futex_), PARKED,
                 is_private_, nullptr);
      if (park_futex_.compare_exchange_strong(notified, EMPTY,
                                              std::memory_order_acquire)) {
        return;
      }
      // spurious wake up
    }
  }

  void unpark() {
    if (park_futex_.exchange(NOTIFIED, std::memory_order_release) == PARKED) {
      futex_wake(reinterpret_cast<uint32_t *>(&park_futex_), 1, is_private_);
    }
  }
};

inline thread_local FutexParker thread_local_parker{true};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE