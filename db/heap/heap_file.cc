#include "db/heap/heap_file.h"

#include <atomic>
#include <cassert>
#include <optional>

#include "port/port_posix.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

auto ExtentManager::AllocNewExtent() -> std::optional<ext_id_t> {
  MutexLock l(&mu_);
  ext_id_t new_ext_id = next_extent_num_++;
  extents_.emplace_back(ExtentMetaData{new_ext_id, kTotalFreeBits});
  sort_extents_.emplace(new_ext_id);
  latch_.emplace(new_ext_id, decltype(latch_)::value_type());
  return new_ext_id;
}

auto ExtentManager::TryLockMostFreeExtent(double allocatable_threshold)
    -> std::optional<ext_id_t> {
  MutexLock l(&mu_);
  for (auto it = sort_extents_.begin(); it != sort_extents_.end(); it++) {
    if (extents_[*it].approximate_free_bits_ / double(kTotalFreeBits) >
        allocatable_threshold) {
      auto latch_it = latch_.find(*it);
      if (latch_it != latch_.end()) {
        return std::nullopt;
      } else {
        latch_.emplace(*it, decltype(latch_)::value_type());
        return *it;
      }
    } else {
      break;
    }
  }
  return std::nullopt;
}

auto ExtentManager::TryLockExtent(ext_id_t extent_number) -> bool {
  MutexLock l(&mu_);
  if (extent_number >= next_extent_num_) {
    return false;
  }
  auto latch_it = latch_.find(extent_number);
  if (latch_it != latch_.end()) {
    return false;
  } else {
    latch_.emplace(extent_number, decltype(latch_)::value_type());
    return true;
  }
}

void ExtentManager::LockExtent(ext_id_t extent_number) {
  std::atomic_bool released = false;
  port::CondVar cv(&mu_);
  MutexLock l(&mu_);
  while (true) {
    auto latch_it = latch_.find(extent_number);
    if (latch_it != latch_.end()) {
      latch_it->second.emplace_back(&released, &cv);
      while (!released.load(std::memory_order_acquire)) {
        cv.Wait();
      }
      // waker pop the cv from the latch
    } else {
      latch_.emplace(extent_number, decltype(latch_)::value_type());
      return;
    }
  }
}

void ExtentManager::UnlockExtent(ext_id_t extent_number,
                                 uint32_t new_approximate_free_bits) {
  MutexLock l(&mu_);
  UnlockExtentInternal(extent_number, new_approximate_free_bits);
}

void ExtentManager::UnlockExtents(const std::vector<ExtentMetaData> &extents) {
  MutexLock l(&mu_);
  for (const auto &extent : extents) {
    UnlockExtentInternal(extent.extent_number_, extent.approximate_free_bits_);
  }
}

void ExtentManager::UnlockExtentInternal(ext_id_t extent_number,
                                         uint32_t new_approximate_free_bits) {
  auto latch_it = latch_.find(extent_number);
  assert(latch_it != latch_.end());
  if (latch_it->second.empty()) {
    latch_.erase(latch_it);
  } else {
    auto &pair = latch_it->second.front();
    pair.first->store(true, std::memory_order_release);
    pair.second->Signal();
    latch_it->second.pop_front();
  }
  sort_extents_.erase(extent_number);
  extents_[extent_number].approximate_free_bits_ = new_approximate_free_bits;
  sort_extents_.insert(extent_number);
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE