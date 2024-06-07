#include "db/heap/extent.h"

#include <cassert>
#include <cstddef>
#include <optional>
#include <vector>

#include "port/likely.h"
#include "util/mutexlock.h"
#ifndef NDEBUG
#include "db/heap/utils.h"
#endif

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

bool ExtentComp::operator()(const Extent *lhs, const Extent *rhs) const {
  if (lhs->meta_.approximate_free_bits_ != rhs->meta_.approximate_free_bits_) {
    return lhs->meta_.approximate_free_bits_ >
           rhs->meta_.approximate_free_bits_;
  }
  return lhs->meta_.extent_number_ < rhs->meta_.extent_number_;
}

auto ExtentManager::AllocNewExtent() -> std::optional<ext_id_t> {
  MutexLock l(&mu_);
  ext_id_t new_ext_id = next_extent_num_++;
  auto ext = GetExtent(new_ext_id);
  ext->wait_mu_.Lock();
  ext->meta_ = ExtentMetaData{new_ext_id, kTotalFreeBits};
  ext->ptr_ = sort_extents_.end();
  return new_ext_id;
}

auto ExtentManager::TryLockMostFreeExtent(double allocatable_threshold)
    -> std::optional<ext_id_t> {
  MutexLock l(&mu_);
  if (sort_extents_.empty()) {
    return std::nullopt;
  }
  for (auto it = sort_extents_.begin(); it != sort_extents_.end(); it++) {
    if ((*it)->wait_mu_.TryLock()) {
      ext_id_t eid = (*it)->meta_.extent_number_;
      (*it)->ptr_ = sort_extents_.end();
      sort_extents_.erase(it);
      return eid;
    }
  }
  return std::nullopt;
}

auto ExtentManager::TryLockExtent(ext_id_t extent_number) -> bool {
  MutexLock l(&mu_);
  if (extent_number >= next_extent_num_) {
    return false;
  }
  auto ext = GetExtent(extent_number);
  if (ext->wait_mu_.TryLock()) {
    if (ext->ptr_ != sort_extents_.end()) {
      sort_extents_.erase(ext->ptr_);
      ext->ptr_ = sort_extents_.end();
    }
    return true;
  }
  return false;
}

void ExtentManager::LockExtent(ext_id_t extent_number) {
  mu_.Lock();
  assert(extent_number < next_extent_num_);
  auto ext = GetExtent(extent_number);
  // fast check
  if (ext->wait_mu_.TryLock()) {
    if (ext->ptr_ != sort_extents_.end()) {
      sort_extents_.erase(ext->ptr_);
      ext->ptr_ = sort_extents_.end();
    }
    mu_.Unlock();
    return;
  }
  // slow path
  mu_.Unlock();
  ext->wait_mu_.Lock();
  mu_.Lock();
  if (ext->ptr_ != sort_extents_.end()) {
    sort_extents_.erase(ext->ptr_);
    ext->ptr_ = sort_extents_.end();
  }
  mu_.Unlock();
}

void ExtentManager::UnlockExtent(ext_id_t extent_number,
                                 uint32_t new_approximate_free_bits,
                                 bool update_free_bits, uint64_t gc_epoch) {
  MutexLock l(&mu_);
  UnlockExtentInternal(extent_number, new_approximate_free_bits,
                       update_free_bits, gc_epoch);
}

void ExtentManager::UnlockExtents(const std::vector<ExtentMetaData> &extents,
                                  bool update_free_bits, uint64_t gc_epoch) {
  MutexLock l(&mu_);
  for (const auto &extent : extents) {
    UnlockExtentInternal(extent.extent_number_, extent.approximate_free_bits_,
                         update_free_bits, gc_epoch);
  }
}

void ExtentManager::UnlockExtentInternal(ext_id_t extent_number,
                                         uint32_t new_approximate_free_bits,
                                         bool update_free_bits,
                                         uint64_t gc_epoch) {
  assert(extent_number < next_extent_num_);
  auto ext = GetExtent(extent_number);
  if (update_free_bits) {
    ext->meta_.approximate_free_bits_ = new_approximate_free_bits;
  }
  if (gc_epoch != 0) {
    ext->gc_epoch_ = gc_epoch;
  }
  if (ext->meta_.approximate_free_bits_ / double(kTotalFreeBits) >
      allocatable_threshold_) {
    auto r = sort_extents_.insert(ext);
    assert(r.second);
    ext->ptr_ = r.first;
  }
  ext->wait_mu_.Unlock();
}

auto ExtentManager::GetExtent(ext_id_t extent_number) -> Extent * {
  size_t pid = extent_number / kExtentPerPage;
  if (UNLIKELY(pages_.size() <= pid)) {
    auto ptr = std::aligned_alloc(4096, kPageSize);
    pages_.push_back(ptr);
    for (size_t i = 0; i < kExtentPerPage; i++) {
      new (reinterpret_cast<Extent *>(ptr) + i) Extent();
    }
  }
  return reinterpret_cast<Extent *>(pages_[pid]) +
         extent_number % kExtentPerPage;
}

void ExtentManager::SubmitGarbage(
    const std::vector<ExtentGarbageSpan> &garbages) {
  if (garbages.empty()) {
    return;
  }
  auto update_pending = [&](Extent *ext) {
    if ((ext->garbage_cnts_ + ext->meta_.approximate_free_bits_) /
            double(kTotalFreeBits) >
        allocatable_threshold_) {
      need_schedule_gc_.insert(ext->meta_.extent_number_);
    }
  };

  MutexLock l(&mu_);
  size_t off = 0;
  Extent *e = GetExtent(garbages[0].extent_number_);
  for (auto g : garbages) {
    assert(g.extent_number_ < next_extent_num_);
    if (g.extent_number_ != e->meta_.extent_number_) {
      update_pending(e);
      e = GetExtent(g.extent_number_);
    }
    e->garbage_cnts_ += g.block_cnt_;
    e->garbage_blocks_.push_back(g);
  }
  update_pending(e);
  current_gc_epoch_++;
  // TODO(wnj): write gc info to manifest, then commit the epoch
  committed_gc_epoch_ = current_gc_epoch_;
}

auto ExtentManager::GenerateGCTask(bool force_all) -> HeapFileGCTask {
  HeapFileGCTask task;
  MutexLock l(&mu_);
  if (!force_all) {
    task.garbages_.reserve(need_schedule_gc_.size());
    task.epoch_ = current_gc_epoch_;
    for (auto ext : need_schedule_gc_) {
      auto e = GetExtent(ext);
      if ((e->garbage_cnts_ + e->meta_.approximate_free_bits_) /
              double(kTotalFreeBits) >
          allocatable_threshold_) {
        task.garbages_.emplace_back(ext, std::vector<GarbageSpan>());
        task.garbages_.back().second.swap(e->garbage_blocks_);
        e->garbage_cnts_ = 0;
      }
    }
  } else {
    task.garbages_.reserve(next_extent_num_);
    task.epoch_ = current_gc_epoch_;
    for (ext_id_t i = 0; i < next_extent_num_; i++) {
      auto e = GetExtent(i);
      if (!e->garbage_blocks_.empty()) {
        task.garbages_.emplace_back(i, std::vector<GarbageSpan>());
        task.garbages_.back().second.swap(e->garbage_blocks_);
        e->garbage_cnts_ = 0;
      }
    }
  }
  need_schedule_gc_.clear();
  // TODO(wnj): wait committed epoch hit task epoch which means the task can
  // be run
  return task;
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE