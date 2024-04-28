#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

#include "port/port_posix.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/coding_lean.h"
#include "util/hash.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

constexpr size_t kHeapFileBlockSize = 512;
constexpr size_t kExtentHeaderSize = 4096;
constexpr size_t kChecksumSize = sizeof(uint32_t);
constexpr size_t kTotalFreeBits = 8 * (kExtentHeaderSize - kChecksumSize);
constexpr size_t kExtentSize =
    kExtentHeaderSize + kTotalFreeBits * kHeapFileBlockSize;

// Extent is the basic unit of storage in heapkv. It is a 4KiB page with a 4B
// checksum at the beginning.
struct ExtentBitmap {
  uint8_t data_[kExtentHeaderSize];
  uint32_t Checksum() const {
    return DecodeFixed32(reinterpret_cast<const char *>(data_));
  }
  bool VerifyChecksum() const { return Checksum() == CalcChecksum(); }
  void GenerateChecksum() {
    EncodeFixed32(reinterpret_cast<char *>(data_), CalcChecksum());
  }

 private:
  uint32_t CalcChecksum() const {
    return Lower32of64(XXH3_64bits(data_ + sizeof(uint32_t),
                                   kExtentHeaderSize - sizeof(uint32_t)));
  }
};

using ext_id_t = uint32_t;

struct ExtentMetaData {
  // uint64_t heapfile_number_;
  ext_id_t extent_number_;
  uint32_t approximate_free_bits_;
};

class HeapFile;

class ExtentManager {
 private:
  struct ExtentComp {
    std::vector<ExtentMetaData> &extents_;
    bool operator()(const ext_id_t &lhs, const ext_id_t &rhs) const {
      if (extents_[lhs].approximate_free_bits_ !=
          extents_[rhs].approximate_free_bits_) {
        return extents_[lhs].approximate_free_bits_ >
               extents_[rhs].approximate_free_bits_;
      }
      return lhs < rhs;
    }
  };

 private:
  std::shared_ptr<HeapFile> heapfile_;
  ext_id_t next_extent_num_{0};
  port::Mutex mu_;
  std::unordered_map<ext_id_t,
                     std::deque<std::pair<std::atomic_bool *, port::CondVar *>>>
      latch_;
  std::vector<ExtentMetaData> extents_;
  std::set<ext_id_t, ExtentComp> sort_extents_;

 public:
  ExtentManager(std::shared_ptr<HeapFile> heapfile, ext_id_t next_extent_num,
                std::vector<ExtentMetaData> &&extents)
      : heapfile_(std::move(heapfile)),
        next_extent_num_(next_extent_num),
        extents_(std::move(extents)),
        sort_extents_(ExtentComp{extents_}) {
    assert(next_extent_num_ == extents_.size());
    for (ext_id_t i = 0; i < next_extent_num_; i++) {
      sort_extents_.insert(i);
    }
  }

  // add a new extent to the heap file, this will lock the allocated new extent
  auto AllocNewExtent() -> std::optional<ext_id_t>;
  // find the first unlocked extent (sorted by free bits) and lock it. only
  // extent with free bits > allocatable_threshold will be returned
  auto TryLockMostFreeExtent(double allocatable_threshold)
      -> std::optional<ext_id_t>;
  // try to lock the extent, return false if the extent is already locked
  auto TryLockExtent(ext_id_t extent_number) -> bool;
  // wait until the extent is locked
  void LockExtent(ext_id_t extent_number);
  // unlock the extent, and update the free bits
  void UnlockExtent(ext_id_t extent_number, uint32_t new_approximate_free_bits);
  // unlock the extents, and update the free bits
  void UnlockExtents(const std::vector<ExtentMetaData> &extents);

 private:
  void UnlockExtentInternal(ext_id_t extent_number,
                            uint32_t new_approximate_free_bits);
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE