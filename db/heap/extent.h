#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <set>
#include <unordered_set>
#include <vector>

#include "port/port_posix.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/coding_lean.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

constexpr size_t kHeapFileBlockSize = 512;
constexpr size_t kExtentHeaderSize = 16 * 1024;
constexpr size_t kChecksumSize = sizeof(uint32_t);
constexpr size_t kBitmapSize = kExtentHeaderSize - kChecksumSize;
constexpr size_t kTotalFreeBits = 8 * kBitmapSize;
constexpr size_t kExtentSize =
    kExtentHeaderSize + kTotalFreeBits * kHeapFileBlockSize;

using ext_id_t = uint32_t;
constexpr ext_id_t InValidExtentId = std::numeric_limits<ext_id_t>::max();

inline uint64_t ExtentHeaderOffset(ext_id_t extent_number) {
  return extent_number * kExtentSize;
}

inline uint64_t ExtentDataOffset(ext_id_t extent_number,
                                 uint32_t block_offset) {
  return ExtentHeaderOffset(extent_number) + kExtentHeaderSize +
         block_offset * kHeapFileBlockSize;
}

// Extent is the basic unit of storage in heapkv. It is a 4KiB page with a 4B
// checksum at the beginning.
struct alignas(kExtentHeaderSize) ExtentBitmap {
  uint8_t data_[kExtentHeaderSize];
  uint8_t *Bitmap() { return data_ + kChecksumSize; }
  const uint8_t *Bitmap() const { return data_ + kChecksumSize; }
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

struct ExtentMetaData {
  // uint64_t heapfile_number_;
  ext_id_t extent_number_;
  uint32_t approximate_free_bits_;
};

struct GarbageSpan {
  uint32_t block_off_;
  uint32_t block_cnt_;
};

struct ExtentGarbageSpan : public GarbageSpan {
  ext_id_t extent_number_;
};

class HeapFile;
struct Extent;
struct ExtentComp {
  bool operator()(const Extent *lhs, const Extent *rhs) const;
};
struct Extent {
  port::Mutex wait_mu_;
  ExtentMetaData meta_;
  std::set<Extent *, ExtentComp>::const_iterator ptr_;
  // GC related fields
  uint32_t garbage_cnts_{0};  //  number of garbage blocks
  uint64_t gc_epoch_{0};      // garbage <= this epoch has been collected
  std::vector<GarbageSpan> garbage_blocks_;  // pending garbage blocks
};

struct HeapFileGCTask {
  uint64_t epoch_;  // epoch when the task is generated
  std::vector<std::pair<ext_id_t, std::vector<GarbageSpan>>> garbages_;
};

class ExtentManager {
 private:
  static constexpr size_t kPageSize = 64 * 1024;
  static constexpr size_t kExtentPerPage = kPageSize / sizeof(Extent);
  using ExtentPage = Extent[kExtentPerPage];

 private:
  HeapFile *heap_file_;
  double allocatable_threshold_;
  mutable port::Mutex mu_;
  ext_id_t next_extent_num_{0};
  uint64_t current_gc_epoch_{0};
  uint64_t committed_gc_epoch_{0};
  std::set<Extent *, ExtentComp> sort_extents_;
  std::vector<void *> pages_;
  std::unordered_set<ext_id_t> need_schedule_gc_;

 public:
  ExtentManager(HeapFile *heapfile, double allocatable_threshold,
                ext_id_t next_extent_num, std::vector<ExtentMetaData> extents)
      : heap_file_(std::move(heapfile)),
        allocatable_threshold_(allocatable_threshold),
        next_extent_num_(next_extent_num),
        sort_extents_(ExtentComp{}) {
    for (ext_id_t i = 0; i < next_extent_num_; i++) {
      auto ext = GetExtent(i);
      ext->meta_ = extents[i];
      if (ext->meta_.approximate_free_bits_ / double(kTotalFreeBits) >
          allocatable_threshold_) {
        auto r = sort_extents_.insert(ext);
        assert(r.second);
        ext->ptr_ = r.first;
      } else {
        ext->ptr_ = sort_extents_.end();
      }
    }
  }

  ~ExtentManager() {
    for (auto page : pages_) {
      free(page);
    }
  }

  auto heap_file() const -> const HeapFile * { return heap_file_; }
  auto heap_file() -> HeapFile * { return heap_file_; }
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
  void UnlockExtent(ext_id_t extent_number, uint32_t new_approximate_free_bits,
                    bool update_free_bits, uint64_t gc_epoch);
  // unlock the extents, and update the free bits
  void UnlockExtents(const std::vector<ExtentMetaData> &extents,
                     bool update_free_bits, uint64_t gc_epoch);
  void SubmitGarbage(const std::vector<ExtentGarbageSpan> &garbages);
  bool NeedScheduleGC() const {
    MutexLock l(&mu_);
    return !need_schedule_gc_.empty();
  }
  auto GenerateGCTask(bool force_all = false) -> HeapFileGCTask;

 private:
  auto GetExtent(ext_id_t extent_number) -> Extent *;
  void UnlockExtentInternal(ext_id_t extent_number,
                            uint32_t new_approximate_free_bits,
                            bool update_free_bits, uint64_t gc_epoch);
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE