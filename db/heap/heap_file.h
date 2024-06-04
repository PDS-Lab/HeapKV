#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "db/heap/io_engine.h"
#include "port/port_posix.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "util/coding_lean.h"
#include "util/hash.h"
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

class HeapFile {
 private:
  std::string filename_;
  int fd_;
  // currently, file_number_ is the same as column_family_id_
  uint32_t file_number_;
  uint32_t column_family_id_;
  bool use_direct_io_;

 public:
  HeapFile(std::string filename, int fd, uint32_t file_number,
           uint32_t column_family_id, bool use_direct_io)
      : filename_(std::move(filename)),
        fd_(fd),
        file_number_(file_number),
        column_family_id_(column_family_id),
        use_direct_io_(use_direct_io) {}
  ~HeapFile() { close(fd_); }

  auto filename() const -> const std::string & { return filename_; }
  auto fd() const -> int { return fd_; }
  auto file_number() const -> uint32_t { return file_number_; }
  auto column_family_id() const -> uint32_t { return column_family_id_; }
  auto use_direct_io() const -> bool { return use_direct_io_; }

  static auto Open(UringIoEngine *io_engine, const std::string &filename,
                   uint32_t column_family_id, bool use_direct_io,
                   std::unique_ptr<HeapFile> *file_handle) -> Status;
  auto Stat(UringIoEngine *io_engine, struct statx *statxbuf) -> Status;
  auto ReadExtentHeaderAsync(UringIoEngine *io_engine,
                             const UringIoOptions &opts, ext_id_t extent_number,
                             ExtentBitmap *bitmap, int fixed_fd_index = -1)
      -> std::unique_ptr<UringCmdFuture>;
  auto ReadExtentHeader(UringIoEngine *io_engine, const UringIoOptions &opts,
                        ext_id_t extent_number, ExtentBitmap *bitmap,
                        int fixed_fd_index = -1) -> Status;
  auto WriteExtentHeaderAsync(
      UringIoEngine *io_engine, const UringIoOptions &opts,
      ext_id_t extent_number, const ExtentBitmap &bitmap,
      int fixed_fd_index = -1) -> std::unique_ptr<UringCmdFuture>;
  auto WriteExtentHeader(UringIoEngine *io_engine, const UringIoOptions &opts,
                         ext_id_t extent_number, const ExtentBitmap &bitmap,
                         int fixed_fd_index = -1) -> Status;
  auto GetHeapValueAsync(UringIoEngine *io_engine, const UringIoOptions &opts,
                         ext_id_t extent_number, uint32_t block_offset,
                         uint32_t block_count, uint8_t *buffer,
                         int fixed_fd_index = -1)
      -> std::unique_ptr<UringCmdFuture>;
  auto GetHeapValue(UringIoEngine *io_engine, const UringIoOptions &opts,
                    ext_id_t extent_number, uint32_t block_offset,
                    uint32_t block_count, uint8_t *buffer,
                    int fixed_fd_index = -1) -> Status;
  auto PutHeapValueAsync(UringIoEngine *io_engine, const UringIoOptions &opts,
                         ext_id_t extent_number, uint32_t block_offset,
                         uint32_t block_count, const uint8_t *buffer,
                         int fixed_fd_index = -1)
      -> std::unique_ptr<UringCmdFuture>;
  auto PutHeapValue(UringIoEngine *io_engine, const UringIoOptions &opts,
                    ext_id_t extent_number, uint32_t block_offset,
                    uint32_t block_count, const uint8_t *buffer,
                    int fixed_fd_index) -> Status;
  // auto FsyncAsync(UringIoEngine *io_engine, const UringIoOptions &opts,
  //                 bool datasync,
  //                 int fixed_fd_index = -1) ->
  //                 std::unique_ptr<UringCmdFuture>;
  auto Fsync(UringIoEngine *io_engine, const UringIoOptions &opts,
             bool datasync, int fixed_fd_index = -1) -> Status;
};

class ExtentManager {
 private:
  struct Extent;
  struct ExtentComp {
    ExtentManager *ext_mgr_;
    bool operator()(const Extent *lhs, const Extent *rhs) const {
      if (lhs->meta_.approximate_free_bits_ !=
          rhs->meta_.approximate_free_bits_) {
        return lhs->meta_.approximate_free_bits_ >
               rhs->meta_.approximate_free_bits_;
      }
      return lhs->meta_.extent_number_ < rhs->meta_.extent_number_;
    }
  };
  struct Extent {
    port::Mutex mu_;
    ExtentMetaData meta_;
    std::set<Extent *, ExtentComp>::const_iterator ptr_;
  };
  static constexpr size_t kPageSize = 64 * 1024;
  static constexpr size_t kExtentPerPage = kPageSize / sizeof(Extent);
  using ExtentPage = Extent[kExtentPerPage];

 private:
  HeapFile *heap_file_;
  double allocatable_threshold_;
  ext_id_t next_extent_num_{0};
  port::Mutex mu_;
  std::set<Extent *, ExtentComp> sort_extents_;
  std::vector<void *> pages_;

 public:
  ExtentManager(HeapFile *heapfile, double allocatable_threshold,
                ext_id_t next_extent_num, std::vector<ExtentMetaData> extents)
      : heap_file_(std::move(heapfile)),
        allocatable_threshold_(allocatable_threshold),
        next_extent_num_(next_extent_num),
        sort_extents_(ExtentComp{this}) {
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
                    bool update_free_bits);
  // unlock the extents, and update the free bits
  void UnlockExtents(const std::vector<ExtentMetaData> &extents,
                     bool update_free_bits);

 private:
  auto GetExtent(ext_id_t extent_number) -> Extent *;
  void UnlockExtentInternal(ext_id_t extent_number,
                            uint32_t new_approximate_free_bits,
                            bool update_free_bits);
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE