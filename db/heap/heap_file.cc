#include "db/heap/heap_file.h"

#include <atomic>
#include <cassert>
#include <optional>

#include "db/heap/io_engine.h"
#include "db/heap/utils.h"
#include "fcntl.h"
#include "port/port_posix.h"
#include "rocksdb/status.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

auto ExtentManager::AllocNewExtent() -> std::optional<ext_id_t> {
  MutexLock l(&mu_);
  ext_id_t new_ext_id = next_extent_num_++;
  extents_.emplace_back(ExtentMetaData{new_ext_id, kTotalFreeBits});
  sort_extents_.emplace(new_ext_id);
  latch_.emplace(new_ext_id, decltype(latch_)::mapped_type());
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
        continue;
      } else {
        latch_.emplace(*it, decltype(latch_)::mapped_type());
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
    latch_.emplace(extent_number, decltype(latch_)::mapped_type());
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
      // we cannot access latch_it since here
      while (!released.load(std::memory_order_acquire)) {
        cv.Wait();
      }
      // waker pop the cv from the latch
      return;
    } else {
      latch_.emplace(extent_number, decltype(latch_)::mapped_type());
      return;
    }
  }
}

void ExtentManager::UnlockExtent(ext_id_t extent_number,
                                 uint32_t new_approximate_free_bits,
                                 bool update_free_bits) {
  MutexLock l(&mu_);
  UnlockExtentInternal(extent_number, new_approximate_free_bits,
                       update_free_bits);
}

void ExtentManager::UnlockExtents(const std::vector<ExtentMetaData> &extents,
                                  bool update_free_bits) {
  MutexLock l(&mu_);
  for (const auto &extent : extents) {
    UnlockExtentInternal(extent.extent_number_, extent.approximate_free_bits_,
                         update_free_bits);
  }
}

void ExtentManager::UnlockExtentInternal(ext_id_t extent_number,
                                         uint32_t new_approximate_free_bits,
                                         bool update_free_bits) {
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
  if (update_free_bits) {
    sort_extents_.erase(extent_number);
    extents_[extent_number].approximate_free_bits_ = new_approximate_free_bits;
    sort_extents_.insert(extent_number);
  }
}

auto HeapFile::Open(UringIoEngine *io_engine, const std::string &filename,
                    uint32_t column_family_id, bool use_direct_io,
                    std::unique_ptr<HeapFile> *file_handle) -> Status {
  int flag = O_RDWR | O_CREAT;
  if (use_direct_io) {
    flag |= O_DIRECT;
  }
  auto f = io_engine->OpenAt(UringIoOptions(), AT_FDCWD, filename.c_str(), flag,
                             0644);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("open failed", strerror(-f->Result()));
  }
  *file_handle = std::make_unique<HeapFile>(
      filename, f->Result(), column_family_id, column_family_id, use_direct_io);
  return Status::OK();
}

auto HeapFile::Stat(UringIoEngine *io_engine,
                    struct statx *statxbuf) -> Status {
  auto f = io_engine->Statx(UringIoOptions(), fd_, "", AT_EMPTY_PATH,
                            STATX_BASIC_STATS, statxbuf);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("stat failed", strerror(-f->Result()));
  }
  return Status::OK();
}

auto HeapFile::ReadExtentHeaderAsync(
    UringIoEngine *io_engine, const UringIoOptions &opts,
    ext_id_t extent_number, ExtentBitmap *bitmap,
    int fixed_fd_index) -> std::unique_ptr<UringCmdFuture> {
  assert(!opts.FixedFile() || fixed_fd_index >= 0);
  return io_engine->Read(opts, opts.FixedFile() ? fixed_fd_index : fd(),
                         bitmap->data_, kExtentHeaderSize,
                         ExtentHeaderOffset(extent_number));
}

auto HeapFile::ReadExtentHeader(UringIoEngine *io_engine,
                                const UringIoOptions &opts,
                                ext_id_t extent_number, ExtentBitmap *bitmap,
                                int fixed_fd_index) -> Status {
  auto f = ReadExtentHeaderAsync(io_engine, opts, extent_number, bitmap,
                                 fixed_fd_index);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("read failed", strerror(-f->Result()));
  } else if (f->Result() != kExtentHeaderSize) {
    return Status::IOError("extent header size mismatch");
  } else if (!bitmap->VerifyChecksum()) {
    return Status::Corruption("extent header checksum mismatch");
  }
  return Status::OK();
}

auto HeapFile::WriteExtentHeaderAsync(
    UringIoEngine *io_engine, const UringIoOptions &opts,
    ext_id_t extent_number, const ExtentBitmap &bitmap,
    int fixed_fd_index) -> std::unique_ptr<UringCmdFuture> {
  assert(!opts.FixedFile() || fixed_fd_index >= 0);
  return io_engine->Write(opts, opts.FixedFile() ? fixed_fd_index : fd(),
                          bitmap.data_, kExtentHeaderSize,
                          ExtentHeaderOffset(extent_number));
}

auto HeapFile::WriteExtentHeader(UringIoEngine *io_engine,
                                 const UringIoOptions &opts,
                                 ext_id_t extent_number,
                                 const ExtentBitmap &bitmap,
                                 int fixed_fd_index) -> Status {
  auto f = WriteExtentHeaderAsync(io_engine, opts, extent_number, bitmap,
                                  fixed_fd_index);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("write failed", strerror(-f->Result()));
  }
  return Status::OK();
}

auto HeapFile::GetHeapValueAsync(
    UringIoEngine *io_engine, const UringIoOptions &opts,
    ext_id_t extent_number, uint16_t block_offset, uint16_t block_count,
    uint8_t *buffer, int fixed_fd_index) -> std::unique_ptr<UringCmdFuture> {
  assert(!opts.FixedFile() || fixed_fd_index >= 0);
  assert(!use_direct_io() ||
         is_aligned(reinterpret_cast<uint64_t>(buffer), kHeapFileBlockSize));
  return io_engine->Read(opts, opts.FixedFile() ? fixed_fd_index : fd(), buffer,
                         block_count * kHeapFileBlockSize,
                         ExtentDataOffset(extent_number, block_offset));
}

auto HeapFile::GetHeapValue(UringIoEngine *io_engine,
                            const UringIoOptions &opts, ext_id_t extent_number,
                            uint16_t block_offset, uint16_t block_count,
                            uint8_t *buffer, int fixed_fd_index) -> Status {
  auto f = GetHeapValueAsync(io_engine, opts, extent_number, block_offset,
                             block_count, buffer, fixed_fd_index);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("read failed", strerror(-f->Result()));
  }
  return Status::OK();
}

auto HeapFile::PutHeapValueAsync(
    UringIoEngine *io_engine, const UringIoOptions &opts,
    ext_id_t extent_number, uint16_t block_offset, uint16_t block_count,
    const uint8_t *buffer,
    int fixed_fd_index) -> std::unique_ptr<UringCmdFuture> {
  assert(!opts.FixedFile() || fixed_fd_index >= 0);
  assert(!use_direct_io() ||
         is_aligned(reinterpret_cast<uint64_t>(buffer), kHeapFileBlockSize));
  return io_engine->Write(opts, opts.FixedFile() ? fixed_fd_index : fd(),
                          buffer, block_count * kHeapFileBlockSize,
                          ExtentDataOffset(extent_number, block_offset));
}

auto HeapFile::PutHeapValue(UringIoEngine *io_engine,
                            const UringIoOptions &opts, ext_id_t extent_number,
                            uint16_t block_offset, uint16_t block_count,
                            const uint8_t *buffer,
                            int fixed_fd_index) -> Status {
  auto f = PutHeapValueAsync(io_engine, opts, extent_number, block_offset,
                             block_count, buffer, fixed_fd_index);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("write failed", strerror(-f->Result()));
  }
  return Status::OK();
}

auto HeapFile::FsyncAsync(UringIoEngine *io_engine, const UringIoOptions &opts,
                          bool datasync, int fixed_fd_index)
    -> std::unique_ptr<UringCmdFuture> {
  assert(!opts.FixedFile() || fixed_fd_index >= 0);
  return io_engine->Fsync(opts, opts.FixedFile() ? fixed_fd_index : fd(),
                          datasync);
}

auto HeapFile::Fsync(UringIoEngine *io_engine, const UringIoOptions &opts,
                     bool datasync, int fixed_fd_index) -> Status {
  auto f = FsyncAsync(io_engine, opts, datasync, fixed_fd_index);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("fsync failed", strerror(-f->Result()));
  }
  return Status::OK();
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE