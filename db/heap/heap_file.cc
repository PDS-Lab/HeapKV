#include "db/heap/heap_file.h"

#include <cassert>
#include <optional>

#include "db/heap/io_engine.h"
#include "port/likely.h"
#ifndef NDEBUG
#include "db/heap/utils.h"
#endif
#include "fcntl.h"
#include "port/port_posix.h"
#include "rocksdb/status.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

auto ExtentManager::AllocNewExtent() -> std::optional<ext_id_t> {
  MutexLock l(&mu_);
  ext_id_t new_ext_id = next_extent_num_++;
  auto ext = GetExtent(new_ext_id);
  ext->mu_.Lock();
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
    if ((*it)->mu_.TryLock()) {
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
  if (ext->mu_.TryLock()) {
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
  if (ext->mu_.TryLock()) {
    if (ext->ptr_ != sort_extents_.end()) {
      sort_extents_.erase(ext->ptr_);
      ext->ptr_ = sort_extents_.end();
    }
    mu_.Unlock();
    return;
  }
  // slow path
  mu_.Unlock();
  ext->mu_.Lock();
  mu_.Lock();
  if (ext->ptr_ != sort_extents_.end()) {
    sort_extents_.erase(ext->ptr_);
    ext->ptr_ = sort_extents_.end();
  }
  mu_.Unlock();
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
  assert(extent_number < next_extent_num_);
  auto ext = GetExtent(extent_number);
  if (update_free_bits) {
    ext->meta_.approximate_free_bits_ = new_approximate_free_bits;
  }
  if (ext->meta_.approximate_free_bits_ / double(kTotalFreeBits) >
      allocatable_threshold_) {
    auto r = sort_extents_.insert(ext);
    assert(r.second);
    ext->ptr_ = r.first;
  }
  ext->mu_.Unlock();
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
  if (f->Result() != kExtentHeaderSize) {
    return Status::IOError("extent header size mismatch");
  }
  return Status::OK();
}

auto HeapFile::GetHeapValueAsync(
    UringIoEngine *io_engine, const UringIoOptions &opts,
    ext_id_t extent_number, uint32_t block_offset, uint32_t block_count,
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
                            uint32_t block_offset, uint32_t block_count,
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
    ext_id_t extent_number, uint32_t block_offset, uint32_t block_count,
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
                            uint32_t block_offset, uint32_t block_count,
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