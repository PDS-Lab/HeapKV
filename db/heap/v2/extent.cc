#include "db/heap/v2/extent.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>

#include "db/heap/io_engine.h"
#include "rocksdb/status.h"
#include "util/coding_lean.h"
#include "util/hash.h"
#include "util/xxhash.h"

namespace HEAPKV_NS_V2 {

Status ExtentFile::Open(ExtentFileName fn, std::string_view base_dir,
                        std::unique_ptr<ExtentFile>* file_ptr) {
  std::string path = BuildPath(fn, base_dir);
  int fd = open(path.c_str(), O_RDWR | O_DIRECT);
  if (fd < 0) {
    return Status::IOError("failed to open file " + path, strerror(errno));
  }
  struct stat64 st;
  int rc = fstat64(fd, &st);
  if (rc != 0) {
    return Status::IOError("failed to stat file " + path, strerror(errno));
  }
  *file_ptr = std::make_unique<ExtentFile>(fn, fd, st.st_size);
  return Status::OK();
}

auto ExtentFile::OpenAsync(UringIoEngine* io_engine, const std::string& path)
    -> std::unique_ptr<UringCmdFuture> {
  // std::string path = BuildPath(fn, base_dir);
  return io_engine->OpenAt(UringIoOptions{}, AT_FDCWD, path.c_str(),
                           O_RDWR | O_DIRECT, 0644);
}

Status ExtentFile::Create(ExtentFileName fn, std::string_view base_dir,
                          std::unique_ptr<ExtentFile>* file_ptr) {
  std::string path = BuildPath(fn, base_dir);
  int fd = open(path.c_str(), O_RDWR | O_DIRECT | O_CREAT | O_EXCL, 0644);
  if (fd < 0) {
    return Status::IOError("failed to create file " + path, strerror(errno));
  }
  ssize_t n = pwrite(fd, EMPTY_META_BUF_INST.b, kBlockSize, kExtentDataSize);
  if (n < 0) {
    return Status::IOError("failed to write init meta block " + path,
                           strerror(errno));
  }
  *file_ptr =
      std::make_unique<ExtentFile>(fn, fd, kExtentDataSize + kBlockSize);
  return Status::OK();
}

auto ExtentFile::ReadValueAsync(UringIoEngine* io_engine, ValueAddr addr,
                                void* buf) -> std::unique_ptr<UringCmdFuture> {
  assert(is_aligned(reinterpret_cast<uint64_t>(buf), kBlockSize));
  return io_engine->Read(UringIoOptions{}, fd_, buf, addr.b_cnt() * kBlockSize,
                         addr.b_off() * kBlockSize);
}

Status ExtentFile::ReadValue(UringIoEngine* io_engine, ValueAddr addr,
                             void* buf) {
  auto f = ReadValueAsync(io_engine, addr, buf);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("read extent file failed " + file_name_.ToString(),
                           strerror(-f->Result()));
  }
  return Status::OK();
}

auto ExtentFile::ReadMetaAsync(UringIoEngine* io_engine,
                               void* buf) -> std::unique_ptr<UringCmdFuture> {
  assert(is_aligned(reinterpret_cast<uint64_t>(buf), kBlockSize));
  return io_engine->Read(UringIoOptions{}, fd_, buf, kBlockSize,
                         kExtentDataSize);
}

auto ExtentFile::WriteValueAsync(UringIoEngine* io_engine, void* buf,
                                 off64_t offset, size_t size)
    -> std::unique_ptr<UringCmdFuture> {
  assert(is_aligned(reinterpret_cast<uint64_t>(buf), kBlockSize));
  return io_engine->Write(UringIoOptions{}, fd_, buf, size, offset);
}

// auto ExtentFile::ReadValueIndexAsync(UringIoEngine* io_engine, void* buf)
//     -> std::unique_ptr<UringCmdFuture> {
//   assert(is_aligned(reinterpret_cast<uint64_t>(buf), kBlockSize));
//   return io_engine->Read(UringIoOptions{}, fd_, buf, value_index_size(),
//                          kExtentValueIndexOffset);
// }

Status ExtentFile::ReadValueIndex(UringIoEngine* io_engine, void* buf) {
  assert(is_aligned(reinterpret_cast<uint64_t>(buf), kBlockSize));
  auto f = io_engine->Read(UringIoOptions{}, fd_, buf, value_index_size(),
                           kExtentValueIndexOffset);
  // auto f = ReadValueIndexAsync(io_engine, buf);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("read extent index failed " + file_name_.ToString(),
                           strerror(-f->Result()));
  }
  return Status::OK();
}

Status ExtentFile::UpdateValueIndex(UringIoEngine* io_engine, ExtentMeta* meta,
                                    const ExtentValueIndex& index_block,
                                    void* buffer) {
  // 1. encode
  size_t n = kBlockSize + CalcValueIndexSize(index_block);
  memset(buffer, 0, n);
  char* cursor = static_cast<char*>(buffer) + kBlockSize;
  size_t block_inuse = 0;
  uint32_t base_alloc_block_off = 0;
  for (auto va : index_block) {
    va.EncodeTo(cursor);
    cursor += sizeof(ValueAddr);
    if (va.has_value()) {
      block_inuse += va.b_cnt();
      base_alloc_block_off =
          std::max(base_alloc_block_off, uint32_t(va.b_off() + va.b_cnt()));
    }
  }
  uint32_t value_index_checksum = Lower32of64(
      XXH3_64bits(static_cast<char*>(buffer) + kBlockSize, n - kBlockSize));
  cursor = static_cast<char*>(buffer);
  EncodeFixed32(cursor + 4, base_alloc_block_off);
  EncodeFixed32(cursor + 8, value_index_checksum);
  EncodeFixed32(cursor + 12, block_inuse);
  uint32_t meta_block_checksum =
      Lower32of64(XXH3_64bits(static_cast<char*>(buffer) + 4, kBlockSize - 4));
  EncodeFixed32(cursor, meta_block_checksum);
  // 2. lock and write
  {
    auto meta_lock = meta->lock_vi();
    auto f =
        io_engine->Write(UringIoOptions{}, fd_, buffer, n, kExtentDataSize);
    f->Wait();
    if (f->Result() < 0) {
      return Status::IOError(
          "write meta and value index failed: " + file_name_.ToString(),
          strerror(-f->Result()));
    }
    size_t new_file_size = kExtentDataSize + n;
    if (new_file_size < file_size_) {
      if (ftruncate64(fd_, file_size_) != 0) {
        return Status::IOError("ftruncate64 failed: " + file_name_.ToString(),
                               strerror(-f->Result()));
      }
    }
    file_size_ = new_file_size;
  }
  // 3. update meta
  ExtentMeta::MetaInfo mi;
  mi.fn_ = file_name_;
  mi.base_alloc_block_off_ = base_alloc_block_off;
  mi.value_index_checksum_ = value_index_checksum;
  mi.inuse_block_num_ = block_inuse;
  mi.meta_block_checksum_ = meta_block_checksum;
  meta->UpdateMeta(mi);
  return Status::OK();
}

}  // namespace HEAPKV_NS_V2