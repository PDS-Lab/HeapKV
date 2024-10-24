#include "db/heap/v2/extent.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
// #include <cstdint>
#include <cstring>
#include <memory>

#include "db/heap/io_engine.h"
#include "rocksdb/status.h"
// #include "util/coding_lean.h"
// #include "util/hash.h"
// #include "util/xxhash.h"

namespace HEAPKV_NS_V2 {

// void ExtentMeta::EncodeTo(char* buf) {
//   memset(buf, 0, kBlockSize);
//   EncodeFixed32(buf + 4, value_index_checksum_);
//   EncodeFixed32(buf + 8, base_alloc_block_off_);
//   uint32_t checksum = Lower32of64(XXH3_64bits(buf + 4, kBlockSize - 4));
//   EncodeFixed32(buf, checksum);
// }

// Status ExtentMeta::DecodeFrom(char* buf) {
//   uint32_t checksum_from_disk = DecodeFixed32(buf);
//   uint32_t checksum = Lower32of64(XXH3_64bits(buf + 4, kBlockSize - 4));
//   if (checksum != checksum_from_disk) {
//     return Status::Corruption("checksum unmatch for extent meta");
//   }
//   value_index_checksum_ = DecodeFixed32(buf + 4);
//   base_alloc_block_off_ = DecodeFixed32(buf + 8);
//   return Status::OK();
// }

Status ExtentFile::Open(ExtentFileName fn, std::string_view base_dir,
                        std::unique_ptr<ExtentFile>* file_ptr) {
  std::string path = BuildPath(fn, base_dir).c_str();
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

auto ExtentFile::ReadValueIndexAsync(UringIoEngine* io_engine, void* buf)
    -> std::unique_ptr<UringCmdFuture> {
  assert(is_aligned(reinterpret_cast<uint64_t>(buf), kBlockSize));
  return io_engine->Read(UringIoOptions{}, fd_, buf, value_index_size(),
                         kExtentValueIndexOffset);
}

Status ExtentFile::ReadValueIndex(UringIoEngine* io_engine, void* buf) {
  auto f = ReadValueIndexAsync(io_engine, buf);
  f->Wait();
  if (f->Result() < 0) {
    return Status::IOError("read extent index failed " + file_name_.ToString(),
                           strerror(-f->Result()));
  }
  return Status::OK();
}

}  // namespace HEAPKV_NS_V2