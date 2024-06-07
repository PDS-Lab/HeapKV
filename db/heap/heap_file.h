#pragma once

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <memory>

#include "db/heap/extent.h"
#include "db/heap/io_engine.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

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
  auto FsyncAsync(UringIoEngine *io_engine, const UringIoOptions &opts,
                  bool datasync,
                  int fixed_fd_index = -1) -> std::unique_ptr<UringCmdFuture>;
  auto Fsync(UringIoEngine *io_engine, const UringIoOptions &opts,
             bool datasync, int fixed_fd_index = -1) -> Status;
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE