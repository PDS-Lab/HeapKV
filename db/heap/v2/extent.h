#pragma once

#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "db/heap/io_engine.h"
#include "db/heap/utils.h"
#include "rocksdb/status.h"
#include "util/coding_lean.h"

namespace HEAPKV_NS_V2 {

constexpr size_t kBlockSize = 512;
constexpr size_t kExtentDataSize = (32ul << 20ul) - kBlockSize;  // 32MiB
constexpr size_t kExtentBlockNum = kExtentDataSize / kBlockSize;
constexpr size_t kExtentValueIndexOffset = kExtentDataSize + kBlockSize;

struct ExtentFileName {
  uint32_t file_number_{0};
  uint32_t file_epoch_{0};
  ExtentFileName() = default;
  ExtentFileName(uint32_t file_number, uint32_t file_epoch)
      : file_number_(file_number), file_epoch_(file_epoch) {}
  std::string ToString() const {
    return ExtentFileName::ToString(file_number_, file_epoch_);
  }
  static std::string ToString(uint32_t file_number, uint32_t file_epoch) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%06u_%06u.heap", file_epoch, file_number);
    return buf;
  }
  static ExtentFileName FromString(const std::string& file_name) {
    uint32_t file_number, file_epoch;
    int n =
        sscanf(file_name.c_str(), "%06u_%06u.heap", &file_epoch, &file_number);
    assert(n == 2);
    return ExtentFileName(file_number, file_epoch);
  }
};

struct ValueAddr {
  uint16_t b_off_;
  uint16_t b_cnt_;
  ValueAddr() : b_off_(0), b_cnt_(0) {}
  ValueAddr(uint16_t off, uint16_t cnt) : b_off_(off), b_cnt_(cnt) {}
  uint16_t b_off() const { return b_off_; }
  uint16_t b_cnt() const { return b_cnt_; }
  bool has_value() const { return b_cnt() != 0; }
  char* EncodeTo(char* buf) {
    EncodeFixed16(buf, b_off_);
    EncodeFixed16(buf + 2, b_cnt_);
    return buf + 4;
  }
  static ValueAddr DecodeFrom(const char* buf) {
    return ValueAddr(DecodeFixed16(buf), DecodeFixed16(buf + 2));
  }
};

// struct ExtentMeta {
//   uint32_t value_index_checksum_;
//   uint32_t base_alloc_block_off_;
//   void EncodeTo(char* buf);
//   Status DecodeFrom(char* buf);
// };

using ExtentValueIndex = std::vector<ValueAddr>;
struct HeapValueIndex;

class ExtentFile {
  friend class std::unique_ptr<ExtentFile>;

 private:
  const ExtentFileName file_name_;
  int fd_;
  size_t file_size_;

 public:
  ExtentFile(ExtentFileName file_name, int fd, size_t file_size)
      : file_name_(file_name), fd_(fd), file_size_(file_size) {}
  ~ExtentFile() {
    if (fd_ > 0) close(fd_);
  }
  static std::string BuildPath(ExtentFileName file_name,
                               std::string_view base_dir);
  static void Remove(ExtentFileName fn, std::string_view base_dir) {
    unlink(BuildPath(fn, base_dir).c_str());
  }
  static Status Open(ExtentFileName fn, std::string_view base_dir,
                     std::unique_ptr<ExtentFile>* file_ptr);

  auto ReadValueAsync(UringIoEngine* io_engine, ValueAddr addr,
                      void* buf) -> std::unique_ptr<UringCmdFuture>;
  Status ReadValue(UringIoEngine* io_engine, ValueAddr addr, void* buf);

  auto ReadValueIndexAsync(UringIoEngine* io_engine,
                           void* buf) -> std::unique_ptr<UringCmdFuture>;
  Status ReadValueIndex(UringIoEngine* io_engine, void* buf);

  auto WriteValueAsync(UringIoEngine* io_engine, void* buf, off64_t offset,
                       size_t size) -> std::unique_ptr<UringCmdFuture>;

  ExtentFileName file_name() const { return file_name_; }
  size_t value_index_size() const {
    return file_size_ - kExtentValueIndexOffset;
  }

 private:
  Status ReflinkFrom(const ExtentFile* source_file);
  void UpdateFileSize(size_t file_size) { file_size_ = file_size; };
};

}  // namespace HEAPKV_NS_V2