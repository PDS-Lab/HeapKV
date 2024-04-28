#pragma once
#include <cstdint>

#include "db/heap/heap_file.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

class HeapValueIndex {
 public:
 private:
  ext_id_t extent_number_{0};
  uint16_t block_offset_{0};
  uint16_t block_cnt_{0};
  uint32_t value_size_{0};
  uint32_t value_checksum_{0};
  CompressionType compression_type_{kNoCompression};
  char _reserved_[3]{0};

 public:
  HeapValueIndex() = default;
  HeapValueIndex(const HeapValueIndex&) = default;
  HeapValueIndex& operator=(const HeapValueIndex&) = default;

  ext_id_t extent_number() const { return extent_number_; }
  uint16_t block_offset() const { return block_offset_; }
  uint16_t block_cnt() const { return block_cnt_; }
  uint32_t value_size() const { return value_size_; }
  uint32_t value_checksum() const { return value_checksum_; }
  CompressionType compression_type() const { return compression_type_; }

  // void set_extent_number(ext_id_t extent_number) {
  //   extent_number_ = extent_number;
  // }
  // void set_block_offset(uint16_t block_offset) { block_offset_ =
  // block_offset; } void set_block_cnt(uint16_t block_cnt) { block_cnt_ =
  // block_cnt; } void set_value_size(uint32_t value_size) { value_size_ =
  // value_size; } void set_value_checksum(uint32_t value_checksum) {
  //   value_checksum_ = value_checksum;
  // }
  // void set_compression_type(CompressionType compression_type) {
  //   compression_type_ = compression_type;
  // }

  Status DecodeFrom(const Slice& slice) {
    if (slice.size() < 20) {
      return Status::Corruption("Error while decoding heap value index",
                                "Slice size is less than 20");
    }
    extent_number_ = DecodeFixed32(slice.data());
    block_offset_ = DecodeFixed16(slice.data() + 4);
    block_cnt_ = DecodeFixed16(slice.data() + 6);
    value_size_ = DecodeFixed32(slice.data() + 8);
    value_checksum_ = DecodeFixed32(slice.data() + 12);
    compression_type_ = static_cast<CompressionType>(slice.data()[16]);
    memset(_reserved_, 0, sizeof(_reserved_));
    return Status::OK();
  }

  void EncodeTo(std::string* dst) const {
    PutFixed32(dst, extent_number_);
    PutFixed16(dst, block_offset_);
    PutFixed16(dst, block_cnt_);
    PutFixed32(dst, value_size_);
    PutFixed32(dst, value_checksum_);
    dst->push_back(static_cast<char>(compression_type_));
    dst->append(_reserved_, sizeof(_reserved_));
  }

  void EncodeTo(char* dst) const {
    EncodeFixed32(dst, extent_number_);
    EncodeFixed16(dst + 4, block_offset_);
    EncodeFixed16(dst + 6, block_cnt_);
    EncodeFixed32(dst + 8, value_size_);
    EncodeFixed32(dst + 12, value_checksum_);
    dst[16] = static_cast<char>(compression_type_);
    memset(dst + 17, 0, sizeof(_reserved_));
  }

  bool operator==(const HeapValueIndex& other) const {
    return extent_number_ == other.extent_number_ &&
           block_offset_ == other.block_offset_ &&
           block_cnt_ == other.block_cnt_;
  }

  bool operator!=(const HeapValueIndex& other) const {
    return !(*this == other);
  }
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE