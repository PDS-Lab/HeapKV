#pragma once
#include <cstddef>
#include <cstdint>
#include <ostream>

#include "db/heap/heap_file.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "util/coding.h"
#include "util/coding_lean.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

class HeapValueIndex {
 public:
  static constexpr size_t IndexSize = 32;

 private:
  ext_id_t extent_number_{0};
  uint32_t block_offset_{0};
  uint32_t block_cnt_{0};
  uint32_t value_size_{0};
  uint32_t value_checksum_{0};
  uint32_t value_uncompressed_size_{0};  // not used now
  uint64_t packed_seqnum_compression_type_{0};

 public:
  HeapValueIndex() = default;
  HeapValueIndex(SequenceNumber seq, ext_id_t extent_number,
                 uint32_t block_offset, uint32_t block_cnt, uint32_t value_size,
                 uint32_t value_checksum, CompressionType compression_type)
      : extent_number_(extent_number),
        block_offset_(block_offset),
        block_cnt_(block_cnt),
        value_size_(value_size),
        value_checksum_(value_checksum),
        value_uncompressed_size_(0),
        packed_seqnum_compression_type_(seq << 8 | compression_type) {}
  HeapValueIndex(const HeapValueIndex&) = default;
  HeapValueIndex& operator=(const HeapValueIndex&) = default;

  ext_id_t extent_number() const { return extent_number_; }
  uint32_t block_offset() const { return block_offset_; }
  uint32_t block_cnt() const { return block_cnt_; }
  uint32_t value_size() const { return value_size_; }
  uint32_t value_checksum() const { return value_checksum_; }
  CompressionType compression_type() const {
    return CompressionType(packed_seqnum_compression_type_ & 0xff);
  }
  uint64_t seq_num() const { return packed_seqnum_compression_type_ >> 8; }

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
    if (slice.size() < 24) {
      return Status::Corruption("Error while decoding heap value index",
                                "Slice size is less than 20");
    }
    extent_number_ = DecodeFixed32(slice.data());
    block_offset_ = DecodeFixed32(slice.data() + 4);
    block_cnt_ = DecodeFixed32(slice.data() + 8);
    value_size_ = DecodeFixed32(slice.data() + 12);
    value_checksum_ = DecodeFixed32(slice.data() + 16);
    value_uncompressed_size_ =
        DecodeFixed32(slice.data() + 20);  // not used now
    packed_seqnum_compression_type_ = DecodeFixed64(slice.data() + 24);
    return Status::OK();
  }

  void EncodeTo(std::string* dst) const {
    PutFixed32(dst, extent_number_);
    PutFixed32(dst, block_offset_);
    PutFixed32(dst, block_cnt_);
    PutFixed32(dst, value_size_);
    PutFixed32(dst, value_checksum_);
    PutFixed32(dst, value_uncompressed_size_);
    PutFixed64(dst, packed_seqnum_compression_type_);
  }

  void EncodeTo(char* dst) const {
    EncodeFixed32(dst, extent_number_);
    EncodeFixed32(dst + 4, block_offset_);
    EncodeFixed32(dst + 8, block_cnt_);
    EncodeFixed32(dst + 12, value_size_);
    EncodeFixed32(dst + 16, value_checksum_);
    EncodeFixed32(dst + 20, value_uncompressed_size_);
    EncodeFixed64(dst + 24, packed_seqnum_compression_type_);
  }

  bool operator==(const HeapValueIndex& other) const {
    return extent_number_ == other.extent_number_ &&
           block_offset_ == other.block_offset_ &&
           block_cnt_ == other.block_cnt_ &&
           packed_seqnum_compression_type_ ==
               other.packed_seqnum_compression_type_;
  }

  bool operator!=(const HeapValueIndex& other) const {
    return !(*this == other);
  }

  friend std::ostream& operator<<(std::ostream& os, const HeapValueIndex& hvi) {
    os << "HeapValueIndex{extent_number=" << hvi.extent_number_
       << ", block_offset=" << hvi.block_offset_
       << ", block_cnt=" << hvi.block_cnt_ << ", value_size=" << hvi.value_size_
       << ", value_checksum=" << hvi.value_checksum_
       << ", seq_num=" << hvi.seq_num()
       << ", compression_type=" << hvi.compression_type() << "}";
    return os;
  }
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE