#pragma once
#include <db/heap/utils.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

#include "db/heap/v2/extent.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/types.h"
#include "util/coding.h"
#include "util/coding_lean.h"

namespace HEAPKV_NS_V2 {

struct HeapValueIndex {
  uint32_t file_number_{0};
  // use value index to search in value index block of extent file
  uint32_t value_index_{0};
  uint32_t value_size_{0};
  uint32_t value_checksum_{0};
  uint64_t packed_seqnum_compression_type_{0};
  HeapValueIndex() = default;
  HeapValueIndex(uint32_t file_number, uint32_t value_index,
                 uint32_t value_size, uint32_t value_checksum,
                 uint64_t packed_seqnum_compression_type)
      : file_number_(file_number),
        value_index_(value_index),
        value_size_(value_size),
        value_checksum_(value_checksum),
        packed_seqnum_compression_type_(packed_seqnum_compression_type) {}
  HeapValueIndex(uint32_t file_number, uint32_t value_index,
                 uint32_t value_size, uint32_t value_checksum,
                 SequenceNumber seq, CompressionType compression_type)
      : file_number_(file_number),
        value_index_(value_index),
        value_size_(value_size),
        value_checksum_(value_checksum),
        packed_seqnum_compression_type_(seq << 8 | compression_type) {}
  CompressionType compression_type() const {
    return CompressionType(packed_seqnum_compression_type_ & 0xff);
  }
  uint64_t seq_num() const { return packed_seqnum_compression_type_ >> 8; }
  // void EncodeTo(char* dst);
  void EncodeTo(std::string* dst);
  static HeapValueIndex DecodeFrom(const Slice& raw);

  bool operator==(const HeapValueIndex& other) const {
    // value addr might change from time to time
    return value_checksum_ == other.value_checksum_ &&
           file_number_ == other.file_number_ &&
           value_index_ == other.value_index_ && seq_num() == other.seq_num();
  }

  bool operator!=(const HeapValueIndex& other) const {
    return !(*this == other);
  }
};

static constexpr size_t kIndexSize = sizeof(HeapValueIndex);

// inline void HeapValueIndex::EncodeTo(char* dst) {
//   EncodeFixed32(dst, extent_.file_number_);
//   dst += 4;
//   EncodeFixed32(dst, extent_.file_epoch_);
//   dst += 4;
//   EncodeFixed16(dst, value_addr_.b_off());
//   dst += 2;
//   EncodeFixed16(dst, value_addr_.b_cnt());
//   dst += 2;
//   EncodeFixed32(dst, value_index_);
//   dst += 4;
//   EncodeFixed32(dst, value_size_);
//   dst += 4;
//   EncodeFixed32(dst, value_checksum_);
//   dst += 4;
//   EncodeFixed64(dst, packed_seqnum_compression_type_);
// }

inline void HeapValueIndex::EncodeTo(std::string* dst) {
  PutFixed32(dst, value_checksum_);
  PutVarint32(dst, file_number_);
  PutVarint32(dst, value_index_);
  PutVarint32(dst, value_size_);
  PutVarint64(dst, packed_seqnum_compression_type_);
  // PutFixed32(dst, extent_.file_number_);
  // PutFixed32(dst, extent_.file_epoch_);
  // PutFixed16(dst, value_addr_.b_off());
  // PutFixed16(dst, value_addr_.b_cnt());
  // PutFixed32(dst, value_index_);
  // PutFixed32(dst, value_size_);
  // PutFixed32(dst, value_checksum_);
  // PutFixed64(dst, packed_seqnum_compression_type_);
}

inline HeapValueIndex HeapValueIndex::DecodeFrom(const Slice& raw) {
  // assert(raw.size() == kIndexSize);
  // const char* cursor = raw.data();
  // ExtentFileName fn(DecodeFixed32(cursor), DecodeFixed32(cursor + 4));
  // cursor += 8;
  // ValueAddr va(DecodeFixed16(cursor), DecodeFixed16(cursor + 2));
  // cursor += 4;
  // uint32_t value_index = DecodeFixed32(cursor);
  // cursor += 4;
  // uint32_t value_size = DecodeFixed32(cursor);
  // cursor += 4;
  // uint32_t value_checksum = DecodeFixed32(cursor);
  // cursor += 4;
  // uint64_t packed_seqnum_compression_type = DecodeFixed64(cursor);
  Slice s = raw;
  uint32_t file_number = 0, value_index = 0, value_size = 0, value_checksum = 0;
  GetFixed32(&s, &value_checksum);
  GetVarint32(&s, &file_number);
  GetVarint32(&s, &value_index);
  GetVarint32(&s, &value_size);
  uint64_t packed_seqnum_compression_type = 0;
  GetVarint64(&s, &packed_seqnum_compression_type);
  return HeapValueIndex(file_number, value_index, value_size, value_checksum,
                        packed_seqnum_compression_type);
}

}  // namespace HEAPKV_NS_V2

namespace std {
// write formatter for error_code
template <>
struct formatter<HEAPKV_NS_V2::HeapValueIndex> {
  constexpr auto parse(std::format_parse_context& ctx) { return ctx.begin(); }
  auto format(const HEAPKV_NS_V2::HeapValueIndex& hvi,
              std::format_context& ctx) const {
    return format_to(
        ctx.out(),
        "heap_value_index{{ .file_number:{}, vi: {}, size: {}, seq: {} }}",
        hvi.file_number_, hvi.value_index_, hvi.value_size_, hvi.seq_num());
  }
};
}  // namespace std