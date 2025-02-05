#pragma once
#include <db/heap/utils.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

#include "db/heap/v2/extent.h"
#include "util/coding.h"

namespace HEAPKV_NS_V2 {

struct HeapValueIndex {
  uint32_t file_number_{0};
  uint32_t file_epoch_{0};
  uint16_t value_index_{0};
  uint16_t b_off_{0};
  uint32_t value_size_{0};
  uint32_t value_checksum_{0};
  uint8_t compression_type_{0};
  HeapValueIndex() = default;
  HeapValueIndex(uint32_t file_number, uint32_t file_epoch,
                 uint16_t value_index, uint16_t b_off, uint32_t value_size,
                 uint32_t value_checksum, uint8_t compression_type)
      : file_number_(file_number),
        file_epoch_(file_epoch),
        value_index_(value_index),
        b_off_(b_off),
        value_size_(value_size),
        value_checksum_(value_checksum),
        compression_type_(compression_type) {}
  // void EncodeTo(char* dst);
  void EncodeTo(std::string* dst);
  static HeapValueIndex DecodeFrom(const Slice& raw);

  bool operator==(const HeapValueIndex& other) const {
    // value addr might change from time to time
    return value_checksum_ == other.value_checksum_ &&
           file_number_ == other.file_number_ &&
           value_index_ == other.value_index_;
  }

  bool operator!=(const HeapValueIndex& other) const {
    return !(*this == other);
  }
};

static constexpr size_t kIndexSize = sizeof(HeapValueIndex);

inline void HeapValueIndex::EncodeTo(std::string* dst) {
  PutVarint32(dst, file_number_);
  PutVarint32(dst, file_epoch_);
  PutFixed16(dst, value_index_);
  PutFixed16(dst, b_off_);
  PutVarint32(dst, value_size_);
  PutFixed32(dst, value_checksum_);
  dst->append(1, compression_type_);
}

inline HeapValueIndex HeapValueIndex::DecodeFrom(const Slice& raw) {
  Slice s = raw;
  uint32_t file_number = 0, file_epoch = 0, value_size = 0, value_checksum = 0;
  uint16_t value_index = 0, b_off = 0;
  uint8_t compression_type = 0;
  GetVarint32(&s, &file_number);
  GetVarint32(&s, &file_epoch);
  GetFixed16(&s, &value_index);
  GetFixed16(&s, &b_off);
  GetVarint32(&s, &value_size);
  GetFixed32(&s, &value_checksum);
  compression_type = s.data_[0];
  s.remove_prefix(1);
  return HeapValueIndex(file_number, file_epoch, value_index, b_off, value_size,
                        value_checksum, compression_type);
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
        "heap_value_index{{ .file_number:{}, vi: {}, size: {}, chksum: {} }}",
        hvi.file_number_, hvi.value_index_, hvi.value_size_,
        hvi.value_checksum_);
  }
};
}  // namespace std