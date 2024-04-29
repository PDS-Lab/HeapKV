#include "db/heap/bitmap_allocator.h"

#include <cstdint>

#include "db/heap/utils.h"

namespace ROCKSDB_NAMESPACE {

namespace heapkv {

void SetBitMap(uint8_t *bm, uint32_t off, uint32_t n) {
  uint32_t i = off / 8;
  uint32_t off_in_byte = off % 8;

  uint32_t to_set = std::min(n, 8 - off_in_byte);
  uint8_t mask = one_seq[to_set].left >> off_in_byte;
  bm[i] |= mask;
  n -= to_set;
  i++;
  while (n > 0) {
    to_set = n >= 8 ? 8 : n;
    bm[i] |= one_seq[to_set].left;
    n -= to_set;
    i++;
  }
}

void UnSetBitMap(uint8_t *bm, uint32_t off, uint32_t n) {
  uint32_t i = off / 8;
  uint32_t off_in_byte = off % 8;

  uint32_t to_set = std::min(n, 8 - off_in_byte);
  uint8_t mask = one_seq[to_set].left >> off_in_byte;
  bm[i] &= ~mask;
  n -= to_set;
  i++;
  while (n > 0) {
    to_set = n >= 8 ? 8 : n;
    bm[i] &= ~one_seq[to_set].left;
    n -= to_set;
    i++;
  }
}

void BitMapAllocator::Init(uint16_t size, uint8_t *bm, bool empty_hint) {
  Reset();
  size_ = size;
  bm_ = bm;

  if (empty_hint) {
    current_alloc_seg_ = Segment(0, size * 8);
    return;
  }

  uint32_t c = 0;
  int32_t s = -1;
  for (uint16_t i = 0; i < size_; i++) {
    uint8_t lz = LeadingZero(bm_[i]);
    uint8_t tz = TailingZero(bm_[i]);
    if (s == -1) {
      if (tz > 0) {
        s = i * 8 + (8 - tz);
        c = tz;
      }
      continue;
    }
    c += lz;
    if (lz < 8) {
      if (c >= 8) {
        free_list_.emplace_back(s, s + c);
        total_free_bits_ += c;
      }
      s = tz > 0 ? i * 8 + (8 - tz) : -1;
      c = tz;
    }
  }

  if (s != -1 && c >= 8) {
    free_list_.emplace_back(s, s + c);
    total_free_bits_ += c;
  }

  std::make_heap(free_list_.begin(), free_list_.end());

  current_alloc_seg_ = PopHeap();
}

int32_t BitMapAllocator::Alloc(uint32_t n) {
  if (current_alloc_seg_.size() >= n) {
    int32_t start = current_alloc_seg_.start;
    current_alloc_seg_.start += n;
    SetBitMap(bm_, start, n);
    total_free_bits_ -= n;
    return start;
  } else {
    if (free_list_.empty() || free_list_.front().size() < n) {
      return -1;
    }
    if (current_alloc_seg_.size() > 0) {
      PushHeap(current_alloc_seg_);
    }
    current_alloc_seg_ = PopHeap();
    int32_t start = current_alloc_seg_.start;
    current_alloc_seg_.start += n;
    SetBitMap(bm_, start, n);
    total_free_bits_ -= n;
    return start;
  }
}

uint32_t BitMapAllocator::CalcApproximateFreeBits(uint8_t *bm, uint32_t size) {
  uint32_t c = 0;
  int32_t s = -1;
  uint32_t total = 0;
  for (uint16_t i = 0; i < size; i++) {
    uint8_t lz = LeadingZero(bm[i]);
    uint8_t tz = TailingZero(bm[i]);
    if (s == -1) {
      if (tz > 0) {
        s = i * 8 + (8 - tz);
        c = tz;
      }
      continue;
    }
    c += lz;
    if (lz < 8) {
      if (c >= 8) {
        total += c;
      }
      s = tz > 0 ? i * 8 + (8 - tz) : -1;
      c = tz;
    }
  }
  if (s != -1 && c >= 8) {
    total += c;
  }
  return total;
}

}  // namespace heapkv

}  // namespace ROCKSDB_NAMESPACE