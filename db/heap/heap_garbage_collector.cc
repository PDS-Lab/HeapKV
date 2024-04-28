#pragma once

#include "db/heap/heap_garbage_collector.h"

#include <algorithm>
#include <cstddef>

#include "db/dbformat.h"
#include "db/heap/heap_value_index.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

Status HeapGarbageCollector::InputKeyValue(const Slice& key,
                                           const Slice& value) {
  if (ExtractValueType(key) != ValueType::kTypeHeapValueIndex) {
    return Status::OK();
  }

  HeapValueIndex hvi;
  Status s = hvi.DecodeFrom(value);
  if (!s.ok()) {
    return s;
  }
  pending_hvi_.push_back(hvi);
  return Status::OK();
}

Status HeapGarbageCollector::OutputKeyValue(const Slice& key,
                                            const Slice& value) {
  if (ExtractValueType(key) != ValueType::kTypeHeapValueIndex) {
    return Status::OK();
  }
  HeapValueIndex hvi;
  Status s = hvi.DecodeFrom(value);
  if (!s.ok()) {
    return s;
  }
  if (pending_hvi_.back() != hvi) {
    return Status::Corruption("HeapValueIndex mismatch",
                              "HeapValueIndex mismatch");
  }
  pending_hvi_.pop_back();
  for (auto& p : pending_hvi_) {
    dropped_blocks_.push_back(GarbageBlocks{
        .extent_number_ = p.extent_number(),
        .block_offset_ = p.block_offset(),
        .block_cnt_ = p.block_cnt(),
    });
  }
  pending_hvi_.clear();
  return Status::OK();
}

auto HeapGarbageCollector::FinalizeDropResult() -> std::vector<GarbageBlocks> {
  if (dropped_blocks_.empty()) {
    return {};
  }
  // sort
  std::sort(dropped_blocks_.begin(), dropped_blocks_.end(),
            [](const GarbageBlocks& a, const GarbageBlocks& b) {
              if (a.extent_number_ == b.extent_number_) {
                return a.block_offset_ < b.block_offset_;
              }
              return a.extent_number_ < b.extent_number_;
            });
  // concat continuous blocks
  GarbageBlocks base_blocks = dropped_blocks_[0];
  size_t cur_pos = 0;
  for (size_t i = 1; i < dropped_blocks_.size(); i++) {
    auto& b = dropped_blocks_[i];
    if (base_blocks.extent_number_ == b.extent_number_ &&
        base_blocks.block_offset_ + base_blocks.block_cnt_ == b.block_offset_) {
      base_blocks.block_cnt_ += b.block_cnt_;
    } else {
      dropped_blocks_[cur_pos++] = base_blocks;
      base_blocks = b;
    }
  }
  dropped_blocks_[cur_pos++] = base_blocks;
  dropped_blocks_.resize(cur_pos);
  return std::move(dropped_blocks_);
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE