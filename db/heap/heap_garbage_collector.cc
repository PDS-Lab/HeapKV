#include "db/heap/heap_garbage_collector.h"

#include <algorithm>
#include <cstddef>

#include "db/dbformat.h"
#include "db/heap/extent.h"
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
    dropped_blocks_.push_back(ExtentGarbageSpan{
        {
            .block_off_ = p.block_offset(),
            .block_cnt_ = p.block_cnt(),
        },
        p.extent_number(),
    });
  }
  pending_hvi_.clear();
  return Status::OK();
}

auto HeapGarbageCollector::FinalizeDropResult()
    -> std::vector<ExtentGarbageSpan> {
  for (auto& p : pending_hvi_) {
    dropped_blocks_.push_back(ExtentGarbageSpan{
        {
            .block_off_ = p.block_offset(),
            .block_cnt_ = p.block_cnt(),
        },
        p.extent_number(),
    });
  }
  if (dropped_blocks_.empty()) {
    return {};
  }
  CompactDropResult(dropped_blocks_);
  return std::move(dropped_blocks_);
}

void HeapGarbageCollector::CompactDropResult(
    std::vector<ExtentGarbageSpan>& garbage) {
  std::sort(garbage.begin(), garbage.end(),
            [](const ExtentGarbageSpan& a, const ExtentGarbageSpan& b) {
              if (a.extent_number_ == b.extent_number_) {
                return a.block_off_ < b.block_off_;
              }
              return a.extent_number_ < b.extent_number_;
            });
  // concat continuous blocks
  ExtentGarbageSpan base_blocks = garbage[0];
  size_t cur_pos = 0;
  for (size_t i = 1; i < garbage.size(); i++) {
    auto& b = garbage[i];
    if (base_blocks.extent_number_ == b.extent_number_ &&
        base_blocks.block_off_ + base_blocks.block_cnt_ == b.block_off_) {
      base_blocks.block_cnt_ += b.block_cnt_;
    } else {
      garbage[cur_pos++] = base_blocks;
      base_blocks = b;
    }
  }
  garbage[cur_pos++] = base_blocks;
  garbage.resize(cur_pos);
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE