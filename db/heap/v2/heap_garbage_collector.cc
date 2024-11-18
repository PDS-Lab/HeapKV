

#include "db/heap/v2/heap_garbage_collector.h"

#include "db/dbformat.h"
#include "db/heap/v2/heap_value_index.h"
#include "rocksdb/status.h"

namespace HEAPKV_NS_V2 {

void HeapGarbageCollector::InputKeyValue(const Slice& key, const Slice& value) {
  if (ExtractValueType(key) != ValueType::kTypeHeapValueIndex) {
    return;
  }
  pending_hvi_.push_back(HeapValueIndex::DecodeFrom(value));
}

Status HeapGarbageCollector::OutputKeyValue(const Slice& key,
                                            const Slice& value) {
  if (ExtractValueType(key) != ValueType::kTypeHeapValueIndex) {
    return Status::OK();
  }
  auto hvi = HeapValueIndex::DecodeFrom(value);

  if (pending_hvi_.back() != hvi) {
    return Status::Corruption("HeapValueIndex mismatch");
  }
  pending_hvi_.pop_back();
  for (auto& p : pending_hvi_) {
    auto it = garbage_.find(p.extent_.file_number_);
    if (it == garbage_.end()) {
      it = garbage_.emplace(p.extent_.file_number_, ExtentGarbage{}).first;
    }
    it->second.b_cnt_ += p.value_addr_.b_cnt_;
    it->second.value_index_list_.push_back(p.value_index_);
  }
  pending_hvi_.clear();
  return Status::OK();
}

auto HeapGarbageCollector::FinalizeDropResult() -> CompactionHeapGarbage {
  for (auto& p : pending_hvi_) {
    auto it = garbage_.find(p.extent_.file_number_);
    if (it == garbage_.end()) {
      it = garbage_.emplace(p.extent_.file_number_, ExtentGarbage{}).first;
    }
    it->second.b_cnt_ += p.value_addr_.b_cnt_;
    it->second.value_index_list_.push_back(p.value_index_);
  }
  if (garbage_.empty()) {
    return {};
  }
  return std::move(garbage_);
}

void MergeGarbage(CompactionHeapGarbage* base,
                  CompactionHeapGarbage* merge_to_base) {
  if (merge_to_base->empty()) {
    return;
  }
  for (auto& [fn, g] : *merge_to_base) {
    auto it = base->find(fn);
    if (it != base->end()) {
      it->second.b_cnt_ += g.b_cnt_;
      it->second.value_index_list_.reserve(it->second.value_index_list_.size() +
                                           g.value_index_list_.size());
      it->second.value_index_list_.insert(it->second.value_index_list_.end(),
                                          g.value_index_list_.begin(),
                                          g.value_index_list_.end());
    } else {
      base->emplace(fn, std::move(g));
    }
  }
}

}  // namespace HEAPKV_NS_V2