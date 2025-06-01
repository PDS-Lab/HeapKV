#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "db/heap/utils.h"
#include "db/heap/v2/extent.h"
#include "db/heap/v2/heap_value_index.h"
#include "table/internal_iterator.h"
#include "util/autovector.h"

namespace HEAPKV_NS_V2 {

struct ExtentGarbage {
  uint16_t b_cnt_{0};
  std::vector<std::vector<uint32_t>> value_index_list_;
};

using CompactionHeapGarbage = std::unordered_map<uint32_t, ExtentGarbage>;

void MergeGarbage(CompactionHeapGarbage* base,
                  CompactionHeapGarbage* merge_to_base);
// A class co-work with compaction process to collect dropped HeapValueIndex
class HeapGarbageCollector {
 private:
  CompactionHeapGarbage garbage_;
  autovector<HeapValueIndex, 16> pending_hvi_;

 public:
  void InputKeyValue(const Slice& key, const Slice& value);
  Status OutputKeyValue(const Slice& key, const Slice& value);
  auto FinalizeDropResult() -> CompactionHeapGarbage;
};

// An internal iterator that passes each key-value encountered to
// HeapGarbageCollector
class HeapValueGarbageCheckIterator : public InternalIterator {
 private:
  InternalIterator* iter_;
  HeapGarbageCollector* garbage_collector_;
  Status status_;

 public:
  HeapValueGarbageCheckIterator(InternalIterator* iter,
                                HeapGarbageCollector* garbage_collector)
      : iter_(iter), garbage_collector_(garbage_collector) {
    assert(iter_);
    assert(garbage_collector_);
  }

  bool Valid() const override { return iter_->Valid() && status_.ok(); }

  void SeekToFirst() override {
    iter_->SeekToFirst();
    UpdateAndCountHeapValueIfNeeded();
  }

  void SeekToLast() override {
    iter_->SeekToLast();
    UpdateAndCountHeapValueIfNeeded();
  }

  void Seek(const Slice& target) override {
    iter_->Seek(target);
    UpdateAndCountHeapValueIfNeeded();
  }

  void SeekForPrev(const Slice& target) override {
    iter_->SeekForPrev(target);
    UpdateAndCountHeapValueIfNeeded();
  }

  void Next() override {
    assert(Valid());

    iter_->Next();
    UpdateAndCountHeapValueIfNeeded();
  }

  bool NextAndGetResult(IterateResult* result) override {
    assert(Valid());

    const bool res = iter_->NextAndGetResult(result);
    UpdateAndCountHeapValueIfNeeded();
    return res;
  }

  void Prev() override {
    assert(Valid());

    iter_->Prev();
    UpdateAndCountHeapValueIfNeeded();
  }

  Slice key() const override {
    assert(Valid());
    return iter_->key();
  }

  Slice user_key() const override {
    assert(Valid());
    return iter_->user_key();
  }

  Slice value() const override {
    assert(Valid());
    return iter_->value();
  }

  Status status() const override { return status_; }

  bool PrepareValue() override {
    assert(Valid());
    return iter_->PrepareValue();
  }

  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return iter_->MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(Valid());
    return iter_->UpperBoundCheckResult();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool IsKeyPinned() const override {
    assert(Valid());
    return iter_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(Valid());
    return iter_->IsValuePinned();
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return iter_->GetProperty(prop_name, prop);
  }

  bool IsDeleteRangeSentinelKey() const override {
    return iter_->IsDeleteRangeSentinelKey();
  }

 private:
  void UpdateAndCountHeapValueIfNeeded() {
    assert(!iter_->Valid() || iter_->status().ok());
    if (!iter_->Valid()) {
      status_ = iter_->status();
      return;
    }
    garbage_collector_->InputKeyValue(key(), value());
  }
};

}  // namespace HEAPKV_NS_V2