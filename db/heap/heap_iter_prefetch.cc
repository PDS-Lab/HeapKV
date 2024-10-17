#include "db/heap/heap_iter_prefetch.h"

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>

#include "db/db_iter.h"
#include "db/heap/heap_value_index.h"
#include "db/heap/io_engine.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

PrefetchGroup::~PrefetchGroup() {
  prefetch_list_.clear();  // done all future;
  for (auto p : used_buffer_) {
    free(p);
  }
  for (auto p : free_buffer_) {
    free(p);
  }
  for (auto p : large_buffer_) {
    free(p);
  }
}

void PrefetchGroup::Reset() {
  prefetch_list_.clear();  // done all future
  for (auto p : large_buffer_) {
    free(p);
  }
  large_buffer_.clear();
  for (auto p : used_buffer_) {
    free_buffer_.push_back(p);
  }
  used_buffer_.clear();
  if (current_buffer_ != nullptr) {
    free_buffer_.push_back(current_buffer_);
    current_buffer_ = nullptr;
  }
  buffer_off_ = 0;
  iter_pos_ = 0;
  heap_value_.Reset();
}

Status PrefetchGroup::DoPrefetch(const ReadOptions &ro,
                                 UringIoEngine *io_engine, DBIter *iter,
                                 size_t batch) {
  Status s = Status::OK();
  if (!iter->Valid()) {
    return s;
  }
  for (size_t i = 0; i < batch && iter->Valid() && s.ok(); i++, iter->Next()) {
    s = AppendUnit(ro, io_engine, iter->key(), iter->value(),
                   iter->IsHeapValue());
  }
  valid_ = s.ok() && !prefetch_list_.empty();
  return s;
}

Status PrefetchGroup::AppendUnit(const ReadOptions &ro,
                                 UringIoEngine *io_engine, const Slice user_key,
                                 const Slice value, const bool is_heap_value) {
  Status s = Status::OK();
  size_t space = user_key.size() + value.size();
  char *ptr = ReserveSpace(space);
  if (ptr == nullptr) {
    s = Status::MemoryLimit("no enough memory for PrefetchGroup");
  }
  if (s.ok()) {
    memcpy(ptr, user_key.data(), user_key.size());
    memcpy(ptr + user_key.size(), value.data(), value.size());
    PrefetchUnit pu;
    pu.user_key_ = Slice(ptr, user_key.size());
    pu.value_ = Slice(ptr + user_key.size(), value.size());
    if (is_heap_value) {
      HeapValueIndex hvi;
      s = hvi.DecodeFrom(value);
      if (!s.ok()) {
        return s;
      }
      auto getCtx = heap_storage_->GetHeapValueAsync(ro, io_engine, hvi);
      pu.async_ctx_ = std::make_unique<decltype(getCtx)>(std::move(getCtx));
    }
    prefetch_list_.push_back(std::move(pu));
  }
  return s;
}

char *PrefetchGroup::ReserveSpace(size_t n) {
  if (n > BUFFER_SIZE) {
    auto ptr = static_cast<char *>(malloc(n));
    if (ptr != nullptr) {
      large_buffer_.push_back(ptr);
    }
    return ptr;
  }

  if (current_buffer_ == nullptr || (buffer_off_ + n >= BUFFER_SIZE)) {
    if (!free_buffer_.empty()) {
      current_buffer_ = free_buffer_.back();
      free_buffer_.pop_back();
    } else {
      current_buffer_ =
          static_cast<char *>(std::aligned_alloc(4096, BUFFER_SIZE));
      if (current_buffer_ == nullptr) {
        return nullptr;
      }
    }
    buffer_off_ = 0;
    used_buffer_.push_back(current_buffer_);
  }
  auto pos = current_buffer_ + buffer_off_;
  buffer_off_ += n;
  return pos;
}

Status PrefetchGroup::Next(const ReadOptions &ro) {
  heap_value_.Reset();
  Status s = Status::OK();
  if (iter_pos_ >= prefetch_list_.size()) {
    valid_ = false;
    return s;
  }
  if (prefetch_list_[iter_pos_].async_ctx_ != nullptr) {
    s = heap_storage_->WaitAsyncGet(
        ro, std::move(*prefetch_list_[iter_pos_].async_ctx_), &heap_value_);
  }
  iter_pos_++;
  return s;
}

Status HeapIterPrefetcher::Next() {
  auto &pg = GetGroup();
  Status s = pg.Next(ro_);
  if (pg.Valid()) {
    return s;
  }
  // group done, switch group
  if (!GetNextGroup().Valid()) {
    return s;
  }
  current_group_++;
  s = GetGroup().StartIter(ro_);
  if (s.ok()) {
    current_batch_ = std::min(current_batch_ * 2, MAX_DEPTH);
    s = GetNextGroup().DoPrefetch(ro_, GetThreadLocalIoEngine(), iter_,
                                  current_batch_);
  }
  return s;
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE