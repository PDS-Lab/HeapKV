#include "db/heap/heap_prefetch_helper.h"

#include <cassert>
#include <cstdint>

#include "db/heap/heap_storage.h"
#include "db/heap/heap_value_index.h"
#include "db/heap/io_engine.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/block.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

void HeapPrefetcher::PrefetchToCache(
    const InternalIteratorBase<IndexValue>* index_iter,
    const DataBlockIter* data_iter) {
  if (!heap_storage_ || !ro_.fill_cache || ro_.read_tier != kReadAllTier ||
      !heap_storage_->HasCache() || !index_iter->Valid() ||
      !data_iter->Valid() || !data_iter->IsHeapValueIndex() ||
      pending_bytes_ >= kMaxPrefetchBytes ||
      pending_prefetch_.size() >= kMaxPrefetchWindow) {
    return;
  }
  if (index_iter->value().handle != current_block_) {
    ClearPrefetch();
    current_block_ = index_iter->value().handle;
    current_prefetch_offset_ = -1;
  }
  uint32_t next_off = current_prefetch_offset_ == -1
                          ? data_iter->HeapValueIndexOffset()
                          : current_prefetch_offset_ + 1;
  while (pending_bytes_ < kMaxPrefetchBytes &&
         pending_prefetch_.size() < kMaxPrefetchWindow) {
    auto raw_hvi = data_iter->GetHeapValueIndex(next_off);
    if (!raw_hvi.has_value()) {
      break;
    }
    HeapValueIndex hvi;
    Status s = hvi.DecodeFrom(raw_hvi.value());
    assert(s.ok());
    auto ctx =
        heap_storage_->GetHeapValueAsync(ro_, GetThreadLocalIoEngine(), hvi);
    // std::cout << "Prefetch: " << hvi << std::endl;
    pending_bytes_ += hvi.block_cnt() * kHeapFileBlockSize;
    pending_prefetch_.emplace_back(std::move(ctx));
    current_prefetch_offset_ = next_off;
    next_off++;
  }
}

Status HeapPrefetcher::WaitPrefetch(const DataBlockIter* data_iter) {
  Status s;
  if (!data_iter->Valid() || !data_iter->IsHeapValueIndex() ||
      pending_prefetch_.empty()) {
    return s;
  }
  auto raw_hvi =
      data_iter->GetHeapValueIndex(data_iter->HeapValueIndexOffset());
  if (!raw_hvi.has_value()) {
    return s;
  }
  HeapValueIndex hvi;
  s = hvi.DecodeFrom(raw_hvi.value());
  if (!s.ok()) {
    return s;
  }
  bool found = false;
  while (!pending_prefetch_.empty() && !found) {
    HeapValueGetContext ctx = std::move(pending_prefetch_.front());
    pending_prefetch_.pop_front();
    pending_bytes_ -= ctx.heap_value_index().block_cnt() * kHeapFileBlockSize;
    found = ctx.heap_value_index() == hvi;
    PinnableSlice value;
    s = heap_storage_->WaitAsyncGet(ro_, std::move(ctx), &value);
    // std::cout << "Prefetch done: " << hvi << std::endl;
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

void HeapPrefetcher::ClearPrefetch() {
  while (!pending_prefetch_.empty()) {
    HeapValueGetContext ctx = std::move(pending_prefetch_.front());
    pending_prefetch_.pop_front();
    pending_bytes_ -= ctx.heap_value_index().block_cnt() * kHeapFileBlockSize;
    PinnableSlice value;
    heap_storage_->WaitAsyncGet(ro_, std::move(ctx), &value);
  }
  current_block_ = BlockHandle::NullBlockHandle();
  current_prefetch_offset_ = -1;
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE