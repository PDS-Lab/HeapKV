#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>

#include "db/heap/heap_storage.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class DataBlockIter;
class IndexBlockIter;

namespace heapkv {

class HeapPrefetcher {
 public:
  static constexpr size_t kMaxPrefetchBytes = 256 * 1024;
  static constexpr size_t kMaxPrefetchWindow = 16;

 private:
  const ReadOptions& ro_;
  CFHeapStorage* heap_storage_;
  std::deque<HeapValueGetContext> pending_prefetch_;
  size_t pending_bytes_{0};
  BlockHandle current_block_{0, 0};
  int32_t current_prefetch_offset_ = -1;

 public:
  using PrefetchHandle = decltype(pending_prefetch_)::const_reference;

 public:
  HeapPrefetcher(const ReadOptions& ro, CFHeapStorage* heap_storage)
      : ro_(ro), heap_storage_(heap_storage) {}
  ~HeapPrefetcher() { ClearPrefetch(); }
  void PrefetchToCache(const InternalIteratorBase<IndexValue>* index_iter,
                       const DataBlockIter* data_iter);
  Status WaitPrefetch(const DataBlockIter* data_iter);
  void ClearPrefetch();
  // Status WaitPrefetch
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE