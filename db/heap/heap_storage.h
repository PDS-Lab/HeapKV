#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>

#include "cache/cache_key.h"
#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/heap/heap_alloc_job.h"
#include "db/heap/heap_file.h"
#include "db/heap/heap_value_index.h"
#include "db/heap/io_engine.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

// TODO(wnj): is 32B cache key too large?
class HeapValueCacheKey : private CacheKey {
 private:
  // seqnumber of the key as we do in-place update in heapfile
  [[maybe_unused]] SequenceNumber seq_;
  ext_id_t ext_id_;
  uint16_t block_offset_;
  [[maybe_unused]] char _reserved_[2]{0};  // pad to alignment to keep unique

 public:
  HeapValueCacheKey(CacheKey cache_key, SequenceNumber seq,
                    const HeapValueIndex& hvi)
      : CacheKey(cache_key),
        seq_(seq),
        ext_id_(hvi.extent_number()),
        block_offset_(hvi.block_offset()) {}
  HeapValueCacheKey(CacheKey cache_key, SequenceNumber seq, ext_id_t ext_id,
                    uint16_t block_offset)
      : CacheKey(cache_key),
        seq_(seq),
        ext_id_(ext_id),
        block_offset_(block_offset) {}
  auto AsSlice() -> Slice {
    return Slice(reinterpret_cast<const char*>(this), sizeof(*this));
  }
};

class CFHeapStorage {
 private:
  const ColumnFamilyData* cfd_;
  CacheKey base_key_;
  std::atomic_uint64_t next_job_id_{0};
  std::shared_ptr<Cache> heap_value_cache_;
  std::unique_ptr<HeapFile> heap_file_;
  std::unique_ptr<ExtentManager> extent_manager_;

 public:
  CFHeapStorage(ColumnFamilyData* cfd, std::shared_ptr<Cache> heap_value_cache,
                std::unique_ptr<HeapFile> heap_file,
                std::unique_ptr<ExtentManager> extent_manager)
      : cfd_(cfd),
        heap_value_cache_(std::move(heap_value_cache)),
        heap_file_(std::move(heap_file)),
        extent_manager_(std::move(extent_manager)) {
    if (heap_value_cache_ != nullptr) {
      base_key_ =
          CacheKey::CreateUniqueForCacheLifetime(heap_value_cache_.get());
    }
  }

  auto NewAllocJob() -> std::unique_ptr<HeapAllocJob> {
    return std::make_unique<HeapAllocJob>(
        next_job_id_.fetch_add(1, std::memory_order_relaxed), cfd_,
        extent_manager_.get());
  }

  auto GetHeapValueAsync(const ReadOptions& ro, const ParsedInternalKey& ikey,
                         const HeapValueIndex& hvi)
      -> std::unique_ptr<UringCmdFuture>;

 private:
  auto NewCacheKey(SequenceNumber seq,
                   const HeapValueIndex& hvi) -> HeapValueCacheKey {
    return HeapValueCacheKey(base_key_, seq, hvi);
  }
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE