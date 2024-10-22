#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "db/column_family.h"
#include "db/heap/extent.h"
#include "db/heap/heap_alloc_job.h"
#include "db/heap/heap_file.h"
#include "db/heap/heap_gc_job.h"
#include "db/heap/heap_value_index.h"
#include "db/heap/io_engine.h"
#include "memory/memory_allocator_impl.h"
#include "port/port_posix.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

// TODO(wnj): is 32B cache key too large?
class HeapValueCacheKey : private CacheKey {
 private:
  // seqnumber of the key as we do in-place update in heapfile
  SequenceNumber seq_;
  ext_id_t ext_id_;
  uint32_t block_offset_;

 public:
  HeapValueCacheKey(CacheKey cache_key, const HeapValueIndex& hvi)
      : CacheKey(cache_key),
        seq_(hvi.seq_num()),
        ext_id_(hvi.extent_number()),
        block_offset_(hvi.block_offset()) {}
  HeapValueCacheKey(CacheKey cache_key, SequenceNumber seq, ext_id_t ext_id,
                    uint32_t block_offset)
      : CacheKey(cache_key),
        seq_(seq),
        ext_id_(ext_id),
        block_offset_(block_offset) {}
  auto AsSlice() -> Slice {
    return Slice(reinterpret_cast<const char*>(this), sizeof(*this));
  }
};

struct HeapValueCacheData {
  CacheAllocationPtr allocation_;
  size_t size_;
  HeapValueCacheData(CacheAllocationPtr&& allocation, size_t size)
      : allocation_(std::move(allocation)), size_(size) {}
  // non copyable
  HeapValueCacheData(const HeapValueCacheData&) = delete;
  HeapValueCacheData& operator=(const HeapValueCacheData&) = delete;
  HeapValueCacheData(HeapValueCacheData&&) = default;
  HeapValueCacheData& operator=(HeapValueCacheData&&) = default;
  ~HeapValueCacheData() = default;
};

class CFHeapStorage;

class HeapValueGetContext {
  friend class CFHeapStorage;

 private:
  Status status_;
  HeapValueIndex hvi_;
  // will be set if cache hit
  CacheHandleGuard<HeapValueCacheData> cache_guard_;
  std::unique_ptr<UringCmdFuture> future_;
  std::unique_ptr<uint8_t[], decltype(std::free)*> buffer_;

 public:
  HeapValueGetContext(Status s, HeapValueIndex hvi,
                      std::unique_ptr<UringCmdFuture> future,
                      std::unique_ptr<uint8_t[], decltype(std::free)*> buffer)
      : status_(s),

        hvi_(hvi),
        future_(std::move(future)),
        buffer_(std::move(buffer)) {}
  HeapValueGetContext(HeapValueIndex hvi,
                      std::unique_ptr<UringCmdFuture> future,
                      std::unique_ptr<uint8_t[], decltype(std::free)*> buffer)
      : hvi_(hvi), future_(std::move(future)), buffer_(std::move(buffer)) {}
  HeapValueGetContext(const HeapValueGetContext&) = delete;
  HeapValueGetContext& operator=(const HeapValueGetContext&) = delete;
  HeapValueGetContext(HeapValueGetContext&&) = default;
  HeapValueGetContext& operator=(HeapValueGetContext&&) = default;
  ~HeapValueGetContext() = default;
  Status status() { return status_; }
  const HeapValueIndex& heap_value_index() const { return hvi_; }
  void SetCacheHandle(Cache* cache, Cache::Handle* handle) {
    cache_guard_ = CacheHandleGuard<HeapValueCacheData>(cache, handle);
  }
};

enum class HeapStorageJobType {
  Alloc,
  GC,
};

class CFHeapStorage {
 public:
  struct PendingHeapGarbageCommitJob {
    uint8_t count_down_;
    std::vector<ExtentGarbageSpan> garbage_;
    PendingHeapGarbageCommitJob(uint8_t count_down,
                                std::vector<ExtentGarbageSpan> garbage)
        : count_down_(count_down), garbage_(std::move(garbage)) {}
  };
  struct HeapGCArg {
    DBImpl* db_;
    std::atomic<bool>* shutting_down_;
    ColumnFamilyData* cfd_;
    CFHeapStorage* storage_;
    std::shared_ptr<PendingHeapGarbageCommitJob> job_;
    bool force_gc_;
  };

 private:
  const ColumnFamilyData* cfd_;
  CacheKey base_key_;
  bool stop_{false};
  uint64_t next_job_id_{0};
  uint64_t bg_running_alloc_jobs_{0};
  uint64_t bg_running_gc_jobs_{0};
  std::shared_ptr<Cache> heap_value_cache_;
  std::unique_ptr<HeapFile> heap_file_;
  std::unique_ptr<ExtentManager> extent_manager_;

  port::Mutex mu_;
  port::CondVar cv_;
  std::unordered_map<uint64_t, std::shared_ptr<PendingHeapGarbageCommitJob>>
      pending_free_jobs_;
  std::unordered_set<uint64_t> running_jobs_;

 public:
  CFHeapStorage(ColumnFamilyData* cfd, std::shared_ptr<Cache> heap_value_cache,
                std::unique_ptr<HeapFile> heap_file,
                std::unique_ptr<ExtentManager> extent_manager)
      : cfd_(cfd),
        heap_value_cache_(std::move(heap_value_cache)),
        heap_file_(std::move(heap_file)),
        extent_manager_(std::move(extent_manager)),
        cv_(&mu_) {
    if (heap_value_cache_ != nullptr) {
      base_key_ =
          CacheKey::CreateUniqueForCacheLifetime(heap_value_cache_.get());
    }
  }
  ~CFHeapStorage() { WaitAllJobDone(); }

  ExtentManager* extent_manager() const { return extent_manager_.get(); }

  bool HasCache() const { return heap_value_cache_ != nullptr; }

  static Status OpenOrCreate(const std::string& db_name, ColumnFamilyData* cfd,
                             std::unique_ptr<CFHeapStorage>* storage_handle);

  auto NewAllocJob() -> std::unique_ptr<HeapAllocJob> {
    uint64_t jid;
    {
      MutexLock lg(&mu_);
      jid = next_job_id_++;
      bg_running_alloc_jobs_++;
      running_jobs_.insert(jid);
    }
    return std::make_unique<HeapAllocJob>(jid, cfd_, extent_manager_.get());
  }

  auto NewGCJob(bool force_gc) -> std::unique_ptr<HeapGCJob> {
    uint64_t jid;
    {
      MutexLock lg(&mu_);
      if (!force_gc && bg_running_gc_jobs_ > 0) {
        return nullptr;
      }
      jid = next_job_id_++;
      bg_running_gc_jobs_++;
      running_jobs_.insert(jid);
    }
    return std::make_unique<HeapGCJob>(jid, cfd_, extent_manager_.get(),
                                       force_gc);
  }

  void CommitGarbageBlocks(const Compaction& compaction,
                           std::vector<ExtentGarbageSpan> garbage);

  auto NotifyFileDeletion(uint64_t file_number)
      -> std::shared_ptr<PendingHeapGarbageCommitJob>;

  auto GetHeapValueAsync(const ReadOptions& ro, UringIoEngine* io_engine,
                         const HeapValueIndex& hvi) -> HeapValueGetContext;
  auto GetHeapValue(const ReadOptions& ro, UringIoEngine* io_engine,
                    const HeapValueIndex& hvi, PinnableSlice* value) -> Status;
  auto WaitAsyncGet(const ReadOptions& ro, HeapValueGetContext ctx,
                    PinnableSlice* value) -> Status;
  void NotifyJobDone(uint64_t job_id, HeapStorageJobType type);
  void MarkStop();
  void WaitAllJobDone();

 private:
  auto NewCacheKey(const HeapValueIndex& hvi) -> HeapValueCacheKey {
    return HeapValueCacheKey(base_key_, hvi);
  }
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE