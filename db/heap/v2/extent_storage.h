#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_set>

#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "db/column_family.h"
#include "db/heap/utils.h"
#include "db/heap/v2/extent.h"
#include "db/heap/v2/heap_value_index.h"
#include "memory/memory_allocator_impl.h"
#include "rocksdb/cache.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace HEAPKV_NS_V2 {

class HeapValueCacheKey : private CacheKey {
 private:
  // seqnumber of the key as we do in-place update in heapfile
  [[maybe_unused]] SequenceNumber seq_;
  [[maybe_unused]] uint32_t file_number_;
  [[maybe_unused]] uint32_t value_index_;

 public:
  HeapValueCacheKey(CacheKey cache_key, SequenceNumber seq,
                    uint32_t file_number, uint32_t value_index)
      : CacheKey(cache_key),
        seq_(seq),
        file_number_(file_number),
        value_index_(value_index) {}
};

struct HeapCacheData {
  CacheAllocationPtr allocation_;
  size_t size_;
  HeapCacheData(CacheAllocationPtr&& allocation, size_t size)
      : allocation_(std::move(allocation)), size_(size) {}
  // non copyable
  HeapCacheData(const HeapCacheData&) = delete;
  HeapCacheData& operator=(const HeapCacheData&) = delete;
  HeapCacheData(HeapCacheData&&) = default;
  HeapCacheData& operator=(HeapCacheData&&) = default;
  ~HeapCacheData() = default;
};

class ValueIndexCacheKey : private CacheKey {
 private:
  ExtentFileName fn_;

 public:
  ValueIndexCacheKey(CacheKey cache_key, ExtentFileName fn)
      : CacheKey(cache_key), fn_(fn) {}
};

class ExtentStorage;

class HeapValueGetContext {
  friend class ExtentStorage;

 private:
  Status status_;
  HeapValueIndex hvi_;
  SequenceNumber seq_;
  std::shared_ptr<ExtentFile> file_;
  // will be set if cache hit
  CacheHandleGuard<HeapCacheData> cache_guard_;
  std::unique_ptr<UringCmdFuture> future_;
  std::unique_ptr<uint8_t[], decltype(std::free)*> buffer_;

 public:
  HeapValueGetContext(Status s, HeapValueIndex hvi, SequenceNumber seq,
                      std::shared_ptr<ExtentFile> file,
                      std::unique_ptr<UringCmdFuture> future,
                      std::unique_ptr<uint8_t[], decltype(std::free)*> buffer)
      : status_(s),
        hvi_(hvi),
        seq_(seq),
        file_(std::move(file)),
        future_(std::move(future)),
        buffer_(std::move(buffer)) {}
  HeapValueGetContext(HeapValueIndex hvi, SequenceNumber seq,
                      std::shared_ptr<ExtentFile> file,
                      std::unique_ptr<UringCmdFuture> future,
                      std::unique_ptr<uint8_t[], decltype(std::free)*> buffer)
      : hvi_(hvi),
        seq_(seq),
        file_(std::move(file)),
        future_(std::move(future)),
        buffer_(std::move(buffer)) {}
  HeapValueGetContext(const HeapValueGetContext&) = delete;
  HeapValueGetContext& operator=(const HeapValueGetContext&) = delete;
  HeapValueGetContext(HeapValueGetContext&&) = default;
  HeapValueGetContext& operator=(HeapValueGetContext&&) = default;
  ~HeapValueGetContext() {
    if (future_ != nullptr) {
      future_->Wait();
    }
  };
  Status status() { return status_; }
  const HeapValueIndex& heap_value_index() const { return hvi_; }
  void SetCacheHandle(Cache* cache, Cache::Handle* handle) {
    cache_guard_ = CacheHandleGuard<HeapCacheData>(cache, handle);
  }
};

using ExtentSpace = std::pair<uint32_t, uint32_t>;
struct ExtentComp {
  bool operator()(const ExtentSpace& lhs, const ExtentSpace& rhs) const {
    if (lhs.second != rhs.second) {
      return lhs.second > rhs.second;
    }
    return lhs.first < rhs.first;
  }
};

class ExtentStorage {
 private:
  using ExtentList = std::array<ExtentMeta, 4096>;  // 32MiB * 4096 = 128GiB
  using SortSet = std::set<ExtentSpace, ExtentComp>;
  const ColumnFamilyData* cfd_;
  const std::string db_name_;
  const std::string base_extent_file_dir_;
  CacheKey hv_cache_key_;
  CacheKey vi_cache_key_;
  std::shared_ptr<Cache> heap_value_cache_;
  // 128GiB * 1024 = 128TiB, max data space, should be far from enough
  std::array<std::atomic<ExtentList*>, 1024> extents_;

  std::mutex mu_;  // for alloc and free
  uint32_t next_extent_file_number_;
  std::unordered_set<uint32_t> lock_map_;
  std::unordered_set<uint32_t> free_space_map_;

 public:
  static Status OpenStorage(std::string_view db_name, ColumnFamilyData* cfd,
                            std::unique_ptr<ExtentStorage>* storage);
  ExtentStorage(std::string_view db_name, ColumnFamilyData* cfd)
      : cfd_(cfd),
        db_name_(db_name),
        base_extent_file_dir_(std::string(db_name) + "/heapkv/"),
        heap_value_cache_(cfd->ioptions()->heap_value_cache) {
    if (heap_value_cache_) {
      hv_cache_key_ =
          CacheKey::CreateUniqueForCacheLifetime(heap_value_cache_.get());
      vi_cache_key_ =
          CacheKey::CreateUniqueForCacheLifetime(heap_value_cache_.get());
    }
  }
  const ColumnFamilyData* cfd() const { return cfd_; }
  const std::string& db_name() const { return db_name_; }
  const std::string& base_extent_file_dir() const {
    return base_extent_file_dir_;
  }
  ExtentMeta* GetExtentMeta(uint32_t file_number);
  Status GetValueAddr(UringIoEngine* io_engine, ExtentMeta* meta,
                      uint32_t value_index, std::shared_ptr<ExtentFile>* file,
                      ValueAddr* value_addr, size_t* issue_io);
  // fetch value
  auto GetHeapValueAsync(const ReadOptions& ro, UringIoEngine* io_engine,
                         const HeapValueIndex& hvi,
                         SequenceNumber seq) -> HeapValueGetContext;
  auto GetHeapValue(const ReadOptions& ro, UringIoEngine* io_engine,
                    const HeapValueIndex& hvi, SequenceNumber seq,
                    PinnableSlice* value) -> Status;
  auto WaitAsyncGet(const ReadOptions& ro, HeapValueGetContext ctx,
                    PinnableSlice* value) -> Status;
  auto GetValueIndexBlock(UringIoEngine* io_engine, ExtentMeta* meta,
                          std::shared_ptr<ExtentFile>* file,
                          PinnableSlice* value_index_block,
                          size_t* issue_io) -> Status;
  void UnlockExtent(uint32_t file_number, uint32_t alloc_off);
  // alloc things
  auto GetExtentForAlloc(ExtentMeta** meta, uint16_t min_free_block) -> Status;
  void EvictValueIndexCache(ExtentFileName file_name);
  // garbage collect
  bool LockExtentForGc(uint32_t file_number);

 private:
  bool ExtentCanAlloc(uint32_t alloc_off) const {
    return kExtentBlockNum - alloc_off >
           (kExtentBlockNum *
            cfd_->ioptions()->heap_extent_relocate_threshold) /
               2;
  }
};

}  // namespace HEAPKV_NS_V2