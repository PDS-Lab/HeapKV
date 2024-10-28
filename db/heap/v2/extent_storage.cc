#include "db/heap/v2/extent_storage.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <shared_mutex>

#include "cache/cache_helpers.h"
#include "db/heap/io_engine.h"
#include "db/heap/v2/extent.h"
#include "memory/memory_allocator_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/hash.h"
#include "util/xxhash.h"

namespace HEAPKV_NS_V2 {

ExtentMeta* ExtentStorage::GetExtentMeta(uint32_t file_number) {
  uint32_t i1 = file_number / (sizeof(ExtentList) / sizeof(ExtentMeta));
  auto list = extents_[i1].load();
  if (list == nullptr) [[unlikely]] {
    ExtentList* l = new ExtentList();
    if (!extents_[i1].compare_exchange_strong(list, l)) [[unlikely]] {
      delete l;
    } else {
      list = l;
    }
  }
  return &list->at(file_number -
                   i1 * (sizeof(ExtentList) / sizeof(ExtentMeta)));
};

Status ExtentStorage::GetExtentFile(uint32_t file_number,
                                    std::shared_ptr<ExtentFile>* file) {
  auto extent_meta = GetExtentMeta(file_number);
  {
    std::shared_lock<std::shared_mutex> g(extent_meta->mu_);
    if (extent_meta->fn_.file_number_ != file_number) {
      return Status::Corruption("extent file not exist, filenumber corrupted?");
    }
    if (extent_meta->file_ == nullptr) {
      std::unique_ptr<ExtentFile> f;
      Status s = ExtentFile::Open(extent_meta->fn_, db_name_ + "/heap/", &f);
      if (!s.ok()) {
        return s;
      }
      extent_meta->file_.reset(f.release());
    }
    *file = extent_meta->file_;
  }
  return Status::OK();
}

Status ExtentStorage::GetValueAddr(UringIoEngine* io_engine, ExtentFile* file,
                                   uint32_t value_index,
                                   ValueAddr* value_addr) {
  PinnableSlice index;
  Status s = GetValueIndexBlock(io_engine, file, &index);
  if (!s.ok()) {
    return s;
  }
  *value_addr =
      ValueAddr::DecodeFrom(index.data() + value_index * sizeof(ValueAddr));
  return Status::OK();
}

auto ExtentStorage::GetHeapValueAsync(
    const ReadOptions& ro, UringIoEngine* io_engine,
    const HeapValueIndex& hvi) -> HeapValueGetContext {
  if (heap_value_cache_) {
    HeapValueCacheKey key(hv_cache_key_, hvi);
    auto handle = heap_value_cache_->Lookup(GetSliceForKey(&key));
    if (handle) {
      // ctx will have ownership of the cache handle
      auto ctx = HeapValueGetContext(hvi, nullptr, {nullptr, std::free});
      ctx.SetCacheHandle(heap_value_cache_.get(), handle);
      return ctx;
    }
  }

  const bool no_io = ro.read_tier == kBlockCacheTier;

  if (no_io) {
    return HeapValueGetContext(
        Status::Incomplete("Cannot read heap value: no disk I/O allowed"), hvi,
        nullptr, {nullptr, std::free});
  }
  std::shared_ptr<ExtentFile> file;
  Status s = GetExtentFile(hvi.extent_.file_number_, &file);
  if (!s.ok()) {
    return HeapValueGetContext(s, hvi, nullptr, {nullptr, std::free});
  }
  ValueAddr va = hvi.value_addr_;
  if (hvi.extent_.file_epoch_ != file->file_name().file_epoch_) {
    // gc happened, we need to fetch new value addr through value index
    s = GetValueAddr(io_engine, file.get(), hvi.value_index_, &va);
  }
  if (!s.ok()) {
    return HeapValueGetContext(s, hvi, nullptr, {nullptr, std::free});
  }
  void* ptr = std::aligned_alloc(kBlockSize, kBlockSize * va.b_cnt());

  auto f = file->ReadValueAsync(io_engine, va, ptr);
  return HeapValueGetContext(hvi, std::move(f),
                             std::unique_ptr<uint8_t[], decltype(std::free)*>(
                                 static_cast<uint8_t*>(ptr), std::free));
}

auto ExtentStorage::GetHeapValue(const ReadOptions& ro,
                                 UringIoEngine* io_engine,
                                 const HeapValueIndex& hvi,
                                 PinnableSlice* value) -> Status {
  auto ctx = GetHeapValueAsync(ro, io_engine, hvi);
  return WaitAsyncGet(ro, std::move(ctx), value);
}

auto ExtentStorage::WaitAsyncGet(const ReadOptions& ro, HeapValueGetContext ctx,
                                 PinnableSlice* value) -> Status {
  Status s = ctx.status();
  if (!s.ok()) {
    return s;
  }
  if (!ctx.cache_guard_.IsEmpty()) {
    value->Reset();
    value->PinSlice(Slice(ctx.cache_guard_.GetValue()->allocation_.get(),
                          ctx.cache_guard_.GetValue()->size_),
                    nullptr);
    ctx.cache_guard_.TransferTo(value);
    return Status::OK();
  }
  // wait io
  ctx.future_->Wait();
  if (ctx.future_->Result() < 0) {
    s = Status::IOError("Failed to read heap value",
                        strerror(-ctx.future_->Result()));
    return s;
  }
  if (ro.verify_checksums) {
    // verify checksum
    uint32_t checksum = Lower32of64(
        XXH3_64bits(ctx.buffer_.get(), ctx.heap_value_index().value_checksum_));
    if (checksum != ctx.heap_value_index().value_checksum_) {
      s = Status::Corruption("Checksum mismatch");
      return s;
    }
  }
  // TODO(wnj): decompress the value
  if (heap_value_cache_ && ro.fill_cache) {
    // cache the value
    HeapValueCacheKey key(hv_cache_key_, ctx.heap_value_index());
    auto cache_ptr = AllocateAndCopyBlock(
        Slice(reinterpret_cast<const char*>(ctx.buffer_.get()),
              ctx.heap_value_index().value_size_),
        heap_value_cache_->memory_allocator());
    auto cache_obj = std::make_unique<HeapCacheData>(
        std::move(cache_ptr), ctx.heap_value_index().value_size_);

    Cache::Handle* cache_handle = nullptr;
    s = heap_value_cache_->Insert(
        GetSliceForKey(&key), cache_obj.get(),
        BasicTypedCacheHelper<HeapCacheData,
                              CacheEntryRole::kHeapValue>::GetBasicHelper(),
        cache_obj->size_ + sizeof(*cache_obj), &cache_handle,
        Cache::Priority::BOTTOM);
    if (s.ok()) {
      auto _ = cache_obj.release();
      auto guard = CacheHandleGuard<HeapCacheData>(heap_value_cache_.get(),
                                                   cache_handle);
      value->Reset();
      value->PinSlice(
          Slice(guard.GetValue()->allocation_.get(), guard.GetValue()->size_),
          nullptr);
      guard.TransferTo(value);
    }
    return s;
  }
  // pin owned buffer, release buffer in ctx to avoid double free
  auto buffer = ctx.buffer_.release();
  value->Reset();
  value->PinSlice(
      Slice(reinterpret_cast<const char*>(buffer),
            ctx.heap_value_index().value_size_),
      [](void* arg1, void*) { free(arg1); }, buffer, nullptr);
  return s;
}

auto ExtentStorage::GetValueIndexBlock(
    UringIoEngine* io_engine, ExtentFile* file,
    PinnableSlice* value_index_block) -> Status {
  ValueIndexCacheKey key(vi_cache_key_, file->file_name());
  if (heap_value_cache_) {
    auto handle = heap_value_cache_->Lookup(GetSliceForKey(&key));
    if (handle != nullptr) {
      auto cache_guard =
          CacheHandleGuard<HeapCacheData>(heap_value_cache_.get(), handle);
      value_index_block->Reset();
      value_index_block->PinSlice(
          Slice(cache_guard.GetValue()->allocation_.get(),
                cache_guard.GetValue()->size_),
          nullptr);
      cache_guard.TransferTo(value_index_block);
      return Status::OK();
    }
  }
  size_t n = file->value_index_size();
  void* ptr = std::aligned_alloc(kBlockSize, n);
  if (ptr == nullptr) {
    return Status::MemoryLimit("failed to alloc value index read buffer");
  }
  auto g = std::unique_ptr<uint8_t[], decltype(std::free)*>(
      static_cast<uint8_t*>(ptr), std::free);
  Status s = file->ReadValueIndex(io_engine, ptr);
  if (!s.ok()) {
    return s;
  }
  if (heap_value_cache_) {
    auto cap = AllocateAndCopyBlock(
        Slice(static_cast<char*>(ptr), n),
        heap_value_cache_ ? heap_value_cache_->memory_allocator() : nullptr);
    auto cache_obj = std::make_unique<HeapCacheData>(std::move(cap), n);
    Cache::Handle* cache_handle = nullptr;
    s = heap_value_cache_->Insert(
        GetSliceForKey(&key), cache_obj.get(),
        BasicTypedCacheHelper<HeapCacheData,
                              CacheEntryRole::kHeapValue>::GetBasicHelper(),
        n + sizeof(*cache_obj), &cache_handle, Cache::Priority::HIGH);
    if (!s.ok()) {
      return s;
    }
    cache_obj.release();
    auto cache_guard =
        CacheHandleGuard<HeapCacheData>(heap_value_cache_.get(), cache_handle);
    value_index_block->Reset();
    value_index_block->PinSlice(Slice(cache_guard.GetValue()->allocation_.get(),
                                      cache_guard.GetValue()->size_),
                                nullptr);
    cache_guard.TransferTo(value_index_block);
  } else {
    g.release();
    value_index_block->Reset();
    value_index_block->PinSlice(
        Slice(reinterpret_cast<const char*>(ptr), n),
        [](void* arg1, void*) { free(arg1); }, ptr, nullptr);
  }
  return Status::OK();
}

}  // namespace HEAPKV_NS_V2