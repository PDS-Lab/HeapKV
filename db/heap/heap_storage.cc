#include "db/heap/heap_storage.h"

#include "cache/cache_helpers.h"
#include "cache/typed_cache.h"
#include "db/heap/heap_file.h"
#include "db/heap/io_engine.h"
#include "memory/memory_allocator_impl.h"
#include "rocksdb/cache.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

auto CFHeapStorage::GetHeapValueAsync(const ReadOptions& ro,
                                      UringIoEngine* io_engine,
                                      const ParsedInternalKey& ikey,
                                      const HeapValueIndex& hvi)
    -> HeapValueGetContext {
  if (heap_value_cache_) {
    HeapValueCacheKey key = NewCacheKey(ikey.sequence, hvi);
    auto handle = heap_value_cache_->Lookup(key.AsSlice());
    if (handle) {
      // ctx will have ownership of the cache handle
      auto ctx = HeapValueGetContext(ikey.sequence, hvi, nullptr,
                                     {nullptr, std::free});
      ctx.SetCacheHandle(heap_value_cache_.get(), handle);
      return ctx;
    }
  }

  const bool no_io = ro.read_tier == kBlockCacheTier;

  if (no_io) {
    return HeapValueGetContext(ikey.sequence, hvi, nullptr,
                               {nullptr, std::free});
  }

  void* ptr = nullptr;
  posix_memalign(&ptr, kHeapFileBlockSize,
                 kHeapFileBlockSize * hvi.block_cnt());
  auto f = heap_file_->GetHeapValueAsync(
      io_engine, UringIoOptions(), hvi.extent_number(), hvi.block_offset(),
      hvi.block_cnt(), static_cast<uint8_t*>(ptr));
  return HeapValueGetContext(ikey.sequence, hvi, std::move(f),
                             std::unique_ptr<uint8_t[], decltype(std::free)*>(
                                 static_cast<uint8_t*>(ptr), std::free));
}

auto CFHeapStorage::GetHeapValue(const ReadOptions& ro,
                                 UringIoEngine* io_engine,
                                 const ParsedInternalKey& ikey,
                                 const HeapValueIndex& hvi,
                                 PinnableSlice* value) -> Status {
  auto ctx = GetHeapValueAsync(ro, io_engine, ikey, hvi);
  return WaitAsyncGet(ro, std::move(ctx), value);
}

auto CFHeapStorage::WaitAsyncGet(const ReadOptions& ro, HeapValueGetContext ctx,
                                 PinnableSlice* value) -> Status {
  Status s;
  if (ctx.IsEmptyCtx()) {
    // no io due to read_tier option
    s = Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
    return s;
  }
  if (!ctx.cache_guard_.IsEmpty()) {
    // we get the value from cache
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
    uint32_t checksum =
        Lower32of64(XXH3_64bits(ctx.buffer_.get(), ctx.hvi_.value_size()));
    if (checksum != ctx.hvi_.value_checksum()) {
      s = Status::Corruption("Checksum mismatch");
      return s;
    }
  }
  // TODO(wnj): decompress the value
  if (heap_value_cache_ && ro.fill_cache) {
    // cache the value
    HeapValueCacheKey key = NewCacheKey(ctx.seq_, ctx.hvi_);
    auto cache_ptr = AllocateAndCopyBlock(
        Slice(reinterpret_cast<const char*>(ctx.buffer_.get()),
              ctx.hvi_.value_size()),
        heap_value_cache_->memory_allocator());
    auto cache_obj = std::make_unique<HeapValueCacheData>(
        std::move(cache_ptr), ctx.hvi_.value_size());

    Cache::Handle* cache_handle = nullptr;
    s = heap_value_cache_->Insert(
        key.AsSlice(), cache_obj.get(),
        BasicTypedCacheHelper<HeapValueCacheData,
                              CacheEntryRole::kHeapValue>::GetBasicHelper(),
        cache_obj->size_ + sizeof(*cache_obj), &cache_handle,
        Cache::Priority::BOTTOM);
    if (s.ok()) {
      auto _ = cache_obj.release();
      auto guard = CacheHandleGuard<HeapValueCacheData>(heap_value_cache_.get(),
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
      Slice(reinterpret_cast<const char*>(buffer), ctx.hvi_.value_size()),
      [](void* arg1, void*) { free(arg1); }, buffer, nullptr);
  return s;
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE