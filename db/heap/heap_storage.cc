#include "db/heap/heap_storage.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>

#include "cache/cache_helpers.h"
#include "cache/typed_cache.h"
#include "db/heap/bitmap_allocator.h"
#include "db/heap/heap_file.h"
#include "db/heap/io_engine.h"
#include "memory/memory_allocator_impl.h"
#include "rocksdb/cache.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

Status CFHeapStorage::OpenOrCreate(
    const std::string& db_name, ColumnFamilyData* cfd,
    std::unique_ptr<CFHeapStorage>* storage_handle) {
  Status s;
  UringIoEngine* io_engine = GetThreadLocalIoEngine();

  char buf[100];
  snprintf(buf, sizeof(buf), "%06llu.%s.%s",
           static_cast<unsigned long long>(cfd->GetID()),
           cfd->GetName().c_str(), "heap");
  std::string heap_file_path = db_name + "/heapkv/" + buf;
  std::unique_ptr<HeapFile> heap_file;
  s = HeapFile::Open(io_engine, heap_file_path, cfd->GetID(),
                     cfd->ioptions()->use_direct_io_for_flush_and_compaction ||
                         cfd->ioptions()->use_direct_reads,
                     &heap_file);
  if (!s.ok()) {
    return s;
  }
  //! TODO(wnj): use extent manifest to generate extent metadata
  struct statx statxbuf;
  s = heap_file->Stat(io_engine, &statxbuf);
  if (!s.ok()) {
    return s;
  }
  uint32_t num_exts = statxbuf.stx_size + kExtentSize - 1 / kExtentSize;
  std::vector<ExtentMetaData> extents(num_exts);
  std::deque<
      std::pair<std::unique_ptr<ExtentBitmap>, std::unique_ptr<UringCmdFuture>>>
      inflight;
  size_t pos = 0;

  auto update_fn = [&](bool end) {
    while (!inflight.empty()) {
      // peek
      if (inflight.front().second->Done()) {
        if (inflight.front().second->Result() < 0) {
          s = Status::IOError("Failed to read extent header",
                              strerror(-inflight.front().second->Result()));
          return;
        }
        extents[pos].extent_number_ = pos;
        extents[pos].approximate_free_bits_ =
            BitMapAllocator::CalcApproximateFreeBits(
                inflight.front().first->Bitmap(), kBitmapSize);
        inflight.pop_front();
        pos++;
      } else if (end) {
        inflight.front().second->Wait();
      } else {
        break;
      }
    }
  };

  for (uint32_t i = 0; i < num_exts; i++) {
    auto bm = std::make_unique<ExtentBitmap>();
    auto f = heap_file->ReadExtentHeaderAsync(io_engine, UringIoOptions(), i,
                                              bm.get());
    inflight.emplace_back(std::move(bm), std::move(f));
    update_fn(false);
  }
  update_fn(true);
  if (!s.ok()) {
    return s;
  }
  auto ext_manager = std::make_unique<ExtentManager>(heap_file.get(), num_exts,
                                                     std::move(extents));
  *storage_handle = std::make_unique<CFHeapStorage>(
      cfd, cfd->ioptions()->heap_value_cache, std::move(heap_file),
      std::move(ext_manager));
  return s;
}

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