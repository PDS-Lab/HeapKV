#include "db/heap/v2/extent_storage.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <format>
#include <memory>
#include <mutex>

#include "cache/cache_helpers.h"
#include "cache/typed_cache.h"
#include "db/heap/io_engine.h"
#include "db/heap/utils.h"
#include "db/heap/v2/extent.h"
#include "logging/logging.h"
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

Status ExtentStorage::OpenStorage(std::string_view db_name,
                                  ColumnFamilyData* cfd,
                                  std::unique_ptr<ExtentStorage>* storage) {
  auto io_engine = GetThreadLocalIoEngine();
  const std::string root_dir{std::format("{}/heap", db_name)};
  struct ExtentOpenCtx {
    ExtentFileName file_name;
    size_t file_size;
    std::unique_ptr<UringCmdFuture> f;
    std::unique_ptr<ExtentFile> file;
    ExtentMeta* meta;
  };
  auto es = std::make_unique<ExtentStorage>(db_name, cfd);
  std::vector<ExtentOpenCtx> async_handle;
  for (auto const& dir_entry : std::filesystem::directory_iterator(root_dir)) {
    auto fn = dir_entry.path().filename().string();
    if (dir_entry.is_regular_file() && fn.ends_with(".heap")) {
      auto file_name = ExtentFileName::FromString(fn);
      ROCKS_LOG_DEBUG(cfd->ioptions()->logger, "open file %s", fn.c_str());
      async_handle.emplace_back(ExtentOpenCtx{
          .file_name = file_name,
          .file_size = dir_entry.file_size(),
          .f = ExtentFile::OpenAsync(io_engine, file_name, root_dir),
          .meta = es->GetExtentMeta(file_name.file_number_),
      });
    }
  }

  constexpr size_t concurrency = 128;
  char* buffer_pool =
      static_cast<char*>(std::aligned_alloc(kBlockSize, 128 * kBlockSize));
  auto _ = finally([b = buffer_pool]() { free(b); });

  auto init_meta = [&](size_t index, char* buf) -> Status {
    auto& h = async_handle[index];
    h.f->Wait();
    if (h.f->Result() < 0) {
      return Status::IOError(
          std::format("failed to read extent meta, file_name: {}, strerror: {}",
                      h.file_name.ToString(), strerror(-h.f->Result())));
    }
    h.meta->InitFromExist(std::move(h.file),
                          buffer_pool + kBlockSize * (index % concurrency));
    return Status::OK();
  };

  for (size_t i = 0; i < async_handle.size(); i++) {
    auto& h = async_handle[i];
    h.f->Wait();
    if (h.f->Result() < 0) {
      return Status::IOError(std::format("failed to open file {}, strerrno: {}",
                                         h.file_name.ToString(),
                                         strerror(-h.f->Result())));
    }
    h.file =
        std::make_unique<ExtentFile>(h.file_name, h.f->Result(), h.file_size);
    size_t off = i % concurrency;
    char* buf = buffer_pool + off * kBlockSize;
    if (i >= concurrency) {
      if (Status s = init_meta(i - concurrency, buf); !s.ok()) {
        return s;
      }
    }
    auto rf = h.file->ReadMetaAsync(io_engine, buf);
    h.f.swap(rf);
  }
  for (size_t i = concurrency * (async_handle.size() / concurrency);
       i < async_handle.size(); i++) {
    char* buf = buffer_pool + kBlockSize * (i % concurrency);
    if (Status s = init_meta(i, buf); !s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status ExtentStorage::GetValueAddr(UringIoEngine* io_engine, ExtentMeta* meta,
                                   uint32_t value_index,
                                   std::shared_ptr<ExtentFile>* file,
                                   ValueAddr* value_addr) {
  PinnableSlice index;
  Status s = GetValueIndexBlock(io_engine, meta, file, &index);
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
  Status s;
  if (heap_value_cache_) {
    HeapValueCacheKey key(hv_cache_key_, hvi);
    auto handle = heap_value_cache_->Lookup(GetSliceForKey(&key));
    if (handle) {
      // ctx will have ownership of the cache handle
      auto ctx =
          HeapValueGetContext(hvi, nullptr, nullptr, {nullptr, std::free});
      ctx.SetCacheHandle(heap_value_cache_.get(), handle);
      return ctx;
    }
  }

  const bool no_io = ro.read_tier == kBlockCacheTier;

  if (no_io) {
    return HeapValueGetContext(
        Status::Incomplete("Cannot read heap value: no disk I/O allowed"), hvi,
        nullptr, nullptr, {nullptr, std::free});
  }

  // read heap value
  ValueAddr va = hvi.value_addr_;
  ExtentMeta* meta = GetExtentMeta(hvi.extent_.file_number_);
  std::shared_ptr<ExtentFile> file = meta->file_.load();
  if (hvi.extent_.file_epoch_ != file->file_name().file_epoch_) {
    // ultra slow path
    // gc happened, we need to fetch new value addr through value index
    s = GetValueAddr(io_engine, meta, hvi.value_index_, &file, &va);
  }
  if (!s.ok()) {
    return HeapValueGetContext(s, hvi, nullptr, nullptr, {nullptr, std::free});
  }
  void* ptr = std::aligned_alloc(kBlockSize, kBlockSize * va.b_cnt());
  auto f = file->ReadValueAsync(io_engine, va, ptr);
  return HeapValueGetContext(hvi, std::move(file), std::move(f),
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
        XXH3_64bits(ctx.buffer_.get(), ctx.heap_value_index().value_size_));
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
    UringIoEngine* io_engine, ExtentMeta* meta,
    std::shared_ptr<ExtentFile>* file,
    PinnableSlice* value_index_block) -> Status {
  Status s;
  ValueIndexCacheKey key(vi_cache_key_, (*file)->file_name());
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
  // lock for reading value index and insert to cache
  auto meta_lock = meta->lock_shared();
  // update file, there might be gc exchange
  *file = meta->file_.load();
  auto f = file->get();
  // maybe after gc so update key
  key = ValueIndexCacheKey(vi_cache_key_, f->file_name());

  size_t n = f->value_index_size();
  if (n == 0) {  // empty file
    return s;
  }
  void* ptr = std::aligned_alloc(kBlockSize, n);
  if (ptr == nullptr) {
    return Status::MemoryLimit("failed to alloc value index read buffer");
  }
  auto g = std::unique_ptr<uint8_t[], decltype(std::free)*>(
      static_cast<uint8_t*>(ptr), std::free);
  s = f->ReadValueIndex(io_engine, ptr);
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
    auto _ = cache_obj.release();
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

auto ExtentStorage::GetExtentForAlloc(ExtentMeta** meta,
                                      uint16_t min_free_block) -> Status {
  // 1. search in sort_extent_;
  {
    std::lock_guard<std::mutex> g(mu_);
    for (auto it = free_space_map_.begin(); it != free_space_map_.end(); it++) {
      ExtentMeta* m = GetExtentMeta(*it);
      if (kExtentBlockNum - m->base_alloc_block_off_ < min_free_block) {
        continue;
      }
      *meta = m;
      lock_map_.insert(m->fn_.file_number_);
      free_space_map_.erase(it);
      return Status::OK();
    }
  }
  // 2. alloc empty extent file
  uint32_t file_number;
  {
    std::lock_guard<std::mutex> g(mu_);
    file_number = next_extent_file_number_++;
  }
  std::unique_ptr<ExtentFile> file;
  Status s = ExtentFile::Create(ExtentFileName(file_number, 0),
                                db_name_ + "/heap/", &file);
  if (!s.ok()) {
    return s;
  }
  {
    std::lock_guard<std::mutex> g(mu_);
    ExtentMeta* new_meta = GetExtentMeta(file_number);
    new_meta->InitFromEmpty(std::move(file));
    lock_map_.insert(file_number);
    *meta = new_meta;
  }
  return Status::OK();
}

void ExtentStorage::FreeExtentAfterAlloc(ExtentFileName file_name,
                                         uint32_t alloc_off) {
  bool can_alloc = ExtentCanAlloc(alloc_off);
  std::lock_guard<std::mutex> g(mu_);
  lock_map_.erase(file_name.file_number_);
  if (can_alloc) {
    free_space_map_.insert(file_name.file_number_);
  }
}

void ExtentStorage::EvictValueIndexCache(ExtentFileName file_name) {
  if (heap_value_cache_) {
    ValueIndexCacheKey key(vi_cache_key_, file_name);
    heap_value_cache_->Erase(GetSliceForKey(&key));
  }
}

}  // namespace HEAPKV_NS_V2