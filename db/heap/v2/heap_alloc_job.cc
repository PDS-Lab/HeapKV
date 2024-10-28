#include "db/heap/v2/heap_alloc_job.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>

#include "db/heap/io_engine.h"
#include "db/heap/v2/extent.h"
#include "db/heap/v2/extent_storage.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/xxhash.h"

namespace HEAPKV_NS_V2 {

ExtentAllocCtx::ExtentAllocCtx(const ExtentFileName fn,
                               uint32_t base_alloc_block_off,
                               std::shared_ptr<ExtentFile> file,
                               const Slice& value_index_block)
    : fn_(fn),
      base_alloc_block_off_(base_alloc_block_off),
      cursor_(0),
      file_(std::move(file)) {
  size_t n = value_index_block.size() / sizeof(ValueAddr);
  value_index_block_.reserve(n);
  const char* cur = value_index_block.data();
  for (size_t i = 0; i < value_index_block.size(); i += 4) {
    value_index_block_.push_back(ValueAddr::DecodeFrom(cur + i));
  }
}

std::optional<uint32_t> ExtentAllocCtx::Alloc(uint16_t b_cnt) {
  if (base_alloc_block_off_ + uint32_t(b_cnt) > kExtentBlockNum) {
    return std::nullopt;
  }
  while (cursor_ < value_index_block_.size() &&
         value_index_block_[cursor_].has_value()) {
    cursor_++;
  }
  if (cursor_ == value_index_block_.size()) {
    value_index_block_.emplace_back();
  }
  ValueAddr va(base_alloc_block_off_, b_cnt);
  value_index_block_[cursor_] = va;
  base_alloc_block_off_ += b_cnt;
  uint32_t value_index = cursor_;
  cursor_++;
  return value_index;
}

HeapAllocJob::~HeapAllocJob() {
  if (buffer1_ != nullptr) free(buffer1_);
  if (buffer2_ != nullptr) free(buffer2_);
}

Status HeapAllocJob::InitJob() {
  io_engine_ = GetThreadLocalIoEngine();
  buffer1_ = static_cast<char*>(std::aligned_alloc(kBlockSize, kBufferSize));
  buffer2_ = static_cast<char*>(std::aligned_alloc(kBlockSize, kBufferSize));
  if (buffer1_ == nullptr || buffer2_ == nullptr) {
    return Status::MemoryLimit("no enough memory for alloc buffer");
  }
  return Status::OK();
}

Status HeapAllocJob::Add(const Slice& key, const Slice& value,
                         HeapValueIndex* hvi) {
  Status s;
  ParsedInternalKey ikey;
  s = ParseInternalKey(key, &ikey, false);
  if (!s.ok()) {
    return s;
  }
  uint32_t aligned_value_size = align_up(value.size(), kBlockSize);
  uint32_t need_block = (value.size() + kBlockSize - 1) / kBlockSize;
  if (need_block > std::numeric_limits<uint16_t>::max()) {
    return Status::IOError("value too large");
  }
  if (aligned_value_size > kBufferSize) {
    // TODO(wnj): handle value larger than 1MB
  }

  uint32_t vi;
  ValueAddr va;
  // AllocSpace will make sure both buffer and extent has space
  s = AllocSpace(need_block, &vi, &va);
  if (!s.ok()) {
    return s;
  }
  auto buf = GetBuffer();
  memcpy(buf + cursor_, value.data(), value.size());
  uint32_t checksum = Lower32of64(XXH3_64bits(value.data_, value.size()));
  cursor_ += aligned_value_size;
  *hvi = HeapValueIndex(ikey.sequence, locked_extents_.back()->file_name(), va,
                        vi, value.size(), checksum, kNoCompression);
  return s;
}

Status HeapAllocJob::Finish(bool commit) {
  Status s;
  if (s = SubmitCurrentBuffer(); !s.ok()) {
    return s;
  }
  if (future_ != nullptr) {
    future_->Wait();
    if (future_->Result() < 0) {
      return Status::IOError("write value failed " +
                                 locked_extents_.back()->file_name().ToString(),
                             strerror(-future_->Result()));
    }
  }
  if (!commit) {
    return s;
  }
  std::vector<std::unique_ptr<UringCmdFuture>> write_meta_future;
  write_meta_future.reserve(locked_extents_.size());
  size_t buf_size = 0;
  for (auto& ctx : locked_extents_) {
    buf_size += kBlockSize + ctx->value_index_block_size();
  }
  void* buf = std::aligned_alloc(kBlockSize, buf_size);
  // std::unique_ptr<void*, decltype(std::free)> g(buf, std::free);
  auto g = std::unique_ptr<void*, decltype(std::free)*>(buf, std::free);
  for (auto& ctx : locked_extents_) {
    // TODO
  }
  for (auto& f : write_meta_future) {
    f->Wait();
    if (f->Result() < 0) {
      return Status::IOError("update file meta failed", strerror(-f->Result()));
    }
  }
  for (auto& ctx : locked_extents_) {
    cfd_->extent_storage()->FreeExtentAfterAlloc(
        ctx->file_name(),
        kExtentDataSize + kBlockSize + ctx->value_index_block_size(),
        ctx->cur_b_off());
  }
  return Status::OK();
}

Status HeapAllocJob::AllocSpace(uint16_t b_cnt, uint32_t* value_index,
                                ValueAddr* value_addr) {
  Status s;
  if (cursor_ + b_cnt * kBlockSize > kBufferSize) {
    if (s = SubmitCurrentBuffer(); !s.ok()) {
      return s;
    }
  }
  PinnableSlice index;

  auto get_free_extent = [&]() -> Status {
    ExtentMeta* meta = nullptr;
    s = cfd_->extent_storage()->GetExtentForAlloc(&meta, b_cnt);
    if (!s.ok()) {
      return s;
    }
    s = cfd_->extent_storage()->GetValueIndexBlock(io_engine_,
                                                   meta->file_.get(), &index);
    if (!s.ok()) {
      return s;
    }
    // add to locked list
    auto ctx = std::make_unique<ExtentAllocCtx>(
        meta->fn_, meta->base_alloc_block_off_, meta->file_, index);
    locked_extents_.push_back(std::move(ctx));
    return Status::OK();
  };

  if (locked_extents_.empty()) [[unlikely]] {
    if (s = get_free_extent(); !s.ok()) {
      return s;
    }
  }
  std::optional<uint32_t> alloc_res = locked_extents_.back()->Alloc(b_cnt);
  while (!alloc_res.has_value()) {
    if (s = SubmitCurrentBuffer(); !s.ok()) {
      return s;
    }
    if (s = get_free_extent(); !s.ok()) {
      return s;
    }
    alloc_res = locked_extents_.back()->Alloc(b_cnt);
  }
  *value_index = alloc_res.value();
  *value_addr = locked_extents_.back()->GetAddr(*value_index);
  return s;
}

Status HeapAllocJob::SubmitCurrentBuffer() {
  if (cursor_ == 0) {
    return Status::OK();
  }
  auto file = locked_extents_.back()->file();
  off64_t offset = locked_extents_.back()->cur_b_off() * kBlockSize - cursor_;
  auto f = file->WriteValueAsync(io_engine_, GetBuffer(), offset, cursor_);
  // swap
  future_.swap(f);
  if (f != nullptr) {
    f->Wait();
    if (f->Result() < 0) {
      return Status::IOError(
          "write value failed " + file->file_name().ToString(),
          strerror(-f->Result()));
    }
  }
  commit_count_++;
  cursor_ = 0;
  return Status::OK();
}

}  // namespace HEAPKV_NS_V2