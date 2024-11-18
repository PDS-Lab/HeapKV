#include "db/heap/v2/heap_alloc_job.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>

#include "db/heap/io_engine.h"
#include "db/heap/utils.h"
#include "db/heap/v2/extent.h"
#include "db/heap/v2/extent_storage.h"
#include "db/heap/v2/heap_job_center.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "util/xxhash.h"

namespace HEAPKV_NS_V2 {

ExtentAllocCtx::ExtentAllocCtx(ExtentMeta* meta, ExtentFileName file_name,
                               uint32_t base_alloc_block_off,
                               std::shared_ptr<ExtentFile> file,
                               const Slice& value_index_block)
    : meta_(meta),
      fn_(file_name),
      base_alloc_block_off_(base_alloc_block_off),
      alloc_off_(base_alloc_block_off),
      cursor_(0),
      file_(std::move(file)) {
  size_t n = value_index_block.size() / sizeof(ValueAddr);
  value_index_block_.reserve(n);
  const char* cur = value_index_block.data();
  for (size_t i = 0; i < value_index_block.size(); i += 4) {
    value_index_block_.push_back(ValueAddr::DecodeFrom(cur + i));
  }
}

Status ExtentAllocCtx::FromMeta(UringIoEngine* io_engine,
                                ExtentStorage* storage, ExtentMeta* meta,
                                std::unique_ptr<ExtentAllocCtx>* ctx) {
  PinnableSlice value_index_block;
  auto file = meta->file();
  size_t issue_io = 0;
  Status s = storage->GetValueIndexBlock(io_engine, meta, &file,
                                         &value_index_block, &issue_io);
  if (!s.ok()) {
    return s;
  }
  if (issue_io > 0) {
    RecordTick(storage->cfd()->ioptions()->statistics.get(),
               HEAPKV_ALLOC_JOB_BYTES_READ, issue_io);
  }
  auto mi = meta->meta();
  *ctx = std::make_unique<ExtentAllocCtx>(
      meta, mi.fn_, mi.base_alloc_block_off_, file, value_index_block);
  return Status::OK();
}

std::optional<uint32_t> ExtentAllocCtx::Alloc(uint16_t b_cnt) {
  if (alloc_off_ + uint32_t(b_cnt) > kExtentBlockNum) {
    return std::nullopt;
  }
  while (cursor_ < value_index_block_.size() &&
         value_index_block_[cursor_].has_value()) {
    cursor_++;
  }
  if (cursor_ == value_index_block_.size()) {
    value_index_block_.emplace_back();
  }
  ValueAddr va(alloc_off_, b_cnt);
  value_index_block_[cursor_] = va;
  alloc_off_ += b_cnt;
  uint32_t value_index = cursor_;
  cursor_++;
  return value_index;
}

HeapAllocJob::~HeapAllocJob() {
  if (buffer1_ != nullptr) free(buffer1_);
  if (buffer2_ != nullptr) free(buffer2_);
  cfd_->heap_job_center()->NotifyJobDone(job_id_);
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
  if (s.ok() && commit) {
    // 1. prepare buffer
    size_t max_buf_size = 0;
    for (auto& ctx : locked_extents_) {
      max_buf_size += std::max(
          max_buf_size,
          kBlockSize + ExtentFile::CalcValueIndexSize(ctx->value_index()));
    }
    void* buf = std::aligned_alloc(kBlockSize, max_buf_size);
    if (buf == nullptr) {
      s = Status::MemoryLimit("no enough memory to write meta");
    }
    if (s.ok()) {
      auto g = finally([buf = buf]() { free(buf); });
      // 2. generate meta, index and write
      for (auto& ctx : locked_extents_) {
        s = ctx->file()->UpdateValueIndex(io_engine_, ctx->meta(),
                                          ctx->value_index(), buf);
        if (!s.ok()) {
          break;
        }
        cfd_->extent_storage()->EvictValueIndexCache(ctx->file_name());
        RecordTick(
            cfd_->ioptions()->statistics.get(), HEAPKV_ALLOC_JOB_BYTES_WRITE,
            kBlockSize + ExtentFile::CalcValueIndexSize(ctx->value_index()));
      }
    }
  }
  // 3. unlock extent
  for (auto& ctx : locked_extents_) {
    cfd_->extent_storage()->UnlockExtent(
        ctx->file_name().file_number_,
        commit && s.ok() ? ctx->cur_b_off() : ctx->base_b_off());
  }
  return s;
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
    std::unique_ptr<ExtentAllocCtx> ctx;
    s = ExtentAllocCtx::FromMeta(io_engine_, cfd_->extent_storage(), meta,
                                 &ctx);
    if (!s.ok()) {
      return s;
    }
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
  RecordTick(cfd_->ioptions()->statistics.get(), HEAPKV_ALLOC_JOB_BYTES_WRITE,
             cursor_);
  commit_count_++;
  cursor_ = 0;
  return Status::OK();
}

}  // namespace HEAPKV_NS_V2