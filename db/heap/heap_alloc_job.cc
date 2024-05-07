#include "db/heap/heap_alloc_job.h"

#include <liburing/io_uring.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "db/heap/bitmap_allocator.h"
#include "db/heap/heap_file.h"
#include "db/heap/heap_value_index.h"
#include "db/heap/io_engine.h"
#include "db/heap/utils.h"
#include "port/likely.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

HeapAllocJob::~HeapAllocJob() {
  if (io_engine_->inflight() > 0) {
    io_engine_->PollCq(true);
  }
  if (current_batch_.buffer_ != nullptr) {
    free(current_batch_.buffer_);
    current_batch_.buffer_ = nullptr;
  }
  if (previous_batch_.buffer_ != nullptr) {
    free(previous_batch_.buffer_);
    current_batch_.buffer_ = nullptr;
  }
  if (registered_) {
    io_engine_->UnregisterFiles();
    registered_ = false;
  }
}

Status HeapAllocJob::InitJob() {
  io_engine_->RegisterFiles(&fd_, 1);
  registered_ = true;
  posix_memalign(reinterpret_cast<void**>(&current_batch_.buffer_), 4096,
                 kBufferSize);
  return Status::OK();
}

Status HeapAllocJob::Add(const Slice& key, const Slice& value,
                         HeapValueIndex* hvi) {
  Status s;
  if (UNLIKELY(ctx_.current_ext_id_ == InValidExtentId)) {  // init
    s = GetNewFreeExtent();
    if (!s.ok()) {
      return s;
    }
  }

  uint32_t aligned_value_size = align_up(value.size(), kHeapFileBlockSize);
  uint32_t need_block =
      (value.size() + kHeapFileBlockSize - 1) / kHeapFileBlockSize;

  int32_t offset = -1;
  while (-1 == (offset = allocator_.Alloc(need_block))) {
    SubmitValueInBuffer();
    s = GetNewFreeExtent();
    if (!s.ok()) {
      return s;
    }
  }
  ctx_.allocated_ = true;

  uint32_t checksum = Lower32of64(XXH3_64bits(value.data_, value.size()));
  *hvi = HeapValueIndex(ctx_.current_ext_id_, static_cast<uint16_t>(offset),
                        static_cast<uint16_t>(need_block),
                        static_cast<uint32_t>(value.size()), checksum,
                        kNoCompression);
  if (UNLIKELY(value.size() > kBufferSize)) {
    SubmitValueInBuffer();
    void* ptr = nullptr;
    posix_memalign(&ptr, 4096, aligned_value_size);
    memcpy(ptr, value.data(), value.size());
    auto f = ext_mgr_->heap_file()->PutHeapValueAsync(
        io_engine_, UringIoOptions(IOSQE_FIXED_FILE), ctx_.current_ext_id_,
        offset, need_block, static_cast<uint8_t*>(ptr), 0);
    IoReq req(std::move(f), reinterpret_cast<uint8_t*>(ptr), true);
    current_batch_.io_reqs_.emplace_back(std::move(req));
    return Status::OK();
  }

  if (buffer_offset_ + aligned_value_size > kBufferSize) {
    SubmitValueInBuffer();
    s = SwitchBuffer();
  }
  if (!s.ok()) {
    return s;
  }

  if (ctx_.base_bno_ + ctx_.cnt_ == offset) {
    // seq write
    ctx_.cnt_ += need_block;
  } else {
    SubmitValueInBuffer();
    ctx_.base_bno_ = offset;
    ctx_.cnt_ = need_block;
  }
  memcpy(current_batch_.buffer_ + buffer_offset_, value.data(), value.size());
  buffer_offset_ += aligned_value_size;
  return Status::OK();
}

Status HeapAllocJob::Finish(bool commit) {
  Status s;
  // 1. process all pending io_req
  SubmitValueInBuffer();
  auto wait_fn = [&](const IoReq& req) {
    req.future_->Wait();
    if (req.future_->Result() < 0) {
      s = Status::IOError("write failed", strerror(-req.future_->Result()));
    }
    if (req.owned_buffer_) {
      free(req.buffer_);
    }
  };
  for (auto& req : previous_batch_.io_reqs_) {
    wait_fn(req);
  }
  for (auto& req : current_batch_.io_reqs_) {
    wait_fn(req);
  }
  previous_batch_.io_reqs_.clear();
  current_batch_.io_reqs_.clear();
  // 2. update and write bitmap (build checksum)
  if (s.ok() && commit) {
    std::vector<std::unique_ptr<UringCmdFuture>> futures;
    futures.reserve(locked_exts_.size());
    for (auto& [ext_id, bitmap] : locked_exts_) {
      if (useless_exts_.count(ext_id)) {
        continue;
      }
      bitmap->GenerateChecksum();
      auto f = ext_mgr_->heap_file()->WriteExtentHeaderAsync(
          io_engine_, UringIoOptions(IOSQE_FIXED_FILE), ext_id, *bitmap, 0);
      futures.push_back(std::move(f));
    }
    for (auto& f : futures) {
      f->Wait();
      if (f->Result() < 0) {
        s = Status::IOError("write failed", strerror(-f->Result()));
      }
    }
  }
  // 3. fsync
  if (s.ok() && commit) {
    s = ext_mgr_->heap_file()->Fsync(io_engine_,
                                     UringIoOptions(IOSQE_FIXED_FILE), true, 0);
  }
  // 4. unlock and update extents
  std::vector<ExtentMetaData> exts;
  exts.reserve(locked_exts_.size());
  for (auto& [ext_id, bitmap] : locked_exts_) {
    exts.push_back(ExtentMetaData{
        .extent_number_ = ext_id,
        .approximate_free_bits_ =
            commit ? BitMapAllocator::CalcApproximateFreeBits(bitmap->Bitmap(),
                                                              kBitmapSize)
                   : 0,
    });
  }
  ext_mgr_->UnlockExtents(exts, commit);
  return s;
}

Status HeapAllocJob::GetNewFreeExtent() {
  Status s;
  if (ctx_.current_ext_id_ != InValidExtentId && !ctx_.allocated_) {
    // this extent is useless for us
    useless_exts_.insert(ctx_.current_ext_id_);
  }
  auto ext = ext_mgr_->TryLockMostFreeExtent(
      cfd_->ioptions()->heap_extent_allocatable_threshold);
  if (ext.has_value()) {
    ctx_.current_ext_id_ = ext.value();
    ctx_.allocated_ = false;
    ctx_.base_bno_ = 0;
    ctx_.cnt_ = 0;
    auto bm = std::make_unique<ExtentBitmap>();

    s = ext_mgr_->heap_file()->ReadExtentHeader(
        io_engine_, UringIoOptions(IOSQE_FIXED_FILE), ctx_.current_ext_id_,
        bm.get(), 0);
    if (s.ok()) {
      allocator_.Init(kBitmapSize, bm->Bitmap());
      locked_exts_.emplace(ctx_.current_ext_id_, std::move(bm));
    }
    return s;
  }
  ext = ext_mgr_->AllocNewExtent();
  if (!ext.has_value()) {
    return Status::Corruption("cannot alloc new extent");
  }
  ctx_.current_ext_id_ = ext.value();
  ctx_.allocated_ = false;
  ctx_.base_bno_ = 0;
  ctx_.cnt_ = 0;
  auto bm = std::make_unique<ExtentBitmap>();
  memset(bm->Bitmap(), 0, kBitmapSize);
  bm->GenerateChecksum();
  s = ext_mgr_->heap_file()->WriteExtentHeader(io_engine_,
                                               UringIoOptions(IOSQE_FIXED_FILE),
                                               ctx_.current_ext_id_, *bm, 0);
  if (s.ok()) {
    allocator_.Init(kBitmapSize, bm->Bitmap(), true);
    locked_exts_.emplace(ctx_.current_ext_id_, std::move(bm));
  }
  return s;
}

void HeapAllocJob::SubmitValueInBuffer() {
  if (ctx_.cnt_ == 0 || ctx_.current_ext_id_ == InValidExtentId) {
    return;
  }
  size_t off_in_buffer = buffer_offset_ - ctx_.cnt_ * kHeapFileBlockSize;
  auto f = ext_mgr_->heap_file()->PutHeapValueAsync(
      io_engine_, UringIoOptions(IOSQE_FIXED_FILE), ctx_.current_ext_id_,
      ctx_.base_bno_, ctx_.cnt_, current_batch_.buffer_ + off_in_buffer, 0);
  IoReq req(std::move(f), current_batch_.buffer_ + off_in_buffer, false);
  current_batch_.io_reqs_.emplace_back(std::move(req));
  ctx_.base_bno_ = 0;
  ctx_.cnt_ = 0;
}

Status HeapAllocJob::SwitchBuffer() {
  Status s;

  if (previous_batch_.buffer_ != nullptr) {
    for (auto& req : previous_batch_.io_reqs_) {
      req.future_->Wait();
      if (req.future_->Result() < 0) {
        s = Status::IOError("write failed", strerror(-req.future_->Result()));
      }
      if (req.owned_buffer_) {
        free(req.buffer_);
      }
    }
    previous_batch_.io_reqs_.clear();
    auto buffer = previous_batch_.buffer_;
    previous_batch_.buffer_ = current_batch_.buffer_;
    previous_batch_.io_reqs_.swap(current_batch_.io_reqs_);
    current_batch_.buffer_ = buffer;
  } else {
    previous_batch_.buffer_ = current_batch_.buffer_;
    previous_batch_.io_reqs_.swap(current_batch_.io_reqs_);
    posix_memalign(reinterpret_cast<void**>(&current_batch_.buffer_), 4096,
                   kBufferSize);
  }
  buffer_offset_ = 0;
  return s;
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE