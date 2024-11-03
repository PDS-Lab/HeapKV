#include "db/heap/v2/heap_gc_job.h"

#include <fcntl.h>
#include <liburing/io_uring.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <format>
#include <memory>
#include <vector>

#include "db/heap/io_engine.h"
#include "db/heap/utils.h"
#include "db/heap/v2/extent.h"
#include "db/heap/v2/heap_job_center.h"
#include "logging/logging.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace HEAPKV_NS_V2 {

HeapGcJob::~HeapGcJob() { cfd_->heap_job_center()->NotifyJobDone(job_id_); }

Status HeapGcJob::InitJob() {
  io_engine_ = GetThreadLocalIoEngine();
  meta_ = cfd_->extent_storage()->GetExtentMeta(file_number_);
  ori_file_ = meta_->file();
  return Status::OK();
}

Status HeapGcJob::ReadOriginValueIndex() {
  PinnableSlice value_index_block;
  Status s = cfd_->extent_storage()->GetValueIndexBlock(
      io_engine_, meta_, &ori_file_, &value_index_block);
  if (!s.ok()) {
    return s;
  }
  size_t n = value_index_block.size() / sizeof(ValueAddr);
  vds_.reserve(n);
  const char* cur = value_index_block.data();
  for (size_t i = 0; i < n; i++) {
    vds_.push_back(
        ValueDescriptor(i, ValueAddr::DecodeFrom(cur + i * sizeof(ValueAddr))));
    if (vds_[i].has_value()) {
      inuse_block_num_ += vds_[i].b_cnt();
    }
  }
  return Status::OK();
}

void HeapGcJob::RemoveGarbage() {
  for (auto g : garbage_.value_index_list_) {
    inuse_block_num_ -= vds_[g].b_cnt();
    vds_[g].reset();
  }
}

void HeapGcJob::SortAndRemoveEmpty() {
  size_t cur = 0;
  for (size_t i = 0; i < vds_.size(); i++) {
    if (vds_[i].has_value()) {
      max_index_ = std::max(max_index_, vds_[i].value_index_);
      vds_[cur] = vds_[i];
      cur++;
    }
  }
  vds_.resize(cur);
  std::sort(vds_.begin(), vds_.end(),
            [](const ValueDescriptor& l, const ValueDescriptor& r) {
              return l.b_off_ < r.b_off_;
            });
}

void HeapGcJob::BuildChunkList() {
  size_t off = 0;
  Chunk chunk;
  for (size_t i = 0; i < vds_.size(); i++) {
    if (vds_[i].b_off_ != off) {  // has empty chunk
      if (chunk.b_cnt_ > 0) {
        chunk_list_.push_back(chunk);
      }
      chunk_list_.push_back(Chunk{
          .is_empty_ = true,
          .b_off_ = off,
          .b_cnt_ = static_cast<size_t>(vds_[i].b_off_ - off),
      });
      chunk = Chunk{
          .is_empty_ = false,
          .b_off_ = vds_[i].b_off_,
          .b_cnt_ = vds_[i].b_cnt_,
          .vd_num_ = 1,
          .off_in_vds_ = i,
      };
    } else {
      chunk.is_empty_ = false;
      chunk.b_cnt_ += vds_[i].b_cnt_;
      chunk.vd_num_++;
    }
    off = size_t(vds_[i].b_off_) + size_t(vds_[i].b_cnt_);
  }
  if (!chunk.is_empty_) {
    chunk_list_.push_back(chunk);
  }
}

GcCost HeapGcJob::AnalyzeNaiveRelocate() {
  GcCost cost;
  for (auto& chunk : chunk_list_) {
    if (!chunk.is_empty_) {
      cost.io_cnt_++;
      size_t bytes = chunk.b_cnt_ * kBlockSize;
      cost.data_move_bytes_ += bytes;
    }
  }
  return cost;
}

GcCost HeapGcJob::AnalyzeFillEmptyRelocate() {
  GcCost cost;
  size_t off = 0;
  std::vector<ValueDescriptor> work_vds(vds_);
  size_t cur = 0;
  while (off < kExtentBlockNum) {
    if (cur < work_vds.size() && work_vds[cur].has_value() &&
        off == work_vds[cur].b_off_) {
      cur++;
      off += work_vds[cur].b_cnt_;
      continue;
    }
    size_t end_off = kExtentBlockNum;
    size_t next_cur = work_vds.size();
    for (size_t i = cur; i < work_vds.size(); i++) {
      if (work_vds[i].has_value()) {
        end_off = work_vds[i].b_off_;
        next_cur = i;
        break;
      }
    }
    size_t empty_cnt = end_off - off;
    for (ssize_t i = work_vds.size() - 1; i >= ssize_t(cur) && empty_cnt > 0;
         i--) {
      if (work_vds[i].has_value() && work_vds[i].b_cnt() <= empty_cnt) {
        empty_cnt -= work_vds[i].b_cnt();
        cost.io_cnt_++;
        cost.data_move_bytes_ += work_vds[i].b_cnt() * kBlockSize;
        work_vds[i].reset();
      }
    }
    off = end_off;
    cur = next_cur;
  }
  return cost;
}

Status HeapGcJob::Run() {
  Status s;
  if (s = ReadOriginValueIndex(); !s.ok()) {
    return s;
  }
  RemoveGarbage();
  SortAndRemoveEmpty();
  if (kExtentBlockNum - inuse_block_num_ <
      kExtentBlockNum * cfd_->ioptions()->heap_extent_relocate_threshold) {
    // dont need relocate
    s = DoValueIndexUpdateOnly();
  } else {
    // need relocate
    BuildChunkList();
    GcCost naive = AnalyzeNaiveRelocate();
    GcCost fill_empty = AnalyzeFillEmptyRelocate();
    ROCKS_LOG_DEBUG(cfd_->ioptions()->logger, "%s",
                    std::format("Naive Gc Cost: {}, Fill Empty Gc Cost: {}",
                                naive, fill_empty)
                        .c_str());
    // if (naive.io_cnt_ * 128 * 1024 + naive.data_move_bytes_ <
    //     fill_empty.io_cnt_ * 128 * 1024 + fill_empty.data_move_bytes_) {
    //   s = RunNaiveRelocate();
    // } else {
    //   s = RunFillEmptyRelocate();
    // }
    s = RunNaiveRelocate();
    if (s.ok()) {
      s = FinalizeRelocate();
    }
  }

  // no matter success or not, unlock extent
  cfd_->extent_storage()->UnlockExtent(file_number_,
                                       meta_->meta().base_alloc_block_off_);
  return s;
}

Status HeapGcJob::DoValueIndexUpdateOnly() {
  Status s;
  ExtentValueIndex value_index_block;
  value_index_block.resize(max_index_ + 1, ValueAddr());
  for (auto& vd : vds_) {
    value_index_block[vd.value_index_] = ValueAddr(vd.b_off(), vd.b_cnt());
  }
  size_t n = kBlockSize + ExtentFile::CalcValueIndexSize(value_index_block);
  char* buf = static_cast<char*>(std::aligned_alloc(kBlockSize, n));
  if (buf == nullptr) {
    return Status::MemoryLimit("no memory for DoValueIndexUpdateOnly buffer");
  }
  s = ori_file_->UpdateValueIndex(io_engine_, meta_, value_index_block, buf);
  free(buf);
  return s;
}

Status HeapGcJob::RunNaiveRelocate() {
  Status s;
  // create tmp file
  tmp_path_ = std::format("{}/heapkv/{}.tmp", cfd_->extent_storage()->db_name(),
                          job_id_);
  new_fd_ = open(tmp_path_.c_str(), O_RDWR | O_DIRECT | O_CREAT | O_EXCL, 0644);

  using BUF = char[128 * 1024];  // 128k
  constexpr size_t MAX_MOVE_BLOCKS_PER_IO = 128 * 1024 / kBlockSize;
  constexpr size_t MAX_DEPTH = 16;
  void* ptr = std::aligned_alloc(kBlockSize, MAX_DEPTH * sizeof(BUF));
  auto buffers = reinterpret_cast<BUF*>(ptr);
  auto defer = finally([&]() { free(ptr); });

  struct RWPair {
    std::unique_ptr<UringCmdFuture> read{nullptr};
    std::unique_ptr<UringCmdFuture> write{nullptr};
  };
  std::array<RWPair, MAX_DEPTH> inflight;

  auto wait_previous_io = [&](size_t index) {
    bool error = false;
    auto& handle = inflight[index];
    if (handle.read != nullptr) {
      handle.read->Wait();
      handle.write->Wait();
      if (handle.read->Result() < 0) {
        error = true;
        ROCKS_LOG_ERROR(cfd_->ioptions()->logger,
                        "failed to read from source extent err: %s",
                        strerror(-handle.read->Result()));
      }
      if (handle.write->Result() < 0) {
        error = true;
        ROCKS_LOG_ERROR(cfd_->ioptions()->logger,
                        "failed to write to target extent err: %s",
                        strerror(-handle.write->Result()));
      }
    }
    return !error ? Status::OK()
                  : Status::IOError("failed to copy extent value");
  };

  uint64_t io_num = 0;
  size_t new_file_b_off = 0;
  for (auto& chunk : chunk_list_) {
    if (chunk.is_empty_) {
      continue;
    }
    if (!s.ok()) {
      break;
    }
    size_t b_off = chunk.b_off_;
    size_t b_cnt = chunk.b_cnt_;
    size_t off_diff = b_off - new_file_b_off;
    while (s.ok() && b_cnt > 0) {
      size_t index = io_num % MAX_DEPTH;
      s = wait_previous_io(index);
      size_t b_to_move = std::min(b_cnt, MAX_MOVE_BLOCKS_PER_IO);
      inflight[index].read = io_engine_->Read(
          UringIoOptions{IOSQE_IO_LINK}, ori_file_->fd(), buffers[index],
          b_to_move * kBlockSize, b_off * kBlockSize);
      inflight[index].write = io_engine_->Write(
          UringIoOptions{}, new_fd_, buffers[index], b_to_move * kBlockSize,
          new_file_b_off * kBlockSize);
      b_off += b_to_move;
      new_file_b_off += b_to_move;
      b_cnt -= b_to_move;
      io_num++;
    }
    for (size_t i = 0; i < chunk.vd_num_; i++) {
      vds_[chunk.off_in_vds_ + i].b_off_ -= off_diff;
    }
  }
  for (size_t i = 0; i < MAX_DEPTH; i++) {
    if (Status ss = wait_previous_io(i); !ss.ok()) {
      s = ss;
    }
  }
  return s;
}

Status HeapGcJob::FinalizeRelocate() {
  Status s;
  ExtentMeta::MetaInfo mi;
  size_t n =
      kBlockSize + align_up((max_index_ + 1) * sizeof(ValueAddr), kBlockSize);
  char* buf = static_cast<char*>(std::aligned_alloc(kBlockSize, n));
  if (buf == nullptr) {
    return Status::MemoryLimit("no memory for DoValueIndexUpdateOnly buffer");
  }
  memset(buf, 0, n);
  uint32_t base_alloc_block_off = 0;
  for (auto vd : vds_) {
    vd.EncodeTo(buf + kBlockSize + vd.value_index_ * sizeof(ValueAddr));
    base_alloc_block_off =
        std::max(base_alloc_block_off, uint32_t(vd.b_off_ + vd.b_cnt_));
  }
  uint32_t value_index_checksum = Lower32of64(
      XXH3_64bits(static_cast<char*>(buf) + kBlockSize, n - kBlockSize));
  EncodeFixed32(buf + 4, base_alloc_block_off);
  EncodeFixed32(buf + 8, value_index_checksum);
  EncodeFixed32(buf + 12, inuse_block_num_);
  uint32_t meta_block_checksum =
      Lower32of64(XXH3_64bits(static_cast<char*>(buf) + 4, kBlockSize - 4));
  EncodeFixed32(buf, meta_block_checksum);
  auto f =
      io_engine_->Write(UringIoOptions{}, new_fd_, buf, n, kExtentDataSize);
  f->Wait();
  if (f->Result() < 0) {
    s = Status::IOError("failed to write meta and value index for tmp file");
  }
  free(buf);
  mi.fn_ = ori_file_->file_name();
  mi.fn_.file_epoch_++;
  mi.base_alloc_block_off_ = base_alloc_block_off;
  mi.value_index_checksum_ = value_index_checksum;
  mi.meta_block_checksum_ = meta_block_checksum;
  mi.inuse_block_num_ = inuse_block_num_;
  close(new_fd_);
  if (!s.ok()) {
    unlink(tmp_path_.c_str());
    return s;
  }
  std::string new_path = ExtentFile::BuildPath(
      mi.fn_, cfd_->extent_storage()->base_extent_file_dir());
  int rc = rename(tmp_path_.c_str(), new_path.c_str());
  if (rc != 0) {
    s = Status::IOError(
        std::format("failed to rename tmp file error:{}", strerror(errno)));
  }
  if (!s.ok()) {
    unlink(tmp_path_.c_str());
    return s;
  }
  std::unique_ptr<ExtentFile> new_file;
  s = ExtentFile::Open(mi.fn_, cfd_->extent_storage()->base_extent_file_dir(),
                       &new_file);
  if (!s.ok()) {
    unlink(new_path.c_str());
    return s;
  }
  unlink(ExtentFile::BuildPath(ori_file_->file_name(),
                               cfd_->extent_storage()->base_extent_file_dir())
             .c_str());
  new_fd_ = new_file->fd();
  meta_->UpdateMetaAndFile(mi, std::move(new_file));
  return s;
}

}  // namespace HEAPKV_NS_V2