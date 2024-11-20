#include "db/heap/v2/heap_gc_job.h"

#include <bits/types/struct_iovec.h>
#include <fcntl.h>
#include <liburing/io_uring.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bitset>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "db/heap/io_engine.h"
#include "db/heap/utils.h"
#include "db/heap/v2/extent.h"
#include "db/heap/v2/heap_job_center.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace HEAPKV_NS_V2 {

HeapGcJob::~HeapGcJob() {
  cfd_->heap_job_center()->NotifyJobDone(job_id_);
  if (buffers_ != nullptr) {
    free(buffers_);
    buffers_ = nullptr;
  }
}

Status HeapGcJob::InitJob() {
  io_engine_ = GetThreadLocalIoEngine();
  meta_ = cfd_->extent_storage()->GetExtentMeta(file_number_);
  ori_file_ = meta_->file();
  return Status::OK();
}

Status HeapGcJob::ReadOriginValueIndex() {
  PinnableSlice value_index_block;
  size_t issue_io = 0;
  Status s = cfd_->extent_storage()->GetValueIndexBlock(
      io_engine_, meta_, &ori_file_, &value_index_block, &issue_io);
  if (!s.ok()) {
    return s;
  }
  bytes_read_ += issue_io;
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
  for (auto& g : garbage_.value_index_list_) {
    for (auto idx : g) {
      inuse_block_num_ -= vds_[idx].b_cnt();
      vds_[idx].reset();
    }
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

void HeapGcJob::BuildEmptyChunkList() {
  uint32_t off = 0;
  uint32_t off_threshold =
      kExtentBlockNum -
      (kExtentBlockNum * cfd_->ioptions()->heap_extent_relocate_threshold);
  for (size_t i = 0; i < vds_.size() && off < off_threshold; i++) {
    if (vds_[i].b_off() != off) {
      empty_chunk_list_.emplace_back();
      empty_chunk_list_.back().b_off_ = off;
      empty_chunk_list_.back().b_cnt_ =
          std::min(uint32_t(vds_[i].b_off() - off), off_threshold - off);
    }
    off = vds_[i].b_off() + vds_[i].b_cnt();
  }
}

Status HeapGcJob::PrepareBuffer() {
  void* ptr = std::aligned_alloc(kBlockSize, MAX_DEPTH * sizeof(BUF));
  if (ptr == nullptr) {
    return Status::MemoryLimit("failed to alloc memory for Gc buffer");
  }
  buffers_ = reinterpret_cast<BUF*>(ptr);
  return Status::OK();
}

Status HeapGcJob::WaitIo(size_t index, bool read) {
  auto& handle = inflight_[index];
  if (read && handle.read != nullptr) {
    handle.read->Wait();
    if (handle.read->Result() < 0) {
      ROCKS_LOG_ERROR(cfd_->ioptions()->logger,
                      "failed to read from source extent err: %s",
                      strerror(-handle.read->Result()));
      return Status::IOError("failed to read while gc");
    }
  }
  if (!read && handle.write != nullptr) {
    handle.write->Wait();
    if (handle.write->Result() < 0) {
      ROCKS_LOG_ERROR(cfd_->ioptions()->logger,
                      "failed to write to target extent err: %s",
                      strerror(-handle.write->Result()));
      return Status::IOError("failed to write while gc");
    }
  }
  return Status::OK();
}

// GcCost HeapGcJob::AnalyzeNaiveRelocate() {
//   GcCost cost;
//   for (auto& chunk : chunk_list_) {
//     if (!chunk.is_empty_) {
//       cost.io_cnt_++;
//       size_t bytes = chunk.b_cnt_ * kBlockSize;
//       cost.data_move_bytes_ += bytes;
//     }
//   }
//   return cost;
// }

GcCost HeapGcJob::AnalyzeFillEmptyRelocate() {
  GcCost cost;
  uint32_t off_threshold =
      kExtentBlockNum -
      (kExtentBlockNum * cfd_->ioptions()->heap_extent_relocate_threshold);
  size_t start_find = 0;
  for (int i = vds_.size() - 1; i >= 0; i--) {
    bool find = false;
    for (size_t j = 0; !find && j < empty_chunk_list_.size(); j++) {
      if (vds_[i].b_off() < empty_chunk_list_[j].b_off_) {
        break;
      }
      if (vds_[i].b_cnt() <= empty_chunk_list_[j].b_cnt_) {
        find = true;
        empty_chunk_list_[j].b_cnt_ -= vds_[i].b_cnt();
        empty_chunk_list_[j].fill_vd_index_.push_back(i);
        empty_chunk_list_[j].fill_cnt_ += vds_[i].b_cnt();
      }
    }
    if (!find) {
      if (vds_[i].b_off() + vds_[i].b_cnt() > off_threshold) {
        cost.data_move_bytes_ = std::numeric_limits<size_t>::max();
        return cost;
      }
      break;
    }
  }
  for (auto& chunk : empty_chunk_list_) {
    if (chunk.fill_vd_index_.empty()) {
      continue;
    }
    std::reverse(chunk.fill_vd_index_.begin(), chunk.fill_vd_index_.end());
    uint32_t off = 0;
    for (auto vd_index : chunk.fill_vd_index_) {
      cost.data_move_bytes_ += vds_[vd_index].b_cnt() * kBlockSize;
      if (vds_[vd_index].b_off() != off) {
        cost.io_cnt_++;
      }
      off = vds_[vd_index].b_off() + vds_[vd_index].b_cnt();
    }
  }
  return cost;
}

Status HeapGcJob::Run() {
  Status s;
  auto start = std::chrono::steady_clock::now();
  if (s = ReadOriginValueIndex(); !s.ok()) {
    return s;
  }
  RemoveGarbage();
  SortAndRemoveEmpty();
  GcCost naive;
  GcCost fill_empty;
  if (kExtentBlockNum - inuse_block_num_ <
      kExtentBlockNum * cfd_->ioptions()->heap_extent_relocate_threshold) {
    // dont need relocate
    s = DoValueIndexUpdateOnly();
  } else {
    // need relocate
    BuildEmptyChunkList();
    naive = GcCost{kExtentDataSize / BUF_SIZE, kExtentDataSize};
    fill_empty = AnalyzeFillEmptyRelocate();
    bool run_naive = fill_empty.data_move_bytes_ > naive.data_move_bytes_ ||
                     fill_empty.io_cnt_ * 8192 > naive.data_move_bytes_;
    if (run_naive) {
      s = PrepareBuffer();
      s = RunNaiveRelocate();
    } else {
      s = RunFillEmptyRelocate();
    }
    if (s.ok()) {
      s = FinalizeRelocate();
    }
  }
  auto end = std::chrono::steady_clock::now();
  ROCKS_LOG_INFO(
      cfd_->ioptions()->logger, "%s",
      std::format(
          "[HeapGcJob({})] Gc time cost: {}, status: {}, "
          "read: {}, "
          "write: {}, ana GcCost: {}",
          job_id_,
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start),
          s.ToString(), bytes_read_, bytes_write_, fill_empty)
          .c_str());
  RecordTick(cfd_->ioptions()->statistics.get(), HEAPKV_FREE_JOB_BYTES_READ,
             bytes_read_);
  RecordTick(cfd_->ioptions()->statistics.get(), HEAPKV_FREE_JOB_BYTES_WRITE,
             bytes_write_);
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
  bytes_write_ += n;
  return s;
}

Status HeapGcJob::RunNaiveRelocate() {
  Status s;
  // create tmp file
  tmp_path_ = std::format("{}/heapkv/{}.tmp", cfd_->extent_storage()->db_name(),
                          job_id_);
  new_fd_ = open(tmp_path_.c_str(), O_RDWR | O_DIRECT | O_CREAT | O_EXCL, 0644);

  int rc = fallocate(new_fd_, 0, 0, inuse_block_num_ * kBlockSize);
  if (rc != 0) {
    close(new_fd_);
    unlink(tmp_path_.c_str());
    return Status::IOError("failed to fallocate");
  }
  constexpr size_t BLOCK_PER_BATCH = BUF_SIZE / kBlockSize;
  std::bitset<align_up(kExtentBlockNum, BLOCK_PER_BATCH)> bitmap;
  for (auto vd : vds_) {
    for (size_t i = 0; i < vd.b_cnt(); i++) {
      bitmap.set(vd.b_off() + i);
    }
  }
  size_t src_off = 0;
  size_t dst_off = 0;
  while (s.ok() && src_off < kExtentDataSize) {
    size_t read_cnt = 0;
    for (size_t i = 0; s.ok() && i < MAX_DEPTH && src_off < kExtentDataSize;
         i++) {
      if (s = WaitIo(i, false); !s.ok()) {
        break;
      }
      inflight_[i].read = io_engine_->Read(UringIoOptions{}, ori_file_->fd(),
                                           buffers_[i], BUF_SIZE, src_off);
      inflight_[i].read_offset_ = src_off;
      src_off += BUF_SIZE;
      bytes_read_ += BUF_SIZE;
      read_cnt++;
    }
    for (size_t i = 0; s.ok() && i < read_cnt; i++) {
      if (s = WaitIo(i, true); !s.ok()) {
        break;
      }
      size_t write_cnt = 0;
      size_t base_b_off = inflight_[i].read_offset_ / kBlockSize;
      for (size_t b_cursor = 0; b_cursor < BLOCK_PER_BATCH; b_cursor++) {
        if (bitmap[base_b_off + b_cursor]) {
          if (write_cnt != b_cursor) {
            memcpy(static_cast<char*>(buffers_[i]) + write_cnt * kBlockSize,
                   static_cast<char*>(buffers_[i]) + b_cursor * kBlockSize,
                   kBlockSize);
          }
          write_cnt++;
        }
      }
      inflight_[i].write =
          io_engine_->Write(UringIoOptions{}, new_fd_, buffers_[i],
                            write_cnt * kBlockSize, dst_off);
      dst_off += write_cnt * kBlockSize;
      bytes_write_ += write_cnt * kBlockSize;
    }
  }
  for (size_t i = 0; i < MAX_DEPTH; i++) {
    if (Status ss = WaitIo(i, true); !ss.ok()) {
      s = ss;
    }
    if (Status ss = WaitIo(i, false); !ss.ok()) {
      s = ss;
    }
  }
  size_t b_off = 0;
  for (size_t i = 0; i < vds_.size(); i++) {
    vds_[i].b_off_ = b_off;
    b_off += vds_[i].b_cnt_;
  }
  return s;
}

Status HeapGcJob::RunFillEmptyRelocate() {
  Status s;
  std::vector<std::unique_ptr<UringCmdFuture>> read_future;
  for (auto& chunk : empty_chunk_list_) {
    if (chunk.fill_vd_index_.empty()) {
      continue;
    }
    chunk.buffer_ = static_cast<char*>(
        std::aligned_alloc(kBlockSize, chunk.fill_cnt_ * kBlockSize));
    size_t cursor = 0;
    size_t base_off = vds_[chunk.fill_vd_index_[0]].b_off();
    size_t cnt = vds_[chunk.fill_vd_index_[0]].b_cnt();
    for (size_t i = 1; i < chunk.fill_vd_index_.size(); i++) {
      if (vds_[chunk.fill_vd_index_[i]].b_off() == base_off + cnt) {
        cnt += vds_[chunk.fill_vd_index_[i]].b_cnt();
      } else {
        auto f = io_engine_->Read(UringIoOptions{}, ori_file_->fd(),
                                  chunk.buffer_ + cursor, cnt * kBlockSize,
                                  base_off * kBlockSize);
        read_future.push_back(std::move(f));
        bytes_read_ += cnt * kBlockSize;
        cursor += cnt * kBlockSize;
        base_off = vds_[chunk.fill_vd_index_[i]].b_off();
        cnt = vds_[chunk.fill_vd_index_[i]].b_cnt();
      }
    }
    auto f = io_engine_->Read(UringIoOptions{}, ori_file_->fd(),
                              chunk.buffer_ + cursor, cnt * kBlockSize,
                              base_off * kBlockSize);
    read_future.push_back(std::move(f));
    bytes_read_ += cnt * kBlockSize;
    chunk.f_index = read_future.size();
  }
  size_t f_cursor = 0;
  std::vector<std::unique_ptr<UringCmdFuture>> write_future;
  for (auto& chunk : empty_chunk_list_) {
    if (!s.ok()) {
      break;
    }
    if (chunk.fill_vd_index_.empty()) {
      continue;
    }
    for (; s.ok() && f_cursor < chunk.f_index; f_cursor++) {
      read_future[f_cursor]->Wait();
      if (read_future[f_cursor]->Result() < 0) {
        s = Status::IOError("failed to read from old file");
      }
    }
    if (s.ok()) {
      auto f = io_engine_->Write(UringIoOptions{}, ori_file_->fd(),
                                 chunk.buffer_, chunk.fill_cnt_ * kBlockSize,
                                 chunk.b_off_ * kBlockSize);
      write_future.push_back(std::move(f));
      bytes_write_ += chunk.fill_cnt_ * kBlockSize;
    }
  }
  for (auto& f : write_future) {
    f->Wait();
    if (f->Result() < 0) {
      s = Status::IOError("failed to write from new file");
    }
  }
  read_future.clear();
  write_future.clear();
  if (!s.ok()) {
    return s;
  }
  tmp_path_ = std::format("{}/heapkv/{}.tmp", cfd_->extent_storage()->db_name(),
                          job_id_);
  new_fd_ = open(tmp_path_.c_str(), O_RDWR | O_DIRECT | O_CREAT | O_EXCL, 0644);
  struct file_clone_range arg;
  arg.src_fd = ori_file_->fd();
  arg.src_offset = 0;
  arg.dest_offset = 0;
  arg.src_length = align_up(kExtentDataSize, 4096);
  int rc = ioctl(new_fd_, FICLONERANGE, &arg);
  if (rc == -1) {
    close(new_fd_);
    unlink(tmp_path_.c_str());
    return Status::IOError(
        std::format("failed to reflink copy file error:{}", strerror(errno)));
  }
  for (auto& chunk : empty_chunk_list_) {
    size_t off = chunk.b_off_;
    for (auto vd_index : chunk.fill_vd_index_) {
      vds_[vd_index].b_off_ = off;
      off += vds_[vd_index].b_cnt_;
    }
  }
  // // create tmp file
  // tmp_path_ = std::format("{}/heapkv/{}.tmp",
  // cfd_->extent_storage()->db_name(),
  //                         job_id_);
  // new_fd_ = open(tmp_path_.c_str(), O_RDWR | O_DIRECT | O_CREAT | O_EXCL,
  // 0644); if (new_fd_ < 0) {
  //   return Status::IOError(
  //       std::format("failed to create tmp file error:{}", strerror(errno)));
  // }
  // // do reflink copy
  // struct file_clone_range arg;
  // arg.src_fd = ori_file_->fd();
  // arg.src_offset = 0;
  // arg.dest_offset = 0;
  // arg.src_length = align_up(kExtentDataSize, 4096);
  // int rc = ioctl(new_fd_, FICLONERANGE, &arg);
  // if (rc == -1) {
  //   close(new_fd_);
  //   unlink(tmp_path_.c_str());
  //   return Status::IOError(
  //       std::format("failed to reflink copy file error:{}",
  //       strerror(errno)));
  // }
  // size_t io_num = 0;
  // for (size_t i = 0; s.ok() && i < move_op_list_.size(); i++) {
  //   MoveOp op = move_op_list_[i];
  //   while (s.ok() && op.b_cnt_ > 0) {
  //     size_t index = io_num % MAX_DEPTH;
  //     s = WaitIo(index);
  //     size_t b_to_move = std::min(size_t(op.b_cnt_), MAX_MOVE_BLOCKS_PER_IO);
  //     inflight_[index].read = io_engine_->Read(
  //         UringIoOptions{IOSQE_IO_LINK}, new_fd_, buffers_[index],
  //         b_to_move * kBlockSize, op.from_off_ * kBlockSize);
  //     inflight_[index].write =
  //         io_engine_->Write(UringIoOptions{}, new_fd_, buffers_[index],
  //                           b_to_move * kBlockSize, op.to_off_ * kBlockSize);
  //     io_num++;
  //     op.from_off_ += b_to_move;
  //     op.to_off_ += b_to_move;
  //     op.b_cnt_ -= b_to_move;
  //   }
  // }
  // for (size_t i = 0; i < MAX_DEPTH; i++) {
  //   if (Status ss = WaitIo(i); !ss.ok()) {
  //     s = ss;
  //   }
  // }
  // if (s.ok()) {
  //   for (auto fill_op : fill_op_list_) {
  //     vds_[fill_op.off_in_vds_].b_off_ = fill_op.to_off_;
  //   }
  // }
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
  // int rc = ftruncate(new_fd_, base_alloc_block_off * kBlockSize);
  // if (rc != 0) {
  //   close(new_fd_);
  //   unlink(tmp_path_.c_str());
  //   s = Status::IOError("failed to do ftruncate for tmp file");
  //   return s;
  // }
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
  bytes_write_ += n;
  return s;
}

}  // namespace HEAPKV_NS_V2