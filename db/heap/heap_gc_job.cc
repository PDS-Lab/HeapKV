#include "db/heap/heap_gc_job.h"

#include <fcntl.h>
#include <liburing/io_uring.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "db/heap/bitmap_allocator.h"
#include "db/heap/extent.h"
#include "db/heap/heap_file.h"
#include "db/heap/heap_storage.h"
#include "db/heap/io_engine.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

HeapGCJob::~HeapGCJob() {
  cfd_->heap_storage()->NotifyJobDone(job_id_, HeapStorageJobType::GC);
}

Status HeapGCJob::Run() {
  Status s;
  auto start = std::chrono::steady_clock::now();
  auto task = extent_manager_->GenerateGCTask(force_gc_);
  if (task.garbages_.empty()) {
    return s;
  }
  auto io_engine = GetThreadLocalIoEngine();
  int fd = extent_manager_->heap_file()->fd();
  io_engine->RegisterFiles(&fd, 1);
  for (const auto &[ext, _] : task.garbages_) {
    extent_manager_->LockExtent(ext);
  }

  if (task.garbages_.size() < 128) {
    s = SubTaskRun(io_engine, task);
  } else {
    // split task
    size_t num_subtasks = (task.garbages_.size() + 127) / 128;
    size_t subtask_size =
        (task.garbages_.size() + num_subtasks - 1) / num_subtasks;
    for (size_t i = 0; i < num_subtasks; i++) {
      size_t l = i * subtask_size;
      size_t r = std::min(l + subtask_size, task.garbages_.size());
      HeapFileGCTask subtask;
      subtask.epoch_ = task.epoch_;
      subtask.garbages_.resize(r - l);
      for (size_t j = l; j < r; j++) {
        subtask.garbages_[j - l].first = task.garbages_[j].first;
        subtask.garbages_[j - l].second.swap(task.garbages_[j].second);
      }
      s = SubTaskRun(io_engine, subtask);
      if (!s.ok()) {
        break;
      }
    }
  }

  if (s.ok()) {
    s = extent_manager_->heap_file()->Fsync(
        io_engine, UringIoOptions(IOSQE_FIXED_FILE_BIT), true, 0);
  }
  // TODO(wnj): write to manifest/wal, no matter whether there are errors
  // ....

  // unlock extents
  extent_manager_->UnlockExtents(exts_, true, task.epoch_);

  io_engine->UnregisterFiles();
  auto end = std::chrono::steady_clock::now();
  ROCKS_LOG_INFO(
      cfd_->ioptions()->info_log,
      "HeapGCJob done: job_id=%lu, extents_num=%lu, gc_blocks=%lu, holes=%d, "
      "status=%s, "
      "last=%lu milliseconds",
      job_id_, task.garbages_.size(), gc_blocks_, 0, s.ToString().c_str(),
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count());
  return s;

  // auto start = std::chrono::steady_clock::now();
  // auto io_engine = GetThreadLocalIoEngine();
  // int fd = extent_manager_->heap_file()->fd();
  // io_engine->RegisterFiles(&fd, 1);

  // Status s;
  // // dropped_blocks_ is sorted by HeapGarbageCollector
  // std::vector<ext_id_t> extents;
  // for (auto &d : dropped_blocks_) {
  //   if (!extents.empty() && d.extent_number_ == extents.back()) {
  //     continue;
  //   }
  //   extent_manager_->LockExtent(d.extent_number_);
  //   extents.push_back(d.extent_number_);
  // }
  // std::vector<std::unique_ptr<ExtentBitmap>> buffers;
  // std::vector<std::unique_ptr<UringCmdFuture>> futures;
  // buffers.reserve(extents.size());
  // futures.reserve(extents.size());
  // for (auto ext : extents) {
  //   auto bm = std::make_unique<ExtentBitmap>();
  //   auto f = extent_manager_->heap_file()->ReadExtentHeaderAsync(
  //       io_engine, UringIoOptions(IOSQE_FIXED_FILE), ext, bm.get(), 0);
  //   buffers.push_back(std::move(bm));
  //   futures.push_back(std::move(f));
  //   ReadWriteTick(true, kExtentHeaderSize);
  // }
  // std::vector<HoleToPunch> holes;
  // size_t pos = 0;
  // for (size_t i = 0; i < extents.size(); i++) {
  //   ext_id_t eid = extents[i];
  //   futures[i]->Wait();
  //   if (futures[i]->Result() < 0) {
  //     assert(false);
  //     s = Status::IOError("Failed to read extent header",
  //                         strerror(-futures[i]->Result()));
  //     break;
  //   }
  //   if (futures[i]->Result() != kExtentHeaderSize) {
  //     s = Status::Corruption("extent header size mismatch");
  //   }
  //   if (!buffers[i]->VerifyChecksum()) {
  //     s = Status::Corruption("extent header checksum mismatch");
  //     break;
  //   }
  //   while (pos < dropped_blocks_.size() &&
  //          dropped_blocks_[pos].extent_number_ == eid) {
  //     UnSetBitMap(buffers[i]->Bitmap(), dropped_blocks_[pos].block_off_,
  //                 dropped_blocks_[pos].block_cnt_);
  //     // auto hole = HoleToPunchAfterFree(eid, *buffers[i],
  //     //                                  dropped_blocks_[pos].block_offset_,
  //     //                                  dropped_blocks_[pos].block_cnt_);
  //     // if (!holes.empty() && hole.size_ > 0 &&
  //     //     holes.back().off_ + holes.back().size_ == hole.off_) {
  //     //   holes.back().size_ += hole.size_;
  //     // } else if (hole.size_ > 0) {
  //     //   holes.push_back(hole);
  //     // }
  //     pos++;
  //   }
  //   buffers[i]->GenerateChecksum();
  // }

  // if (s.ok()) {
  //   futures.clear();
  //   for (size_t i = 0; i < extents.size(); i++) {
  //     auto f = extent_manager_->heap_file()->WriteExtentHeaderAsync(
  //         io_engine, UringIoOptions(IOSQE_FIXED_FILE), extents[i],
  //         *buffers[i], 0);
  //     futures.push_back(std::move(f));
  //     ReadWriteTick(false, kExtentHeaderSize);
  //   }
  //   for (auto hole : holes) {
  //     auto f = io_engine->Fallocate(UringIoOptions(IOSQE_FIXED_FILE), 0,
  //                                   FALLOC_FL_PUNCH_HOLE |
  //                                   FALLOC_FL_KEEP_SIZE, hole.off_,
  //                                   hole.size_);
  //     futures.push_back(std::move(f));
  //   }
  //   for (auto &f : futures) {
  //     f->Wait();
  //     if (f->Result() < 0) {
  //       assert(false);
  //       s = Status::IOError("Failed to write extent header or punch hole",
  //                           strerror(-f->Result()));
  //     }
  //     if (f->Type() == UringIoType::Write && f->Result() !=
  //     kExtentHeaderSize) {
  //       s = Status::IOError("extent header size mismatch");
  //     }
  //   }
  //   futures.clear();
  // }
  // auto f = extent_manager_->heap_file()->FsyncAsync(
  //     io_engine, UringIoOptions(IOSQE_FIXED_FILE), true, 0);
  // f->Wait();
  // if (f->Result() < 0) {
  //   assert(false);
  //   s = Status::IOError("Failed to fsync", strerror(-f->Result()));
  // }
  // std::vector<ExtentMetaData> exts;
  // exts.reserve(extents.size());
  // for (size_t i = 0; i < extents.size(); i++) {
  //   exts.push_back(ExtentMetaData{
  //       .extent_number_ = extents[i],
  //       .approximate_free_bits_ = BitMapAllocator::CalcApproximateFreeBits(
  //           buffers[i]->Bitmap(), kBitmapSize),
  //   });
  // }
  // extent_manager_->UnlockExtents(exts, true);
  // io_engine->UnregisterFiles();
  // auto end = std::chrono::steady_clock::now();
  // ROCKS_LOG_INFO(
  //     cfd_->ioptions()->info_log,
  //     "HeapGCJob done: job_id=%lu, extents_num=%lu, holes=%lu, status=%s, "
  //     "last=%lu milliseconds",
  //     job_id_, extents.size(), holes.size(), s.ToString().c_str(),
  //     std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
  //         .count());
  // return s;
}

Status HeapGCJob::SubTaskRun(UringIoEngine *io_engine,
                             const HeapFileGCTask &task) {
  Status s;
  auto buffers = std::make_unique<ExtentBitmap[]>(task.garbages_.size());
  std::vector<std::unique_ptr<UringCmdFuture>> read_f;
  std::vector<std::unique_ptr<UringCmdFuture>> write_f;
  read_f.reserve(task.garbages_.size());
  write_f.reserve(task.garbages_.size());
  for (size_t i = 0; i < task.garbages_.size(); i++) {
    auto f = extent_manager_->heap_file()->ReadExtentHeaderAsync(
        io_engine, UringIoOptions(IOSQE_FIXED_FILE), task.garbages_[i].first,
        &buffers[i], 0);
    read_f.push_back(std::move(f));
    ReadWriteTick(true, kExtentHeaderSize);
  }
  for (size_t i = 0; i < task.garbages_.size(); i++) {
    read_f[i]->Wait();
    if (read_f[i]->Result() < 0) {
      assert(false);
      s = Status::IOError("Failed to read extent header",
                          strerror(-read_f[i]->Result()));
      break;
    }
    if (read_f[i]->Result() != kExtentHeaderSize) {
      s = Status::Corruption("extent header size mismatch");
      break;
    }
    if (!buffers[i].VerifyChecksum()) {
      s = Status::Corruption("extent header checksum mismatch");
      break;
    }
    for (auto &g : task.garbages_[i].second) {
      gc_blocks_ += g.block_cnt_;
      UnSetBitMap(buffers[i].Bitmap(), g.block_off_, g.block_cnt_);
      //     // auto hole = HoleToPunchAfterFree(eid, *buffers[i],
      //     // dropped_blocks_[pos].block_offset_,
      //     // dropped_blocks_[pos].block_cnt_);
      //     // if (!holes.empty() && hole.size_ > 0 &&
      //     //     holes.back().off_ + holes.back().size_ == hole.off_) {
      //     //   holes.back().size_ += hole.size_;
      //     // } else if (hole.size_ > 0) {
      //     //   holes.push_back(hole);
      //     // }
    }
    buffers[i].GenerateChecksum();
    auto f = extent_manager_->heap_file()->WriteExtentHeaderAsync(
        io_engine, UringIoOptions(IOSQE_FIXED_FILE), task.garbages_[i].first,
        buffers[i], 0);
    write_f.push_back(std::move(f));
    ReadWriteTick(false, kExtentHeaderSize);
  }
  read_f.clear();
  if (s.ok()) {
    for (auto &f : write_f) {
      f->Wait();
      if (f->Result() < 0) {
        assert(false);
        s = Status::IOError("Failed to write extent header",
                            strerror(-f->Result()));
      }
      if (s.ok() && f->Result() != kExtentHeaderSize) {
        s = Status::IOError("extent header size mismatch");
      }
    }
  }
  write_f.clear();
  for (size_t i = 0; i < task.garbages_.size(); i++) {
    exts_.push_back(ExtentMetaData{
        .extent_number_ = task.garbages_[i].first,
        .approximate_free_bits_ = BitMapAllocator::CalcApproximateFreeBits(
            buffers[i].Bitmap(), kBitmapSize),
    });
  }
  return s;
}

HeapGCJob::HoleToPunch HeapGCJob::HoleToPunchAfterFree(ext_id_t ext_id,
                                                       const ExtentBitmap &bm,
                                                       uint32_t block_offset,
                                                       uint32_t block_cnt) {
  size_t start_byte = block_offset / 8;
  size_t end_byte = (block_offset + block_cnt - 1) / 8;
  size_t start_4bytes = start_byte / 4;
  size_t end_4bytes = end_byte / 4;
  HoleToPunch hole;
  hole.off_ = ExtentDataOffset(ext_id, start_4bytes * 4 * 8);
  for (size_t i = start_4bytes; i <= end_4bytes; i++) {
    const uint32_t *ptr = reinterpret_cast<const uint32_t *>(bm.Bitmap());
    if (ptr[i] != 0) {
      break;
    }
    hole.size_ += 4 * 8 * kHeapFileBlockSize;
  }
  return hole;
}

void HeapGCJob::ReadWriteTick(bool read, size_t size) {
  if (read) {
    RecordTick(cfd_->ioptions()->stats, HEAPKV_FREE_JOB_BYTES_READ, size);
  } else {
    RecordTick(cfd_->ioptions()->stats, HEAPKV_FREE_JOB_BYTES_WRITE, size);
  }
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE