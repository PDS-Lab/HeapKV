#include "db/heap/heap_free_job.h"

#include <fcntl.h>
#include <liburing/io_uring.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "db/heap/bitmap_allocator.h"
#include "db/heap/heap_file.h"
#include "db/heap/heap_storage.h"
#include "db/heap/io_engine.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

HeapFreeJob::~HeapFreeJob() { cfd_->heap_storage()->NotifyJobDone(job_id_); }

Status HeapFreeJob::Run() {
  auto start = std::chrono::steady_clock::now();
  int fd = extent_manager_->heap_file()->fd();
  io_engine_->RegisterFiles(&fd, 1);

  Status s;
  // dropped_blocks_ is sorted by HeapGarbageCollector
  std::vector<ext_id_t> extents;
  for (auto &d : dropped_blocks_) {
    if (!extents.empty() && d.extent_number_ == extents.back()) {
      continue;
    }
    extent_manager_->LockExtent(d.extent_number_);
    extents.push_back(d.extent_number_);
  }
  std::vector<std::unique_ptr<ExtentBitmap>> buffers;
  std::vector<std::unique_ptr<UringCmdFuture>> futures;
  buffers.reserve(extents.size());
  futures.reserve(extents.size());
  for (auto ext : extents) {
    auto bm = std::make_unique<ExtentBitmap>();
    auto f = extent_manager_->heap_file()->ReadExtentHeaderAsync(
        io_engine_, UringIoOptions(IOSQE_FIXED_FILE), ext, bm.get(), 0);
    buffers.push_back(std::move(bm));
    futures.push_back(std::move(f));
    ReadWriteTick(true, kExtentHeaderSize);
  }
  std::vector<HoleToPunch> holes;
  size_t pos = 0;
  for (size_t i = 0; i < extents.size(); i++) {
    ext_id_t eid = extents[i];
    futures[i]->Wait();
    if (futures[i]->Result() < 0) {
      assert(false);
      s = Status::IOError("Failed to read extent header",
                          strerror(-futures[i]->Result()));
      break;
    }
    if (futures[i]->Result() != kExtentHeaderSize) {
      s = Status::Corruption("extent header size mismatch");
    }
    if (!buffers[i]->VerifyChecksum()) {
      s = Status::Corruption("extent header checksum mismatch");
      break;
    }
    while (pos < dropped_blocks_.size() &&
           dropped_blocks_[pos].extent_number_ == eid) {
      UnSetBitMap(buffers[i]->Bitmap(), dropped_blocks_[pos].block_offset_,
                  dropped_blocks_[pos].block_cnt_);
      auto hole = HoleToPunchAfterFree(eid, *buffers[i],
                                       dropped_blocks_[pos].block_offset_,
                                       dropped_blocks_[pos].block_cnt_);
      if (!holes.empty() && hole.size_ > 0 &&
          holes.back().off_ + holes.back().size_ == hole.off_) {
        holes.back().size_ += hole.size_;
      } else if (hole.size_ > 0) {
        holes.push_back(hole);
      }
      pos++;
    }
    buffers[i]->GenerateChecksum();
  }

  if (s.ok()) {
    futures.clear();
    for (size_t i = 0; i < extents.size(); i++) {
      auto f = extent_manager_->heap_file()->WriteExtentHeaderAsync(
          io_engine_, UringIoOptions(IOSQE_FIXED_FILE), extents[i], *buffers[i],
          0);
      futures.push_back(std::move(f));
      ReadWriteTick(false, kExtentHeaderSize);
    }
    for (auto hole : holes) {
      auto f = io_engine_->Fallocate(UringIoOptions(IOSQE_FIXED_FILE), 0,
                                     FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                                     hole.off_, hole.size_);
      futures.push_back(std::move(f));
    }
    for (auto &f : futures) {
      f->Wait();
      if (f->Result() < 0) {
        assert(false);
        s = Status::IOError("Failed to write extent header or punch hole",
                            strerror(-f->Result()));
      }
      if (f->Type() == UringIoType::Write && f->Result() != kExtentHeaderSize) {
        s = Status::IOError("extent header size mismatch");
      }
    }
    futures.clear();
  }
  auto f = extent_manager_->heap_file()->FsyncAsync(
      io_engine_, UringIoOptions(IOSQE_FIXED_FILE), true, 0);
  f->Wait();
  if (f->Result() < 0) {
    assert(false);
    s = Status::IOError("Failed to fsync", strerror(-f->Result()));
  }
  std::vector<ExtentMetaData> exts;
  exts.reserve(extents.size());
  for (size_t i = 0; i < extents.size(); i++) {
    exts.push_back(ExtentMetaData{
        .extent_number_ = extents[i],
        .approximate_free_bits_ = BitMapAllocator::CalcApproximateFreeBits(
            buffers[i]->Bitmap(), kBitmapSize),
    });
  }
  extent_manager_->UnlockExtents(exts, true);
  io_engine_->UnregisterFiles();
  auto end = std::chrono::steady_clock::now();
  ROCKS_LOG_INFO(
      cfd_->ioptions()->info_log,
      "HeapFreeJob done: job_id=%lu, extents_num=%lu, holes=%lu, status=%s, "
      "last=%lu milliseconds",
      job_id_, extents.size(), holes.size(), s.ToString().c_str(),
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count());
  return s;
}

HeapFreeJob::HoleToPunch HeapFreeJob::HoleToPunchAfterFree(
    ext_id_t ext_id, const ExtentBitmap &bm, uint32_t block_offset,
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

void HeapFreeJob::ReadWriteTick(bool read, size_t size) {
  if (read) {
    RecordTick(cfd_->ioptions()->stats, HEAPKV_FREE_JOB_BYTES_READ, size);
  } else {
    RecordTick(cfd_->ioptions()->stats, HEAPKV_FREE_JOB_BYTES_WRITE, size);
  }
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE