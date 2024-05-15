#pragma once

#include <liburing/io_uring.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_set>

#include "db/column_family.h"
#include "db/heap/bitmap_allocator.h"
#include "db/heap/heap_file.h"
#include "db/heap/heap_value_index.h"
#include "db/heap/io_engine.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

// A thread unsafe job. This job is designed to run with flush job.
class HeapAllocJob {
 private:
  static constexpr size_t kBufferSize = 1 << 20;  // 1MB
 private:
  struct AllocCtx {
    ext_id_t current_ext_id_{InValidExtentId};
    bool allocated_{false};
    uint16_t base_bno_{0};
    uint16_t cnt_{0};
  };

  struct IoReq {
    std::unique_ptr<UringCmdFuture> future_;
    uint8_t* buffer_;
    bool owned_buffer_;
    IoReq(std::unique_ptr<UringCmdFuture> future, uint8_t* buffer,
          bool owned_buffer)
        : future_(std::move(future)),
          buffer_(buffer),
          owned_buffer_(owned_buffer) {}
    IoReq(const IoReq&) = delete;
    IoReq& operator=(const IoReq&) = delete;
    IoReq(IoReq&&) = default;
    IoReq& operator=(IoReq&&) = default;
  };

  struct Batch {
    uint8_t* buffer_{nullptr};
    std::vector<IoReq> io_reqs_;
  };

 private:
  const uint64_t job_id_;
  const ColumnFamilyData* cfd_;
  ExtentManager* ext_mgr_;
  UringIoEngine* io_engine_;
  int fd_;
  bool registered_{false};
  BitMapAllocator allocator_;
  AllocCtx ctx_;
  size_t buffer_offset_{0};
  Batch current_batch_;
  Batch previous_batch_;
  // since c++17, make_unique can do aligned allocation with alignas due to
  // align new operation
  std::unordered_map<ext_id_t, std::unique_ptr<ExtentBitmap>> locked_exts_;
  std::unordered_set<ext_id_t> useless_exts_;

 public:
  HeapAllocJob(const uint64_t job_id, const ColumnFamilyData* cfd,
               ExtentManager* ext_mgr)
      : job_id_(job_id),
        cfd_(cfd),
        ext_mgr_(ext_mgr),
        io_engine_(GetThreadLocalIoEngine()),
        fd_(ext_mgr->heap_file()->fd()) {}

  ~HeapAllocJob();

  uint32_t min_heap_value_size() const {
    return cfd_->ioptions()->min_heap_value_size;
  }
  Status InitJob();
  Status Add(const Slice& key, const Slice& value, HeapValueIndex* hvi);
  Status Finish(bool commit);

 private:
  Status GetNewFreeExtent();
  void SubmitValueInBuffer();
  Status SwitchBuffer();
  void ReadWriteTick(bool read, size_t size);
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE