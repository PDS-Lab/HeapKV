#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "db/column_family.h"
#include "db/heap/io_engine.h"
#include "db/heap/utils.h"
#include "db/heap/v2/extent.h"
#include "db/heap/v2/extent_storage.h"
#include "db/heap/v2/heap_value_index.h"
#include "rocksdb/status.h"

namespace HEAPKV_NS_V2 {

class ExtentAllocCtx {
 private:
  ExtentMeta* meta_;
  ExtentFileName fn_;
  const uint32_t base_alloc_block_off_;
  uint32_t alloc_off_;
  uint32_t cursor_;
  std::shared_ptr<ExtentFile> file_;
  ExtentValueIndex value_index_block_;

 public:
  static Status FromMeta(UringIoEngine* io_engine, ExtentStorage* storage,
                         ExtentMeta* meta,
                         std::unique_ptr<ExtentAllocCtx>* ctx);
  ExtentAllocCtx(ExtentMeta* meta, ExtentFileName file_name,
                 uint32_t base_alloc_block_off,
                 std::shared_ptr<ExtentFile> file,
                 const Slice& value_index_block);
  ExtentMeta* meta() { return meta_; }
  ExtentFileName file_name() const { return fn_; }
  ExtentFile* file() const { return file_.get(); }
  uint32_t base_b_off() const { return base_alloc_block_off_; }
  uint32_t cur_b_off() const { return alloc_off_; }
  ValueAddr GetAddr(uint32_t value_index) const {
    return value_index_block_[value_index];
  }
  std::optional<uint32_t> Alloc(uint16_t b_cnt);
  const ExtentValueIndex& value_index() const { return value_index_block_; }
};

class HeapAllocJob {
  static constexpr size_t kBufferSize = 1 << 20;

 private:
  const uint64_t job_id_;
  const ColumnFamilyData* cfd_;
  UringIoEngine* io_engine_{nullptr};
  std::vector<std::unique_ptr<ExtentAllocCtx>> locked_extents_;
  char* buffer1_{nullptr};
  char* buffer2_{nullptr};
  size_t cursor_{0};
  size_t commit_count_{0};
  std::unique_ptr<UringCmdFuture> future_{nullptr};

 public:
  HeapAllocJob(const uint64_t job_id, const ColumnFamilyData* cfd)
      : job_id_(job_id), cfd_(cfd) {}
  ~HeapAllocJob();
  uint64_t job_id() const { return job_id_; }
  Status InitJob();
  Status Add(const Slice& key, const Slice& value, HeapValueIndex* hvi);
  Status Finish(bool commit);
  size_t min_heap_value_size() const {
    return cfd_->ioptions()->min_heap_value_size;
  }

 private:
  char* GetBuffer() { return commit_count_ & 1 ? buffer2_ : buffer1_; }
  Status AllocSpace(uint16_t b_cnt, uint32_t* value_index,
                    ValueAddr* value_addr);
  Status SubmitCurrentBuffer();
};

}  // namespace HEAPKV_NS_V2