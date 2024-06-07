#pragma once

#include <cstdint>

#include "db/column_family.h"
#include "db/heap/extent.h"
#include "db/heap/io_engine.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

class HeapGCJob {
 private:
  const uint64_t job_id_;
  const ColumnFamilyData* cfd_;
  const bool force_gc_;
  uint64_t gc_blocks_{0};
  ExtentManager* extent_manager_;
  std::vector<ExtentMetaData> exts_;

 public:
  HeapGCJob(uint64_t job_id, const ColumnFamilyData* cfd,
            ExtentManager* extent_manager, bool force_gc)
      : job_id_(job_id),
        cfd_(cfd),
        force_gc_(force_gc),
        extent_manager_(extent_manager) {}
  ~HeapGCJob();
  Status Run();

 private:
  Status SubTaskRun(UringIoEngine* io_engine, const HeapFileGCTask& task);
  struct HoleToPunch {
    uint64_t off_{0};   // off in file
    uint64_t size_{0};  // size to punch
  };
  HoleToPunch HoleToPunchAfterFree(ext_id_t ext_id, const ExtentBitmap& bm,
                                   uint32_t block_offset, uint32_t block_cnt);
  void ReadWriteTick(bool read, size_t size);
};

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE