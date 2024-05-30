#pragma once

#include <cstdint>
#include <vector>

#include "db/column_family.h"
#include "db/heap/heap_file.h"
#include "db/heap/heap_garbage_collector.h"
#include "db/heap/io_engine.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

class HeapFreeJob {
 private:
  const uint64_t job_id_;
  const ColumnFamilyData* cfd_;
  UringIoEngine* io_engine_;
  ExtentManager* extent_manager_;
  std::vector<HeapGarbageCollector::GarbageBlocks> dropped_blocks_;

 public:
  HeapFreeJob(uint64_t job_id, const ColumnFamilyData* cfd,
              ExtentManager* extent_manager,
              std::vector<HeapGarbageCollector::GarbageBlocks> dropped_blocks)
      : job_id_(job_id),
        cfd_(cfd),
        io_engine_(GetThreadLocalIoEngine()),
        extent_manager_(extent_manager),
        dropped_blocks_(std::move(dropped_blocks)) {}
  ~HeapFreeJob();
  Status Run();

 private:
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