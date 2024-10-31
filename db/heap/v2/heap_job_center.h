#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "db/column_family.h"
#include "db/heap/utils.h"
#include "db/heap/v2/heap_alloc_job.h"
#include "db/heap/v2/heap_garbage_collector.h"

namespace HEAPKV_NS_V2 {

class HeapJobCenter {
 private:
  const ColumnFamilyData* cfd_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::atomic_uint64_t next_job_id_{1};
  std::unordered_set<uint64_t> running_jobs_;
  std::unordered_map<uint64_t, std::shared_ptr<CompactionHeapGarbage>>
      pending_garbage_map_;

 public:
  HeapJobCenter(const ColumnFamilyData* cfd) : cfd_(cfd) {}
  ~HeapJobCenter() { WaitJobDone(); }
  auto NewAllocJob() -> std::unique_ptr<HeapAllocJob>;
  void CommitGarbage(const Compaction& compaction,
                     CompactionHeapGarbage garbage);
  void NotifyFileDeletion(uint64_t file_number);
  void NotifyJobDone(uint64_t jid);
  void WaitJobDone(uint64_t jid = 0);
  // NewFreeJob
};

}  // namespace HEAPKV_NS_V2