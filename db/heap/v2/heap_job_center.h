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
#include "db/heap/v2/heap_gc_job.h"

namespace HEAPKV_NS_V2 {

struct BGGcArgs {
  HeapJobCenter* jc_;
  std::unique_ptr<HeapGcJob> job;
};

class HeapJobCenter {
 private:
  const ColumnFamilyData* cfd_;
  std::mutex mu_;
  bool pending_purge_{false};
  std::condition_variable cv_;
  std::atomic_uint64_t next_job_id_{1};
  uint32_t bg_running_gc_{0};  // how many BGRunGcJob is running at bg
  std::unordered_set<uint64_t> running_jobs_;
  std::unordered_map<uint64_t, std::shared_ptr<CompactionHeapGarbage>>
      pending_garbage_map_;
  CompactionHeapGarbage can_gc_;
  std::unordered_set<uint32_t> might_schedule_gc_;

 public:
  HeapJobCenter(const ColumnFamilyData* cfd) : cfd_(cfd) {}
  ~HeapJobCenter() { WaitJobDone(); }
  auto NewAllocJob() -> std::unique_ptr<HeapAllocJob>;
  void SubmitGarbage(const Compaction& compaction,
                     CompactionHeapGarbage garbage);
  void NotifyFileDeletion(uint64_t file_number);
  void NotifyJobDone(uint64_t jid);
  void WaitJobDone(uint64_t jid = 0);
  // if job != null, return allocated job
  // if job == null, auto schedule the job to env thread pool
  void MaybeScheduleGc(std::unique_ptr<HeapGcJob>* job = nullptr);
  // call before db close, this is a dangerous function, make sure no more read
  // and write to db (including user read/write and flush/compaction read/write)
  void PurgeAllGarbage();

 private:
  static void BGRunGcJob(void* arg);
  static void BGRunPurgeJob(void* arg);
};

}  // namespace HEAPKV_NS_V2