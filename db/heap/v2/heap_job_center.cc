#include "db/heap/v2/heap_job_center.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

#include "db/compaction/compaction.h"
#include "db/heap/heap_alloc_job.h"
#include "db/heap/v2/heap_alloc_job.h"
#include "db/heap/v2/heap_garbage_collector.h"

namespace HEAPKV_NS_V2 {

auto HeapJobCenter::NewAllocJob() -> std::unique_ptr<HeapAllocJob> {
  uint64_t jid = next_job_id_.fetch_add(1, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> g(mu_);
    running_jobs_.insert(jid);
  }
  return std::make_unique<HeapAllocJob>(jid, cfd_);
}

void HeapJobCenter::CommitGarbage(const Compaction& compaction,
                                  CompactionHeapGarbage garbage) {
  size_t lvls = compaction.num_input_levels();
  auto ptr = std::make_shared<CompactionHeapGarbage>();
  ptr->swap(garbage);
  {
    std::lock_guard<std::mutex> g(mu_);
    for (size_t l = 0; l < lvls; l++) {
      for (const auto file : *compaction.inputs(l)) {
        pending_garbage_map_.emplace(file->fd.GetNumber(), ptr);
      }
    }
  }
}

void HeapJobCenter::NotifyFileDeletion(uint64_t file_number) {
  std::shared_ptr<CompactionHeapGarbage> to_gc;
  {
    std::lock_guard<std::mutex> g(mu_);
    auto it = pending_garbage_map_.find(file_number);
    if (it != pending_garbage_map_.end()) {
      if (it->second.unique()) {
        to_gc = std::move(it->second);
      }
      pending_garbage_map_.erase(it);
    }
  }
  if (to_gc != nullptr) {
    // TODO(wnj): trigger gc here
  }
}

void HeapJobCenter::NotifyJobDone(uint64_t jid) {
  std::lock_guard<std::mutex> g(mu_);
  running_jobs_.erase(jid);
  cv_.notify_all();
}

void HeapJobCenter::WaitJobDone(uint64_t jid) {
  bool done = false;
  std::unique_lock<std::mutex> g(mu_);
  while (!done) {
    if (jid > 0) {
      done = !running_jobs_.contains(jid);
    } else {
      done = running_jobs_.empty();
    }
    if (!done) {
      cv_.wait(g);
    }
  }
}

}  // namespace HEAPKV_NS_V2