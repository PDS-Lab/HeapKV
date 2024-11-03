#include "db/heap/v2/heap_job_center.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>

#include "db/compaction/compaction.h"
#include "db/heap/utils.h"
#include "db/heap/v2/extent.h"
#include "db/heap/v2/heap_alloc_job.h"
#include "db/heap/v2/heap_garbage_collector.h"
#include "db/heap/v2/heap_gc_job.h"
#include "logging/logging.h"
#include "rocksdb/status.h"

namespace HEAPKV_NS_V2 {

auto HeapJobCenter::NewAllocJob() -> std::unique_ptr<HeapAllocJob> {
  uint64_t jid = next_job_id_.fetch_add(1, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> g(mu_);
    running_jobs_.insert(jid);
  }
  return std::make_unique<HeapAllocJob>(jid, cfd_);
}

void HeapJobCenter::SubmitGarbage(const Compaction& compaction,
                                  CompactionHeapGarbage garbage) {
  if (garbage.empty()) {
    return;
  }
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
  std::lock_guard<std::mutex> g(mu_);
  auto it = pending_garbage_map_.find(file_number);
  if (it != pending_garbage_map_.end()) {
    if (it->second.unique()) {
      to_gc = std::move(it->second);
    }
    pending_garbage_map_.erase(it);
  }
  constexpr uint32_t might_gc_threshold = (1 << 20) / kBlockSize;
  if (to_gc != nullptr) {
    MergeGarbage(&can_gc_, to_gc.get(), might_gc_threshold,
                 &might_schedule_gc_);
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
      done = bg_running_gc_ == 0 && running_jobs_.empty();
    }
    if (!done) {
      cv_.wait(g);
    }
  }
}

void HeapJobCenter::MaybeScheduleGc(std::unique_ptr<HeapGcJob>* job) {
  std::lock_guard<std::mutex> g(mu_);
  for (auto file_number : might_schedule_gc_) {
    auto it = can_gc_.find(file_number);
    if (it == can_gc_.end()) {
      continue;
    }
    auto extent_storage = cfd_->extent_storage();
    auto mi = extent_storage->GetExtentMeta(file_number)->meta();
    uint32_t garbage_blk =
        mi.base_alloc_block_off_ - mi.inuse_block_num_ + it->second.b_cnt_;
    if (garbage_blk <
        kExtentBlockNum * cfd_->ioptions()->heap_extent_relocate_threshold) {
      continue;
    }
    if (!extent_storage->LockExtentForGc(file_number)) {
      continue;
    }
    ExtentGarbage job_garbage = std::move(it->second);
    can_gc_.erase(it);
    might_schedule_gc_.erase(file_number);
    uint64_t jid = next_job_id_.fetch_add(1, std::memory_order_relaxed);
    auto j = std::make_unique<HeapGcJob>(jid, cfd_, file_number,
                                         std::move(job_garbage));
    running_jobs_.insert(jid);
    if (job != nullptr) {
      assert(*job == nullptr);
      job->swap(j);
    } else {
      auto arg = new BGGcArgs();
      arg->jc_ = this;
      arg->job.swap(j);
      bg_running_gc_++;
      cfd_->ioptions()->env->Schedule(&HeapJobCenter::BGRunGcJob, arg);
    }
    break;
  }
}

void HeapJobCenter::PurgeAllGarbage() {
  WaitJobDone();
  std::lock_guard<std::mutex> g(mu_);
  might_schedule_gc_.clear();
  for (auto it = pending_garbage_map_.begin(); it != pending_garbage_map_.end();
       it++) {
    if (it->second.unique()) {
      MergeGarbage(&can_gc_, it->second.get());
    }
    it->second.reset();
  }
  pending_garbage_map_.clear();
  for (auto& [file_number, garbage] : can_gc_) {
    uint64_t jid = next_job_id_.fetch_add(1, std::memory_order_relaxed);
    auto j =
        std::make_unique<HeapGcJob>(jid, cfd_, file_number, std::move(garbage));
    running_jobs_.insert(jid);
    auto arg = new BGGcArgs();
    arg->jc_ = this;
    arg->job.swap(j);
    cfd_->ioptions()->env->Schedule(&HeapJobCenter::BGRunPurgeJob, arg);
  }
  can_gc_.clear();
}

void HeapJobCenter::BGRunGcJob(void* _arg) {
  BGGcArgs* arg = static_cast<BGGcArgs*>(_arg);
  auto defer = finally([&]() {
    arg->jc_->mu_.lock();
    arg->jc_->bg_running_gc_--;
    arg->jc_->mu_.unlock();
    arg->jc_->cv_.notify_all();
    delete arg;
  });
  uint64_t round = 0;
  while (arg->job != nullptr && round++ < 6) {
    Status s;
    s = arg->job->InitJob();
    if (!s.ok()) {
      ROCKS_LOG_ERROR(arg->jc_->cfd_->ioptions()->logger,
                      "HeapGcJob init error %s", s.ToString().c_str());
      break;
    }
    s = arg->job->Run();
    if (!s.ok()) {
      ROCKS_LOG_ERROR(arg->jc_->cfd_->ioptions()->logger,
                      "HeapGcJob run error %s", s.ToString().c_str());
      break;
    }
    arg->job.reset(nullptr);
    arg->jc_->MaybeScheduleGc(&arg->job);
  }
}

void HeapJobCenter::BGRunPurgeJob(void* _arg) {
  BGGcArgs* arg = static_cast<BGGcArgs*>(_arg);
  auto defer = finally([&]() { delete arg; });
  assert(arg->job != nullptr);
  Status s;
  s = arg->job->InitJob();
  if (!s.ok()) {
    ROCKS_LOG_ERROR(arg->jc_->cfd_->ioptions()->logger,
                    "HeapGcJob init error %s", s.ToString().c_str());
    return;
  }
  s = arg->job->Run();
  if (!s.ok()) {
    ROCKS_LOG_ERROR(arg->jc_->cfd_->ioptions()->logger,
                    "HeapGcJob run error %s", s.ToString().c_str());
    return;
  }
  arg->job.reset(nullptr);
}

}  // namespace HEAPKV_NS_V2