#pragma once

#include <liburing.h>
#include <liburing/io_uring.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

namespace heapkv {

// TODO(wangnengjie): replace std::cerr

enum class UringIoType : char {
  UnInit,
  Open,
  Close,
  Fallocate,
  Read,
  Write,
  Readv,
  Writev,
};

class UringIoEngine;

// using callback_fn = void (*)(void* context, int32_t res, uint32_t flags);
class UringCmdFuture {
  friend class UringIoEngine;

 private:
  UringIoEngine* engine_;
  std::atomic_bool done_;
  int32_t res_;
  uint32_t flags_;

 public:
  UringCmdFuture(UringIoEngine* engine)
      : engine_(engine), done_(false), res_(0), flags_(0) {}
  ~UringCmdFuture() { Wait(); }
  void Wait();
  auto Done() -> bool { return done_.load(std::memory_order_acquire); }
  auto Result() -> int32_t { return res_; }
  auto Flags() -> uint32_t { return flags_; }

 private:
  void SetResult(int32_t res, uint32_t flags) {
    res_ = res;
    flags_ = flags;
    done_.store(true, std::memory_order_release);
  }
};

struct UringIoOptions {
  uint32_t flags_{0};
  bool submit_now_{true};
};

// a thread unsafe io engine
class UringIoEngine {
 public:
  constexpr static size_t kRingDepth = 1024;
  constexpr static size_t kPollBatchSize = 32;
  friend std::unique_ptr<UringIoEngine> std::make_unique<UringIoEngine>();

 private:
  struct UringCmdHandle {
    UringCmdFuture* future;  // UringCmdFuture dropped after Io complete
    UringIoType type;
    uint32_t next_free;
  };

 private:
  io_uring ring_;
  size_t inflight_{0};
  size_t next_free_{0};
  UringCmdHandle handles_[kRingDepth];
  UringIoEngine() = default;

 public:
  // noncopyable
  UringIoEngine(const UringIoEngine&) = delete;
  UringIoEngine& operator=(const UringIoEngine&) = delete;
  static auto NewUringIoEngine() -> std::unique_ptr<UringIoEngine> {
    auto engine = std::make_unique<UringIoEngine>();
    int ret = io_uring_queue_init(kRingDepth, &engine->ring_, 0);
    if (ret < 0) {
      std::cerr << "io_uring_queue_init failed: " << ret << " "
                << strerror(-ret) << std::endl;
      return nullptr;
    }
    engine->inflight_ = 0;
    engine->next_free_ = 0;
    for (size_t i = 0; i < kRingDepth; i++) {
      engine->handles_[i].future = nullptr;
      engine->handles_[i].type = UringIoType::UnInit;
      engine->handles_[i].next_free = i + 1;
    }
    return engine;
  }

  ~UringIoEngine() {
    while (inflight_ > 0) {
      PollCq(true);
    }
    io_uring_queue_exit(&ring_);
  }

  void PollCq(bool wait);
  auto OpenAt(const UringIoOptions opts, int dfd, const char* path, int flags,
              mode_t mode) -> std::unique_ptr<UringCmdFuture>;
  auto Close(const UringIoOptions opts, int fd)
      -> std::unique_ptr<UringCmdFuture>;
  auto Fallocate(const UringIoOptions opts, int fd, int mode, off_t offset,
                 off_t len) -> std::unique_ptr<UringCmdFuture>;
  auto Read(const UringIoOptions opts, int fd, void* buf, size_t count,
            off_t offset) -> std::unique_ptr<UringCmdFuture>;
  auto Write(const UringIoOptions opts, int fd, const void* buf, size_t count,
             off_t offset) -> std::unique_ptr<UringCmdFuture>;
  auto Readv(const UringIoOptions opts, int fd, const struct iovec* iov,
             int iovcnt, off_t offset) -> std::unique_ptr<UringCmdFuture>;
  auto Writev(const UringIoOptions opts, int fd, const struct iovec* iov,
              int iovcnt, off_t offset) -> std::unique_ptr<UringCmdFuture>;

 private:
  UringCmdHandle* GetFreeHandle() {
    if (next_free_ == kRingDepth) {
      return nullptr;
    }
    auto handle = &handles_[next_free_];
    next_free_ = handle->next_free;
    return handle;
  }

  void FreeHandle(UringCmdHandle* handle) {
    handle->next_free = next_free_;
    next_free_ = handle - handles_;
  }

  void SubmitIo(const UringIoOptions opts, UringCmdHandle* handle,
                io_uring_sqe* sqe);

  void ProcessCqe(io_uring_cqe* cqe, bool advance);
};

auto GetThreadLocalIoEngine() -> UringIoEngine*;

}  // namespace heapkv

}  // namespace ROCKSDB_NAMESPACE