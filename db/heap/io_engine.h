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
  Statx,
  Fallocate,
  Read,
  Write,
  Readv,
  Writev,
  Fsync,
};

class UringIoEngine;

// using callback_fn = void (*)(void* context, int32_t res, uint32_t flags);
class UringCmdFuture {
  friend class UringIoEngine;

 private:
  UringIoEngine* engine_;
  std::atomic_bool done_;
  UringIoType type_;
  int32_t res_;
  uint32_t flags_;

 public:
  UringCmdFuture(UringIoEngine* engine, UringIoType type)
      : engine_(engine), done_(false), type_(type), res_(0), flags_(0) {}
  ~UringCmdFuture() { Wait(); }
  void Wait();
  auto Done() -> bool { return done_.load(std::memory_order_acquire); }
  auto Type() -> UringIoType { return type_; }
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
  UringIoOptions() = default;
  UringIoOptions(uint32_t flags) : flags_(flags) {}
  bool FixedFile() const { return flags_ & IOSQE_FIXED_FILE; }
};

// a thread unsafe io engine
class UringIoEngine {
 public:
  constexpr static size_t kRingDepth = 128;
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
    io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_IOPOLL;
    params.sq_thread_idle = 50;
    int ret = io_uring_queue_init_params(kRingDepth, &engine->ring_, &params);
    if (ret < 0) {
      std::cerr << "io_uring_queue_init failed: " << ret << " "
                << strerror(-ret) << std::endl;
      return nullptr;
    }
    uint32_t max_wrk[2]{1, 1};
    ret = io_uring_register_iowq_max_workers(&engine->ring_, max_wrk);
    if (ret < 0) {
      std::cerr << "io_uring_register_iowq_max_workers failed: " << ret << " "
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

  size_t inflight() const { return inflight_; }

  void PollCq(bool wait);
  // auto OpenAt(const UringIoOptions opts, int dfd, const char* path, int
  // flags,
  //             mode_t mode) -> std::unique_ptr<UringCmdFuture>;
  // auto Close(const UringIoOptions opts,
  //            int fd) -> std::unique_ptr<UringCmdFuture>;
  // auto Statx(const UringIoOptions opts, int dfd, const char* path, int flags,
  //            unsigned mask, struct statx* statxbuf)
  //     -> std::unique_ptr<UringCmdFuture>;
  // auto Fallocate(const UringIoOptions opts, int fd, int mode, off_t offset,
  //                off_t len) -> std::unique_ptr<UringCmdFuture>;
  auto Read(const UringIoOptions opts, int fd, void* buf, size_t count,
            off_t offset) -> std::unique_ptr<UringCmdFuture>;
  auto Write(const UringIoOptions opts, int fd, const void* buf, size_t count,
             off_t offset) -> std::unique_ptr<UringCmdFuture>;
  auto Readv(const UringIoOptions opts, int fd, const struct iovec* iov,
             int iovcnt, off_t offset) -> std::unique_ptr<UringCmdFuture>;
  auto Writev(const UringIoOptions opts, int fd, const struct iovec* iov,
              int iovcnt, off_t offset) -> std::unique_ptr<UringCmdFuture>;
  // auto Fsync(const UringIoOptions opts, int fd,
  //            bool datasync) -> std::unique_ptr<UringCmdFuture>;

  auto RegisterFiles(const int* fds, uint32_t count) -> int {
    return io_uring_register_files(&ring_, fds, count);
  }
  auto UnregisterFiles() -> int { return io_uring_unregister_files(&ring_); }

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