#include "db/heap/io_engine.h"

#include <liburing.h>
#include <liburing/io_uring.h>

#include <cstdint>
#include <cstring>
#include <iostream>

#include "port/likely.h"

namespace ROCKSDB_NAMESPACE {
namespace heapkv {

void UringCmdFuture::Wait() {
  while (!Done()) {
    engine_->PollCq(true);
  }
}

void UringIoEngine::PollCq(bool wait) {
  // first peek
  io_uring_cqe* cqes[kPollBatchSize];
  uint32_t n = io_uring_peek_batch_cqe(&ring_, cqes, kPollBatchSize);
  if (n != 0) {
    for (uint32_t i = 0; i < n; i++) {
      auto cqe = cqes[i];
      ProcessCqe(cqe, false);
    }
    io_uring_cq_advance(&ring_, n);
    return;
  }
  if (wait && inflight_ > 0) {
    int ret = io_uring_wait_cqe(&ring_, cqes);
    if (ret != 0) {
      std::cerr << "io_uring_wait_cqe failed: " << ret << " " << strerror(-ret)
                << std::endl;
      return;
    }
    ProcessCqe(cqes[0], true);
  }
}

void UringIoEngine::SubmitIo(const UringIoOptions opts, UringCmdHandle* handle,
                             io_uring_sqe* sqe) {
  io_uring_sqe_set_data(sqe, handle);
  io_uring_sqe_set_flags(sqe, opts.flags_);
  int ret = 0;
  if (opts.submit_now_) {
    ret = io_uring_submit(&ring_);
    if (UNLIKELY(ret < 0)) {
      std::cerr << "io_uring_submit failed: " << ret << " " << strerror(-ret)
                << std::endl;
      handle->future->SetResult(ret, 0);
      FreeHandle(handle);
    }
  }
  if (ret >= 0) {
    inflight_++;
  }
}

void UringIoEngine::ProcessCqe(io_uring_cqe* cqe, bool advance) {
  auto handle = reinterpret_cast<UringCmdHandle*>(cqe->user_data);
  handle->future->SetResult(cqe->res, cqe->flags);
  FreeHandle(handle);
  inflight_--;
  if (advance) {
    io_uring_cq_advance(&ring_, 1);
  }
}

auto UringIoEngine::OpenAt(const UringIoOptions opts, int dfd, const char* path,
                           int flags,
                           mode_t mode) -> std::unique_ptr<UringCmdFuture> {
  UringCmdHandle* handle = nullptr;
  while (nullptr == (handle = GetFreeHandle())) {
    PollCq(true);
  }
  auto future = std::make_unique<UringCmdFuture>(this, UringIoType::Open);
  handle->future = future.get();
  handle->type = UringIoType::Open;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  io_uring_prep_openat(sqe, dfd, path, flags, mode);
  SubmitIo(opts, handle, sqe);
  return future;
}

auto UringIoEngine::Close(const UringIoOptions opts,
                          int fd) -> std::unique_ptr<UringCmdFuture> {
  UringCmdHandle* handle = nullptr;
  while (nullptr == (handle = GetFreeHandle())) {
    PollCq(true);
  }
  auto future = std::make_unique<UringCmdFuture>(this, UringIoType::Close);
  handle->future = future.get();
  handle->type = UringIoType::Close;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  io_uring_prep_close(sqe, fd);
  SubmitIo(opts, handle, sqe);
  return future;
}

auto UringIoEngine::Statx(const UringIoOptions opts, int dfd, const char* path,
                          int flags, unsigned mask, struct statx* statxbuf)
    -> std::unique_ptr<UringCmdFuture> {
  UringCmdHandle* handle = nullptr;
  while (nullptr == (handle = GetFreeHandle())) {
    PollCq(true);
  }
  auto future = std::make_unique<UringCmdFuture>(this, UringIoType::Statx);
  handle->future = future.get();
  handle->type = UringIoType::Statx;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  io_uring_prep_statx(sqe, dfd, path, flags, mask, statxbuf);
  SubmitIo(opts, handle, sqe);
  return future;
}

auto UringIoEngine::Fallocate(const UringIoOptions opts, int fd, int mode,
                              off_t offset,
                              off_t len) -> std::unique_ptr<UringCmdFuture> {
  UringCmdHandle* handle = nullptr;
  while (nullptr == (handle = GetFreeHandle())) {
    PollCq(true);
  }
  auto future = std::make_unique<UringCmdFuture>(this, UringIoType::Fallocate);
  handle->future = future.get();
  handle->type = UringIoType::Fallocate;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  io_uring_prep_fallocate(sqe, fd, mode, offset, len);
  SubmitIo(opts, handle, sqe);
  return future;
}

auto UringIoEngine::Read(const UringIoOptions opts, int fd, void* buf,
                         size_t count,
                         off_t offset) -> std::unique_ptr<UringCmdFuture> {
  UringCmdHandle* handle = nullptr;
  while (nullptr == (handle = GetFreeHandle())) {
    PollCq(true);
  }
  auto future = std::make_unique<UringCmdFuture>(this, UringIoType::Read);
  handle->future = future.get();
  handle->type = UringIoType::Read;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  io_uring_prep_read(sqe, fd, buf, count, offset);
  SubmitIo(opts, handle, sqe);
  return future;
}

auto UringIoEngine::Write(const UringIoOptions opts, int fd, const void* buf,
                          size_t count,
                          off_t offset) -> std::unique_ptr<UringCmdFuture> {
  UringCmdHandle* handle = nullptr;
  while (nullptr == (handle = GetFreeHandle())) {
    PollCq(true);
  }
  auto future = std::make_unique<UringCmdFuture>(this, UringIoType::Write);
  handle->future = future.get();
  handle->type = UringIoType::Write;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  io_uring_prep_write(sqe, fd, buf, count, offset);
  SubmitIo(opts, handle, sqe);
  return future;
}

auto UringIoEngine::Readv(const UringIoOptions opts, int fd,
                          const struct iovec* iov, int iovcnt,
                          off_t offset) -> std::unique_ptr<UringCmdFuture> {
  UringCmdHandle* handle = nullptr;
  while (nullptr == (handle = GetFreeHandle())) {
    PollCq(true);
  }
  auto future = std::make_unique<UringCmdFuture>(this, UringIoType::Readv);
  handle->future = future.get();
  handle->type = UringIoType::Readv;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  io_uring_prep_readv(sqe, fd, iov, iovcnt, offset);
  SubmitIo(opts, handle, sqe);
  return future;
}

auto UringIoEngine::Writev(const UringIoOptions opts, int fd,
                           const struct iovec* iov, int iovcnt,
                           off_t offset) -> std::unique_ptr<UringCmdFuture> {
  UringCmdHandle* handle = nullptr;
  while (nullptr == (handle = GetFreeHandle())) {
    PollCq(true);
  }
  auto future = std::make_unique<UringCmdFuture>(this, UringIoType::Writev);
  handle->future = future.get();
  handle->type = UringIoType::Writev;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  io_uring_prep_writev(sqe, fd, iov, iovcnt, offset);
  SubmitIo(opts, handle, sqe);
  return future;
}

auto UringIoEngine::Fsync(const UringIoOptions opts, int fd,
                          bool datasync) -> std::unique_ptr<UringCmdFuture> {
  UringCmdHandle* handle = nullptr;
  while (nullptr == (handle = GetFreeHandle())) {
    PollCq(true);
  }
  auto future = std::make_unique<UringCmdFuture>(this, UringIoType::Fsync);
  handle->future = future.get();
  handle->type = UringIoType::Fsync;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  io_uring_prep_fsync(sqe, fd, datasync ? IORING_FSYNC_DATASYNC : 0);
  SubmitIo(opts, handle, sqe);
  return future;
}

auto GetThreadLocalIoEngine() -> UringIoEngine* {
  static thread_local std::unique_ptr<UringIoEngine> uring_io_engine{nullptr};
  if (uring_io_engine == nullptr) {
    uring_io_engine = UringIoEngine::NewUringIoEngine();  // lazy init
  }
  return uring_io_engine.get();
}

}  // namespace heapkv
}  // namespace ROCKSDB_NAMESPACE