#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <vector>

#include "db/dbformat.h"
#include "db/heap/io_engine.h"
#include "db/heap/v2/extent_storage.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class DBIter;

namespace heapkv {

struct PrefetchUnit {
  Slice user_key_;
  Slice value_;
  std::unique_ptr<v2::HeapValueGetContext> async_ctx_;
};

class PrefetchGroup {
  const uint32_t BUFFER_SIZE = 8192;

 private:
  v2::ExtentStorage *extent_storage_;
  char *current_buffer_{nullptr};
  uint32_t buffer_off_{0};
  uint32_t iter_pos_{0};
  bool valid_{false};
  PinnableSlice heap_value_;
  std::vector<char *> used_buffer_;
  std::vector<char *> free_buffer_;
  std::vector<char *> large_buffer_;  // for unexpected buffer size
  std::vector<PrefetchUnit> prefetch_list_;

 public:
  PrefetchGroup(v2::ExtentStorage *extent_storage)
      : extent_storage_(extent_storage) {}
  ~PrefetchGroup();
  Status DoPrefetch(const ReadOptions &ro, UringIoEngine *io_engine,
                    DBIter *iter, size_t batch);
  void Reset();
  bool Valid() const { return valid_; };
  Status StartIter(const ReadOptions &ro) { return Next(ro); }
  Status Next(const ReadOptions &ro);
  Slice key() const { return prefetch_list_[iter_pos_ - 1].user_key_; };
  Slice value() const {
    if (heap_value_.IsPinned()) {
      return heap_value_;
    }
    return prefetch_list_[iter_pos_ - 1].value_;
  }

 private:
  Status AppendUnit(const ReadOptions &ro, UringIoEngine *io_engine,
                    const Slice user_key, SequenceNumber seq, const Slice value,
                    const bool is_heap_value);
  char *ReserveSpace(size_t n);
};

class HeapIterPrefetcher {
  const uint32_t MAX_DEPTH = 16;

 private:
  const ReadOptions &ro_;
  DBIter *iter_;
  PrefetchGroup pg1_;
  PrefetchGroup pg2_;
  uint8_t current_group_{0};
  uint32_t current_batch_{2};

 public:
  HeapIterPrefetcher(const ReadOptions &ro, v2::ExtentStorage *extent_storage,
                     DBIter *iter)
      : ro_(ro), iter_(iter), pg1_(extent_storage), pg2_(extent_storage) {}
  Status StartPrefetch() {
    pg1_.Reset();
    pg2_.Reset();
    Status s1 = pg1_.DoPrefetch(ro_, GetThreadLocalIoEngine(), iter_, 2);
    Status s2 = pg2_.DoPrefetch(ro_, GetThreadLocalIoEngine(), iter_, 2);
    if (!s1.ok()) {
      return s1;
    }
    if (!s2.ok()) {
      return s2;
    }
    return GetGroup().StartIter(ro_);
  }
  bool Valid() const { return GetGroup().Valid(); }
  Slice key() const { return GetGroup().key(); }
  Slice value() const { return GetGroup().value(); }
  Status Next();
  // Slice

 private:
  PrefetchGroup &GetGroup() { return current_group_ & 1 ? pg2_ : pg1_; }
  const PrefetchGroup &GetGroup() const {
    return current_group_ & 1 ? pg2_ : pg1_;
  }
  PrefetchGroup &GetNextGroup() { return current_group_ & 1 ? pg1_ : pg2_; }
  const PrefetchGroup &GetNextGroup() const {
    return current_group_ & 1 ? pg1_ : pg2_;
  }
};

}  // namespace heapkv

}  // namespace ROCKSDB_NAMESPACE