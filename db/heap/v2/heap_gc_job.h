#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "db/column_family.h"
#include "db/heap/io_engine.h"
#include "db/heap/utils.h"
#include "db/heap/v2/extent.h"
#include "db/heap/v2/heap_garbage_collector.h"
#include "rocksdb/status.h"

namespace HEAPKV_NS_V2 {

struct ValueDescriptor : public ValueAddr {
  uint32_t value_index_{0};
  ValueDescriptor() = default;
  ValueDescriptor(uint32_t value_index, ValueAddr value_addr)
      : ValueAddr(value_addr), value_index_(value_index) {}
};

struct GcCost {
  uint64_t io_cnt_{0};
  uint64_t data_move_bytes_{0};
};

struct Chunk {
  bool is_empty_{true};
  size_t b_off_{0};
  size_t b_cnt_{0};
  size_t vd_num_{0};
  size_t off_in_vds_{0};
};

class HeapGcJob {
 private:
  const uint64_t job_id_;
  const uint32_t file_number_;
  const ColumnFamilyData* cfd_;
  ExtentGarbage garbage_;
  UringIoEngine* io_engine_{nullptr};
  ExtentMeta* meta_{nullptr};
  uint32_t max_index_{0};
  uint32_t inuse_block_num_{0};
  std::shared_ptr<ExtentFile> ori_file_;
  std::vector<ValueDescriptor> vds_;
  std::vector<Chunk> chunk_list_;
  int new_fd_{-1};
  std::string tmp_path_;

 public:
  HeapGcJob(const uint64_t job_id, const ColumnFamilyData* cfd,
            uint32_t file_number, ExtentGarbage garbage)
      : job_id_(job_id),
        file_number_(file_number),
        cfd_(cfd),
        garbage_(std::move(garbage)) {}
  ~HeapGcJob();
  Status InitJob();
  Status Run();

 private:
  Status ReadOriginValueIndex();
  void RemoveGarbage();
  void SortAndRemoveEmpty();
  void BuildChunkList();
  GcCost AnalyzeNaiveRelocate();
  GcCost AnalyzeFillEmptyRelocate();
  Status RunNaiveRelocate();
  // Status RunFillEmptyRelocate();
  Status FinalizeRelocate();
  Status DoValueIndexUpdateOnly();
};
}  // namespace HEAPKV_NS_V2

namespace std {

template <>
struct formatter<HEAPKV_NS_V2::GcCost> {
  constexpr auto parse(std::format_parse_context& ctx) { return ctx.begin(); }
  auto format(const HEAPKV_NS_V2::GcCost& cost,
              std::format_context& ctx) const {
    return format_to(ctx.out(),
                     "GcCost{{ io_cnt_ = {}, data_move_bytes_ = {} }}",
                     cost.io_cnt_, cost.data_move_bytes_);
  }
};

}  // namespace std