// Copyright (C) 2022 Speedb Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "table/block_based/filter_policy_internal.h"

namespace ROCKSDB_NAMESPACE {

namespace speedb_filter {
inline constexpr size_t kPairedBloomBatchSizeInBlocks = 32U;
// Max supported BPK. Filters using higher BPK-s will use the max
inline constexpr int kMinMillibitsPerKey = 1000;

// Types of proprietary Speedb's filters
enum class FilterType : uint8_t {
  kPairedBlockBloom = 1,
  kFutureUnknown = 0xFF,  // User to indicate an unrecognized filter type from a
                          // future version
};

// Bloom Filter's data provided by Speedb:
//             0 |-----------------------------------|
//               | Raw Paired Bloom filter data      |
//               | ...                               |
//           len |-----------------------------------|
//               | bytes Spdb Filter Types           |
//               |   1: SpdbPairedBloom              |
//               |   other: reserved                 |
//         len+1 |-----------------------------------|
//               | byte for block_and_probes         |
//               |   0 in top 3 bits -> 6 -> 64-byte |
//               |   reserved:                       |
//               |   1 in top 3 bits -> 7 -> 128-byte|
//               |   2 in top 3 bits -> 8 -> 256-byte|
//               |   ...                             |
//               |   num_probes in bottom 5 bits,    |
//               |     except 0 and 31 reserved      |
//         len+2 |-----------------------------------|
//               | two bytes reserved                |
//               |   possibly for hash seed          |
// len_with_meta |-----------------------------------|
class FilterMetadata {
 public:
  // Metadata trailer size for Speedb's filters. (This is separate from
  // block-based table block trailer). Starting at len in the diagram above
  static constexpr uint32_t kMetadataLen = 4U;

  struct Fields {
    size_t num_probes;
    FilterType filter_type;
  };

 public:
  static void WriteMetadata(char* metadata, size_t len, const Fields& fields);
  static Fields ReadMetadata(const char* metadata);
};

}  // namespace speedb_filter

// ===========================================================================================================
class SpdbPairedBloomBitsBuilder : public XXPH3FilterBitsBuilder {
 public:
  // Callback function to create a compatible reader. This is needed when
  // performing post-verify during filter construction / filter block writing
  // (See BlockBasedTableBuilder::WriteRawBlock()
  using FilterBitsReaderCreateFunc =
      std::function<FilterBitsReader*(const Slice& /*filter_content*/)>;

 public:
  // Non-null aggregate_rounding_balance implies optimize_filters_for_memory
  explicit SpdbPairedBloomBitsBuilder(
      const int millibits_per_key,
      std::atomic<int64_t>* aggregate_rounding_balance,
      const std::shared_ptr<CacheReservationManager> cache_res_mgr,
      bool detect_filter_construct_corruption,
      const FilterBitsReaderCreateFunc& reader_create_func, bool is_bottomost);

  ~SpdbPairedBloomBitsBuilder() override {}

  // No Copy allowed
  SpdbPairedBloomBitsBuilder(const SpdbPairedBloomBitsBuilder&) = delete;
  void operator=(const SpdbPairedBloomBitsBuilder&) = delete;

 protected:
  size_t RoundDownUsableSpace(size_t available_size) override;

  FilterBitsReader* GetBitsReader(const Slice& filter_content) override;

 private:
  // Stores the per-block information used to sort and pair blocks in the
  // algorithm
  struct BlockHistogramInfo {
    // Number of keys mapped to this block
    uint16_t num_keys = 0U;

    // Records the original in-batch block idx of the block before sorting
    uint8_t original_in_batch_block_idx = std::numeric_limits<uint8_t>::max();

    // Allows block to be sorted using std sorting algorithms
    bool operator<(const BlockHistogramInfo& other) const {
      return (num_keys < other.num_keys);
    }
  };

  // Records the info about a block's pair in the batch
  struct PairingInfo {
    uint32_t pair_in_batch_block_idx;
    uint8_t hash_set_selector;
  };

  using BatchBlocksHistogram =
      std::array<BlockHistogramInfo,
                 speedb_filter::kPairedBloomBatchSizeInBlocks>;
  using BatchPairingInfo =
      std::array<PairingInfo, speedb_filter::kPairedBloomBatchSizeInBlocks>;

 public:
  Slice Finish(std::unique_ptr<const char[]>* buf) override {
    return Finish(buf, nullptr);
  }

  Slice Finish(std::unique_ptr<const char[]>* buf, Status* status) override;

  size_t ApproximateNumEntries(size_t len_with_metadata) override;
  size_t CalculateSpace(size_t num_entries) override;
  double EstimatedFpRate(size_t /*num_entries*/,
                         size_t /*len_with_metadata*/) override;

 private:
  size_t GetNumProbes();

  void InitVars(uint64_t len_no_metadata);
  void InitBlockHistogram();
  void BuildBlocksHistogram(uint32_t data_len_bytes);
  void SortBatchBlocks(uint32_t batch_idx);
  void PairBatchBlocks(uint32_t batch_idx);
  void PairBlocks();
  void SetBlocksPairs(char* data);
  void BuildBlocks(char* data, uint32_t data_len_bytes);
  void CleanupBuildData();

  void AddAllEntries(char* data, uint32_t data_len_bytes);

  void AddCacheReservation(std::size_t incremental_memory_used);

 private:
  // Target allocation per added key, in thousandths of a bit.
  int millibits_per_key_;

  bool is_bottomost_;
  size_t num_blocks_ = 0U;
  size_t num_batches_ = 0U;
  size_t num_probes_ = 0U;

  std::vector<BatchBlocksHistogram> blocks_histogram_;
  std::vector<BatchPairingInfo> pairing_table_;

  // For managing cache reservations needed for the building of the filter
  std::vector<std::unique_ptr<CacheReservationManager::CacheReservationHandle>>
      internal_cache_res_handles_;

  FilterBitsReaderCreateFunc reader_create_func_;
};

class SpdbPairedBloomBitsReader : public BuiltinFilterBitsReader {
 public:
  SpdbPairedBloomBitsReader(const char* data, size_t num_probes,
                            uint32_t data_len_bytes)
      : data_(data), num_probes_(num_probes), data_len_bytes_(data_len_bytes) {}

  ~SpdbPairedBloomBitsReader() override {}

  // No Copy allowed
  SpdbPairedBloomBitsReader(const SpdbPairedBloomBitsReader&) = delete;
  void operator=(const SpdbPairedBloomBitsReader&) = delete;

  bool HashMayMatch(const uint64_t /*hash*/) override;
  bool MayMatch(const Slice& key) override;
  void MayMatch(int num_keys, Slice** keys, bool* may_match) override;

 private:
  const char* data_;
  const size_t num_probes_;
  const uint32_t data_len_bytes_;
};

}  // namespace ROCKSDB_NAMESPACE
