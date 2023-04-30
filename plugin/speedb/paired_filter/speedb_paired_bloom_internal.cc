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

#include "plugin/speedb/paired_filter/speedb_paired_bloom_internal.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstring>
#include <utility>
#include <vector>

#include "port/likely.h"  // for LIKELY
#include "port/port.h"    // for PREFETCH
#include "test_util/sync_point.h"
#include "util/bloom_impl.h"
#include "util/fastrange.h"

#ifdef HAVE_AVX2
#include <immintrin.h>
#endif

namespace ROCKSDB_NAMESPACE {

namespace {

using InBatchBlockIdx = uint8_t;

// We currently assume the in-batch block index fits within the 1st byte (8
// bits) of the block and it is a power of 2
static_assert(speedb_filter::kPairedBloomBatchSizeInBlocks <= (1 << 8U));
static_assert((speedb_filter::kPairedBloomBatchSizeInBlocks > 0) &&
              ((speedb_filter::kPairedBloomBatchSizeInBlocks &
                (speedb_filter::kPairedBloomBatchSizeInBlocks - 1)) == 0));

// Number of bits to point to any block in a batch (in-batch block index)
static const uint32_t kInBatchIdxNumBits = static_cast<uint32_t>(
    std::ceil(std::log2(speedb_filter::kPairedBloomBatchSizeInBlocks)));

// kBlockSizeInBytes must be a power of 2 (= Cacheline size)
constexpr uint32_t kBlockSizeInBytes = 64U;
static_assert((kBlockSizeInBytes > 0) &&
              ((kBlockSizeInBytes & (kBlockSizeInBytes - 1)) == 0));
constexpr uint32_t kBlockSizeInBits = kBlockSizeInBytes * 8U;
static const uint32_t kBlockSizeNumBits =
    static_cast<uint32_t>(std::ceil(std::log2(kBlockSizeInBits)));
static const uint32_t kNumBlockSizeBitsShiftBits = 32 - kBlockSizeNumBits;

// Number of bits to represent kBlockSizeInBytes
static const uint32_t kNumBitsForBlockSize =
    static_cast<uint32_t>(std::log2(kBlockSizeInBytes));
static const uint32_t KNumBitsInBlockBloom =
    kBlockSizeInBits - kInBatchIdxNumBits;

constexpr uint32_t kBatchSizeInBytes =
    speedb_filter::kPairedBloomBatchSizeInBlocks * kBlockSizeInBytes;

constexpr uint64_t kNumMillibitsInByte = 8 * 1000U;

[[maybe_unused]] constexpr uint32_t kMaxSupportLenWithMetadata = 0xffffffffU;
constexpr uint32_t kMaxSupportedSizeNoMetadata = 0xffffffc0U;

constexpr size_t kMaxNumProbes = 30U;
static_assert(kMaxNumProbes % 2 == 0U);

static const uint8_t kInBatchIdxMask = (uint8_t{1U} << kInBatchIdxNumBits) - 1;
static const uint8_t kFirstByteBitsMask = ~kInBatchIdxMask;

// ==================================================================================================
//
// Helper Functions
//

inline uint32_t HashToGlobalBlockIdx(uint32_t h1, uint32_t len_bytes) {
  return FastRange32(h1, len_bytes >> kNumBitsForBlockSize);
}

inline void PrefetchBlock(const char* block_address) {
  PREFETCH(block_address, 0 /* rw */, 1 /* locality */);
  PREFETCH(block_address + kBlockSizeInBytes - 1, 0 /* rw */, 1 /* locality */);
}

inline uint32_t GetContainingBatchIdx(uint32_t global_block_idx) {
  return (global_block_idx / speedb_filter::kPairedBloomBatchSizeInBlocks);
}

inline uint8_t GetInBatchBlockIdx(uint32_t global_block_idx) {
  return (global_block_idx % speedb_filter::kPairedBloomBatchSizeInBlocks);
}

inline uint8_t GetHashSetSelector(uint32_t first_in_batch_block_idx,
                                  uint32_t second_in_batch_block_idx) {
  assert((first_in_batch_block_idx <
          speedb_filter::kPairedBloomBatchSizeInBlocks) &&
         (second_in_batch_block_idx <
          speedb_filter::kPairedBloomBatchSizeInBlocks));
  return (first_in_batch_block_idx < second_in_batch_block_idx) ? 0U : 1U;
}

inline uint32_t GetFirstGlobalBlockIdxOfBatch(uint32_t batch_idx) {
  return batch_idx * speedb_filter::kPairedBloomBatchSizeInBlocks;
}

inline char* GetBlockAddress(char* data, uint32_t global_block_idx) {
  return (data + global_block_idx * kBlockSizeInBytes);
}

inline const char* GetBlockAddress(const char* data,
                                   uint32_t global_block_idx) {
  return (data + global_block_idx * kBlockSizeInBytes);
}

inline double CalcAdjustedBitsPerKey(size_t millibits_per_key) {
  return static_cast<double>((millibits_per_key * KNumBitsInBlockBloom) /
                             kBlockSizeInBits / 1000);
}

inline double CalcRawNumProbes(size_t millibits_per_key) {
  static const auto log_2 = std::log(2);
  return (log_2 * CalcAdjustedBitsPerKey(millibits_per_key));
}

inline size_t CalcNumProbes(size_t millibits_per_key) {
  double raw_num_probes = CalcRawNumProbes(millibits_per_key);

  // Num probes must be even
  auto num_probes = static_cast<size_t>(std::ceil(raw_num_probes / 2.0) * 2);
  assert(num_probes % 2 == 0U);

  return std::min<size_t>(num_probes, kMaxNumProbes);
}

// False positive rate of a standard Bloom filter, for given ratio of
// filter memory bits to added keys, and number of probes per operation.
// (The false positive rate is effectively independent of scale, assuming
// the implementation scales OK.)
inline double SpdbStandardFpRate(double bits_per_key, double raw_num_probes) {
  // Standard very-good-estimate formula. See
  // https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
  return std::pow(1.0 - std::exp(-raw_num_probes / bits_per_key),
                  raw_num_probes);
}

class BuildBlock {
 public:
  BuildBlock() = default;
  BuildBlock(char* data, uint32_t global_block_idx, bool prefetch_block);

  uint8_t GetInBatchBlockIdxOfPair() const;
  void SetInBatchBlockIdxOfPair(uint8_t pair_batch_block_idx);
  void SetBlockBloomBits(uint32_t hash, uint8_t set_idx, size_t hash_set_size);

 private:
  char* const block_address_ = nullptr;
};

inline BuildBlock::BuildBlock(char* data, uint32_t global_block_idx,
                              bool prefetch_block)
    : block_address_(GetBlockAddress(data, global_block_idx)) {
  if (prefetch_block) {
    PrefetchBlock(block_address_);
  }
}

inline uint8_t BuildBlock::GetInBatchBlockIdxOfPair() const {
  return static_cast<uint8_t>(*block_address_) & kInBatchIdxMask;
}

inline void BuildBlock::SetInBatchBlockIdxOfPair(
    InBatchBlockIdx pair_batch_block_idx) {
  assert(((*block_address_ & kInBatchIdxMask) == 0U) ||
         ((*block_address_ & kInBatchIdxMask) == pair_batch_block_idx));

  *block_address_ =
      (pair_batch_block_idx | (*block_address_ & kFirstByteBitsMask));
}

inline int GetBitPosInBlockForHash(uint32_t hash, uint8_t set_idx) {
  assert(set_idx <= 1U);

  int bitpos = 0;

  if (set_idx == 0) {
    bitpos = hash >> 23;
    if (LIKELY(bitpos > static_cast<int>(kInBatchIdxNumBits - 1))) {
      return bitpos;
    }
    hash <<= 9;
  } else {
    constexpr uint32_t mask = 0x007FC000;
    bitpos = (hash & mask) >> 14;
    if (LIKELY(bitpos > static_cast<int>(kInBatchIdxNumBits - 1))) {
      return bitpos;
    }
  }

  return kInBatchIdxNumBits +
         (static_cast<uint32_t>(KNumBitsInBlockBloom *
                                (hash >> kBlockSizeNumBits)) >>
          (kNumBlockSizeBitsShiftBits));
}

inline void BuildBlock::SetBlockBloomBits(uint32_t hash, uint8_t set_idx,
                                          size_t hash_set_size) {
  for (auto i = 0U; i < hash_set_size; ++i) {
    int bitpos = GetBitPosInBlockForHash(hash, set_idx);
    // Find the byte, and set the proper bit within that byte
    block_address_[bitpos >> 3] |= (char{1} << (bitpos & 7));
    hash *= 0x9e3779b9;
  }
}

class ReadBlock {
 public:
  ReadBlock(const char* data, uint32_t global_block_idx, bool prefetch_block);

  uint8_t GetInBatchBlockIdxOfPair() const;
  bool AreAllBlockBloomBitsSet(uint32_t hash, uint8_t set_idx,
                               size_t hash_set_size) const;

 private:
#ifdef HAVE_AVX2
  bool AreAllBlockBloomBitsSetAvx2(uint32_t hash, uint32_t set_idx,
                                   size_t hash_set_size) const;
#endif
  bool AreAllBlockBloomBitsSetNonAvx2(uint32_t hash, uint8_t set_idx,
                                      size_t hash_set_size) const;

 private:
  const char* const block_address_;
};

inline ReadBlock::ReadBlock(const char* data, uint32_t global_block_idx,
                            bool prefetch_block)
    : block_address_(GetBlockAddress(data, global_block_idx)) {
  if (prefetch_block) {
    PrefetchBlock(block_address_);
  }
}

inline uint8_t ReadBlock::GetInBatchBlockIdxOfPair() const {
  return static_cast<uint8_t>(*block_address_) & kInBatchIdxMask;
}

bool ReadBlock::AreAllBlockBloomBitsSet(uint32_t hash, uint8_t set_idx,
                                        size_t hash_set_size) const {
#ifdef HAVE_AVX2
  // The AVX2 code currently supports only cache-line / block sizes of 64 bytes
  // (512 bits)
  if (kBlockSizeInBits == 512) {
    return AreAllBlockBloomBitsSetAvx2(hash, set_idx, hash_set_size);
  } else {
    return AreAllBlockBloomBitsSetNonAvx2(hash, set_idx, hash_set_size);
  }
#else
  return AreAllBlockBloomBitsSetNonAvx2(hash, set_idx, hash_set_size);
#endif
}

#ifdef HAVE_AVX2
const __m256i mask_vec = _mm256_set1_epi32(0x007FC000);
const __m256i max_bitpos_vec = _mm256_set1_epi32(kInBatchIdxNumBits);
const __m256i fast_range_vec = _mm256_set1_epi32(KNumBitsInBlockBloom);
const __m256i num_idx_bits_vec = _mm256_set1_epi32(kInBatchIdxNumBits);

// Powers of 32-bit golden ratio, mod 2**32.
const __m256i multipliers =
    _mm256_setr_epi32(0x00000001, 0x9e3779b9, 0xe35e67b1, 0x734297e9,
                      0x35fbe861, 0xdeb7c719, 0x448b211, 0x3459b749);

bool ReadBlock::AreAllBlockBloomBitsSetAvx2(uint32_t hash, uint32_t set_idx,
                                            size_t hash_set_size) const {
  assert(kBlockSizeInBytes == 64U);

  int rem_probes = static_cast<int>(hash_set_size);

  // NOTE: This code is an adaptation of the equivalent code for RocksDB's
  // bloom filter testing code using AVX2.
  // See bloom_impl.h for more details

  for (;;) {
    // Eight copies of hash
    __m256i hash_vector = _mm256_set1_epi32(hash);

    // Same effect as repeated multiplication by 0x9e3779b9 thanks to
    // associativity of multiplication.
    hash_vector = _mm256_mullo_epi32(hash_vector, multipliers);

    __m256i orig_hash_vector = hash_vector;

    if (set_idx == 0) {
      // hash >> 23
      hash_vector = _mm256_srli_epi32(hash_vector, 23);
    } else {
      // hash & mask (0x007FC000)
      hash_vector = _mm256_and_si256(hash_vector, mask_vec);

      // hash >> 14
      hash_vector = _mm256_srli_epi32(hash_vector, 14);
    }

    // // Find the bit positions that are < 7
    __m256i smaller_than_7_vec =
        _mm256_cmpgt_epi32(max_bitpos_vec, hash_vector);

    if (_mm256_testz_si256(smaller_than_7_vec, smaller_than_7_vec) == false) {
      __m256i hash_vector_fast_range = orig_hash_vector;

      if (set_idx == 0) {
        // << 9
        hash_vector_fast_range = _mm256_slli_epi32(orig_hash_vector, 9);
      }

      // AVX2 code to calculate the equivalent of
      // GetBitPosInBlockForHash1stPass() for up to 8 hashes

      // Shift right the hashes by kBlockSizeNumBits
      hash_vector_fast_range =
          _mm256_srli_epi32(hash_vector_fast_range, kBlockSizeNumBits);

      // Multiplying by 505 => The result (lower 32 bits will be in the range
      // 0-504 (in the 9 MSB bits).
      hash_vector_fast_range =
          _mm256_mullo_epi32(hash_vector_fast_range, fast_range_vec);
      hash_vector_fast_range =
          _mm256_srli_epi32(hash_vector_fast_range, kNumBlockSizeBitsShiftBits);

      // Add 7 to get the final bit position in the range 7 - 511 (In the 9 MSB
      // bits)
      hash_vector_fast_range =
          _mm256_add_epi32(hash_vector_fast_range, num_idx_bits_vec);

      hash_vector = _mm256_blendv_epi8(hash_vector, hash_vector_fast_range,
                                       smaller_than_7_vec);
    }

    hash_vector = _mm256_slli_epi32(hash_vector, kNumBlockSizeBitsShiftBits);

    auto [is_done, answer] = FastLocalBloomImpl::CheckBitsPositionsInBloomBlock(
        rem_probes, hash_vector, block_address_);
    if (is_done) {
      return answer;
    }

    // otherwise
    // Need another iteration. 0xab25f4c1 == golden ratio to the 8th power
    hash *= 0xab25f4c1;
    rem_probes -= 8;
  }
}

#endif  // HAVE_AVX2

bool ReadBlock::AreAllBlockBloomBitsSetNonAvx2(uint32_t hash, uint8_t set_idx,
                                               size_t hash_set_size) const {
  for (auto i = 0U; i < hash_set_size; ++i) {
    int bitpos = GetBitPosInBlockForHash(hash, set_idx);
    // Find the byte, and check the proper bit within that byte
    if ((block_address_[bitpos >> 3] & (char{1} << (bitpos & 7))) == 0) {
      return false;
    }
    hash *= 0x9e3779b9;
  }
  return true;
}

}  // Unnamed namespace

// ==================================================================================================
namespace speedb_filter {

void FilterMetadata::WriteMetadata(char* metadata, [[maybe_unused]] size_t len,
                                   const Fields& fields) {
  assert(len == kMetadataLen);

  // Init the metadata to all Zeros
  std::memset(metadata, 0x0, kMetadataLen);

  metadata[0] = static_cast<char>(speedb_filter::FilterType::kPairedBlockBloom);

  assert(fields.num_probes <= 30U);
  metadata[1] = static_cast<char>(fields.num_probes);
  // rest of metadata stays zero
}

auto FilterMetadata::ReadMetadata(const char* metadata) -> Fields {
  char filter_type = *metadata;
  char block_and_probes = *(metadata + 1);

  // TODO: Avoid the use of magic numbers
  size_t num_probes = (block_and_probes & 0x1F);
  if (num_probes < 1 || num_probes > 30) {
    // Reserved / future safe
    return {num_probes, FilterType::kFutureUnknown};
  }

  uint16_t rest = DecodeFixed16(metadata + 2);
  if (rest != 0) {
    // Reserved, possibly for hash seed
    // Future safe
    return {num_probes, FilterType::kFutureUnknown};
  }

  if (speedb_filter::FilterType(filter_type) ==
      speedb_filter::FilterType::kPairedBlockBloom) {  // FastLocalBloom
    // TODO: Avoid the use of magic numbers
    auto log2_block_bytes = ((block_and_probes >> 5) & 7);
    if (log2_block_bytes == 0U) {  // Only block size supported for now
      return {num_probes, FilterType::kPairedBlockBloom};
    }
  }

  return {num_probes, FilterType::kFutureUnknown};
}

}  // namespace speedb_filter

// ==================================================================================================
SpdbPairedBloomBitsBuilder::SpdbPairedBloomBitsBuilder(
    const int millibits_per_key,
    std::atomic<int64_t>* aggregate_rounding_balance,
    const std::shared_ptr<CacheReservationManager> cache_res_mgr,
    bool detect_filter_construct_corruption,
    const FilterBitsReaderCreateFunc& reader_create_func, bool is_bottomost)
    : XXPH3FilterBitsBuilder(aggregate_rounding_balance, cache_res_mgr,
                             detect_filter_construct_corruption),
      millibits_per_key_(millibits_per_key),
      is_bottomost_(is_bottomost),
      reader_create_func_(reader_create_func) {
  assert(millibits_per_key >= speedb_filter::kMinMillibitsPerKey);
}

void SpdbPairedBloomBitsBuilder::InitVars(uint64_t len_no_metadata) {
  assert((len_no_metadata % kBatchSizeInBytes) == 0U);
  num_blocks_ = len_no_metadata / kBlockSizeInBytes;
  num_blocks_ = std::max<size_t>(num_blocks_,
                                 speedb_filter::kPairedBloomBatchSizeInBlocks);
  // num_blocks must be event and a multiple of the batch size
  assert(num_blocks_ > 0U);
  assert(num_blocks_ % 2 == 0);
  assert(num_blocks_ % speedb_filter::kPairedBloomBatchSizeInBlocks == 0);

  if (is_bottomost_) {
    num_batches_ = (num_blocks_ / speedb_filter::kPairedBloomBatchSizeInBlocks);
  } else {
    num_batches_ = static_cast<size_t>(
        std::ceil(static_cast<double>(num_blocks_) /
                  speedb_filter::kPairedBloomBatchSizeInBlocks));
  }
  // There must be at least 1 batch
  assert(num_batches_ > 0U);

  pairing_table_.resize(num_batches_);
  AddCacheReservation(num_batches_ *
                      sizeof(decltype(pairing_table_)::value_type));

  num_probes_ = CalcNumProbes(millibits_per_key_);
}

Slice SpdbPairedBloomBitsBuilder::Finish(std::unique_ptr<const char[]>* buf,
                                         Status* status) {
  const size_t num_entries = hash_entries_info_.entries.size();
  size_t len_with_metadata = CalculateSpace(num_entries);

  std::unique_ptr<char[]> mutable_buf;
  std::unique_ptr<CacheReservationManager::CacheReservationHandle>
      final_filter_cache_res_handle;
  len_with_metadata =
      AllocateMaybeRounding(len_with_metadata, num_entries, &mutable_buf);
  assert(mutable_buf);
  assert(len_with_metadata >= speedb_filter::FilterMetadata::kMetadataLen);
  // Max size supported by implementation
  assert(len_with_metadata <= kMaxSupportLenWithMetadata);

  // Cache reservation for mutable_buf
  if (cache_res_mgr_) {
    Status s = cache_res_mgr_->MakeCacheReservation(
        len_with_metadata * sizeof(char), &final_filter_cache_res_handle);
    s.PermitUncheckedError();
  }

  uint32_t len_no_metadata = static_cast<uint32_t>(
      len_with_metadata - speedb_filter::FilterMetadata::kMetadataLen);
  InitVars(len_no_metadata);

  if (len_no_metadata > 0) {
    TEST_SYNC_POINT_CALLBACK(
        "XXPH3FilterBitsBuilder::Finish::"
        "TamperHashEntries",
        &hash_entries_info_.entries);
    AddAllEntries(mutable_buf.get(), len_no_metadata);
    Status verify_hash_entries_checksum_status =
        MaybeVerifyHashEntriesChecksum();
    if (!verify_hash_entries_checksum_status.ok()) {
      if (status) {
        *status = verify_hash_entries_checksum_status;
      }
      return FinishAlwaysTrue(buf);
    }
  }

  bool keep_entries_for_postverify = detect_filter_construct_corruption_;
  if (!keep_entries_for_postverify) {
    ResetEntries();
  }

  speedb_filter::FilterMetadata::Fields metadata_fields{
      num_probes_, speedb_filter::FilterType::kPairedBlockBloom};
  speedb_filter::FilterMetadata::WriteMetadata(
      &mutable_buf[len_no_metadata],
      speedb_filter::FilterMetadata::kMetadataLen, metadata_fields);

  auto TEST_arg_pair __attribute__((__unused__)) =
      std::make_pair(&mutable_buf, len_with_metadata);
  TEST_SYNC_POINT_CALLBACK("XXPH3FilterBitsBuilder::Finish::TamperFilter",
                           &TEST_arg_pair);

  Slice rv(mutable_buf.get(), len_with_metadata);
  *buf = std::move(mutable_buf);
  final_filter_cache_res_handles_.push_back(
      std::move(final_filter_cache_res_handle));
  if (status) {
    *status = Status::OK();
  }
  return rv;
}

size_t SpdbPairedBloomBitsBuilder::ApproximateNumEntries(
    size_t len_with_metadata) {
  size_t len_no_meta =
      len_with_metadata >= speedb_filter::FilterMetadata::kMetadataLen
          ? RoundDownUsableSpace(len_with_metadata) -
                speedb_filter::FilterMetadata::kMetadataLen
          : 0;
  return static_cast<size_t>(kNumMillibitsInByte * len_no_meta /
                             millibits_per_key_);
}

size_t SpdbPairedBloomBitsBuilder::CalculateSpace(size_t num_entries) {
  size_t len_without_metadata =
      num_entries * millibits_per_key_ / kNumMillibitsInByte;
  // Make sure we have enough space for at least 1 batch
  len_without_metadata =
      std::max<size_t>(len_without_metadata, kBatchSizeInBytes);
  return RoundDownUsableSpace(len_without_metadata +
                              speedb_filter::FilterMetadata::kMetadataLen);
}

size_t SpdbPairedBloomBitsBuilder::GetNumProbes() {
  return CalcNumProbes(millibits_per_key_);
}

double SpdbPairedBloomBitsBuilder::EstimatedFpRate(
    size_t /*num_entries*/, size_t /*len_with_metadata*/) {
  auto raw_num_probes = CalcRawNumProbes(millibits_per_key_);

  double adjusted_bits_per_key = CalcAdjustedBitsPerKey(millibits_per_key_);
  return SpdbStandardFpRate(adjusted_bits_per_key, raw_num_probes);
}

size_t SpdbPairedBloomBitsBuilder::RoundDownUsableSpace(size_t available_size) {
  size_t rv = available_size - speedb_filter::FilterMetadata::kMetadataLen;

  // round down to multiple of a Batch for bottomost level, and round up for
  // other levels
  if (is_bottomost_) {
    rv = std::max<size_t>((rv / kBatchSizeInBytes) * kBatchSizeInBytes,
                          kBatchSizeInBytes);
  } else {
    rv = static_cast<size_t>(
        std::ceil(static_cast<double>(rv) / kBatchSizeInBytes) *
        kBatchSizeInBytes);
  }

  if (rv >= kMaxSupportedSizeNoMetadata) {
    // Max supported for this data structure implementation
    rv = kMaxSupportedSizeNoMetadata;
  }

  return rv + speedb_filter::FilterMetadata::kMetadataLen;
}

FilterBitsReader* SpdbPairedBloomBitsBuilder::GetBitsReader(
    const Slice& filter_content) {
  assert(reader_create_func_ != nullptr);
  return reader_create_func_ ? reader_create_func_(filter_content) : nullptr;
}

void SpdbPairedBloomBitsBuilder::InitBlockHistogram() {
  blocks_histogram_.resize(num_batches_);
  AddCacheReservation(num_batches_ *
                      sizeof(decltype(blocks_histogram_)::value_type));

  for (auto batch_idx = 0U; batch_idx < blocks_histogram_.size(); ++batch_idx) {
    for (uint8_t in_batch_block_idx = 0;
         in_batch_block_idx < blocks_histogram_[batch_idx].size();
         ++in_batch_block_idx) {
      blocks_histogram_[batch_idx][in_batch_block_idx]
          .original_in_batch_block_idx = in_batch_block_idx;
    }
  }
}

void SpdbPairedBloomBitsBuilder::BuildBlocksHistogram(uint32_t data_len_bytes) {
  for (const auto& hash : hash_entries_info_.entries) {
    const uint32_t global_block_idx =
        HashToGlobalBlockIdx(Lower32of64(hash), data_len_bytes);
    const uint8_t in_batch_block_idx = GetInBatchBlockIdx(global_block_idx);
    const uint32_t batch_idx = GetContainingBatchIdx(global_block_idx);

    ++blocks_histogram_[batch_idx][in_batch_block_idx].num_keys;
  }
}

void SpdbPairedBloomBitsBuilder::SortBatchBlocks(uint32_t batch_idx) {
  assert(batch_idx < num_batches_);
  BatchBlocksHistogram& batch_blocks_histrogram = blocks_histogram_[batch_idx];
  std::stable_sort(batch_blocks_histrogram.begin(),
                   batch_blocks_histrogram.end());
}

void SpdbPairedBloomBitsBuilder::PairBatchBlocks(uint32_t batch_idx) {
  assert(batch_idx < num_batches_);
  BatchBlocksHistogram& batch_blocks_histrogram = blocks_histogram_[batch_idx];
  auto& batch_pairing_info = pairing_table_[batch_idx];

  for (auto in_batch_block_idx = 0U;
       in_batch_block_idx < speedb_filter::kPairedBloomBatchSizeInBlocks;
       ++in_batch_block_idx) {
    const auto pair_in_batch_block_idx =
        batch_blocks_histrogram.size() - in_batch_block_idx - 1;
    auto original_in_batch_block_idx =
        batch_blocks_histrogram[in_batch_block_idx].original_in_batch_block_idx;

    batch_pairing_info[original_in_batch_block_idx].pair_in_batch_block_idx =
        batch_blocks_histrogram[pair_in_batch_block_idx]
            .original_in_batch_block_idx;
    batch_pairing_info[original_in_batch_block_idx].hash_set_selector =
        GetHashSetSelector(original_in_batch_block_idx,
                           batch_blocks_histrogram[pair_in_batch_block_idx]
                               .original_in_batch_block_idx);
  }
}

void SpdbPairedBloomBitsBuilder::PairBlocks() {
  for (auto batch_idx = 0U; batch_idx < num_batches_; ++batch_idx) {
    SortBatchBlocks(batch_idx);
    PairBatchBlocks(batch_idx);
  }
}

void SpdbPairedBloomBitsBuilder::SetBlocksPairs(char* data) {
  for (auto batch_idx = 0U; batch_idx < pairing_table_.size(); ++batch_idx) {
    for (auto in_batch_block_idx = 0U;
         in_batch_block_idx < speedb_filter::kPairedBloomBatchSizeInBlocks;
         ++in_batch_block_idx) {
      uint32_t global_block_idx =
          batch_idx * speedb_filter::kPairedBloomBatchSizeInBlocks +
          in_batch_block_idx;
      BuildBlock block(data, global_block_idx, false /* prefetch */);
      const uint32_t pair_in_batch_block_idx =
          pairing_table_[batch_idx][in_batch_block_idx].pair_in_batch_block_idx;
      block.SetInBatchBlockIdxOfPair(
          static_cast<uint8_t>(pair_in_batch_block_idx));
    }
  }
}

//
// Build the blocks in similarly to how Rocksd does it
// The idea is to trigger blocks prefetching in batches, and access the
// prefetched blocks in batches.
void SpdbPairedBloomBitsBuilder::BuildBlocks(char* data,
                                             uint32_t data_len_bytes) {
  const size_t num_entries = hash_entries_info_.entries.size();
  constexpr size_t kBufferMask = 7;
  static_assert(((kBufferMask + 1) & kBufferMask) == 0,
                "Must be power of 2 minus 1");

  constexpr auto kArraySize = kBufferMask + 1;
  std::array<BuildBlock, kArraySize> primary_blocks;
  std::array<BuildBlock, kArraySize> secondary_blocks;
  std::array<uint8_t, kArraySize> primary_hash_selectors;
  std::array<uint32_t, kArraySize> upper_32_bits_of_hashes;

  auto const hash_set_size = num_probes_ / 2;

  size_t i = 0;
  std::deque<uint64_t>::iterator hash_entries_it =
      hash_entries_info_.entries.begin();

  for (; i <= kBufferMask && i < num_entries; ++i) {
    uint64_t hash = *hash_entries_it;

    // Primary Block
    uint32_t primary_global_block_idx =
        HashToGlobalBlockIdx(Lower32of64(hash), data_len_bytes);
    new (&primary_blocks[i]) BuildBlock(data, primary_global_block_idx, true);

    const uint32_t batch_idx = GetContainingBatchIdx(primary_global_block_idx);
    const uint8_t primary_in_batch_block_idx =
        GetInBatchBlockIdx(primary_global_block_idx);
    const uint32_t secondary_in_batch_block_idx =
        pairing_table_[batch_idx][primary_in_batch_block_idx]
            .pair_in_batch_block_idx;

    primary_hash_selectors[i] = GetHashSetSelector(
        primary_in_batch_block_idx, secondary_in_batch_block_idx);

    const uint32_t secondary_global_block_idx =
        GetFirstGlobalBlockIdxOfBatch(batch_idx) + secondary_in_batch_block_idx;
    new (&secondary_blocks[i])
        BuildBlock(data, secondary_global_block_idx, true);

    upper_32_bits_of_hashes[i] = Upper32of64(hash);
    ++hash_entries_it;
  }

  // Process and buffer
  for (; i < num_entries; ++i) {
    auto idx = i & kBufferMask;
    uint32_t& upper_32_bits_of_hash_ref = upper_32_bits_of_hashes[idx];
    auto& primary_block_ref = primary_blocks[idx];
    auto& secondary_block_ref = secondary_blocks[idx];
    auto& primary_hash_selector_ref = primary_hash_selectors[idx];

    primary_block_ref.SetBlockBloomBits(
        upper_32_bits_of_hash_ref, primary_hash_selector_ref, hash_set_size);
    secondary_block_ref.SetBlockBloomBits(upper_32_bits_of_hash_ref,
                                          1 - primary_hash_selector_ref,
                                          hash_set_size);
    // And buffer
    uint64_t hash = *hash_entries_it;

    // Primary Block
    uint32_t primary_global_block_idx =
        HashToGlobalBlockIdx(Lower32of64(hash), data_len_bytes);
    new (&primary_block_ref) BuildBlock(data, primary_global_block_idx, true);

    const uint32_t batch_idx = GetContainingBatchIdx(primary_global_block_idx);
    const uint8_t primary_in_batch_block_idx =
        GetInBatchBlockIdx(primary_global_block_idx);
    const uint32_t secondary_in_batch_block_idx =
        pairing_table_[batch_idx][primary_in_batch_block_idx]
            .pair_in_batch_block_idx;
    primary_hash_selector_ref = GetHashSetSelector(
        primary_in_batch_block_idx, secondary_in_batch_block_idx);
    const uint32_t secondary_global_block_idx =
        GetFirstGlobalBlockIdxOfBatch(batch_idx) + secondary_in_batch_block_idx;
    new (&secondary_block_ref)
        BuildBlock(data, secondary_global_block_idx, true);

    upper_32_bits_of_hash_ref = Upper32of64(hash);
    ++hash_entries_it;
  }

  // Finish processing
  for (i = 0; i <= kBufferMask && i < num_entries; ++i) {
    primary_blocks[i].SetBlockBloomBits(
        upper_32_bits_of_hashes[i], primary_hash_selectors[i], hash_set_size);
    secondary_blocks[i].SetBlockBloomBits(upper_32_bits_of_hashes[i],
                                          1 - primary_hash_selectors[i],
                                          hash_set_size);
  }
}

void SpdbPairedBloomBitsBuilder::AddAllEntries(char* data,
                                               uint32_t data_len_bytes) {
  InitBlockHistogram();
  BuildBlocksHistogram(data_len_bytes);
  PairBlocks();
  SetBlocksPairs(data);
  BuildBlocks(data, data_len_bytes);
  CleanupBuildData();
}

void SpdbPairedBloomBitsBuilder::CleanupBuildData() {
  blocks_histogram_.clear();
  blocks_histogram_.shrink_to_fit();

  pairing_table_.clear();
  pairing_table_.shrink_to_fit();

  internal_cache_res_handles_.clear();
  internal_cache_res_handles_.shrink_to_fit();
}

void SpdbPairedBloomBitsBuilder::AddCacheReservation(
    std::size_t incremental_memory_used) {
  if (cache_res_mgr_) {
    std::unique_ptr<CacheReservationManager::CacheReservationHandle>
        filter_cache_res_handle;
    Status s = cache_res_mgr_->MakeCacheReservation(incremental_memory_used,
                                                    &filter_cache_res_handle);
    s.PermitUncheckedError();

    internal_cache_res_handles_.push_back(std::move(filter_cache_res_handle));
  }
}

// =======================================================================================================================
bool SpdbPairedBloomBitsReader::HashMayMatch(const uint64_t hash) {
  uint32_t primary_global_block_idx =
      HashToGlobalBlockIdx(Lower32of64(hash), data_len_bytes_);
  // Not prefetching as performance seems to improve
  // TODO: Needs additional verification
  ReadBlock primary_block(data_, primary_global_block_idx, true /* prefetch */);

  uint8_t primary_in_batch_block_idx =
      GetInBatchBlockIdx(primary_global_block_idx);
  uint8_t secondary_in_batch_block_idx =
      primary_block.GetInBatchBlockIdxOfPair();
  auto primary_block_hash_selector = GetHashSetSelector(
      primary_in_batch_block_idx, secondary_in_batch_block_idx);

  auto const hash_set_size = num_probes_ / 2;

  const uint32_t upper_32_bits_of_hash = Upper32of64(hash);
  if (primary_block.AreAllBlockBloomBitsSet(upper_32_bits_of_hash,
                                            primary_block_hash_selector,
                                            hash_set_size) == false) {
    return false;
  }

  uint8_t secondary_block_hash_selector = 1 - primary_block_hash_selector;
  uint32_t batch_idx = GetContainingBatchIdx(primary_global_block_idx);
  uint32_t secondary_global_block_idx =
      GetFirstGlobalBlockIdxOfBatch(batch_idx) + secondary_in_batch_block_idx;

  ReadBlock secondary_block(data_, secondary_global_block_idx,
                            true /* prefetch */);
  return secondary_block.AreAllBlockBloomBitsSet(
      upper_32_bits_of_hash, secondary_block_hash_selector, hash_set_size);
}

bool SpdbPairedBloomBitsReader::MayMatch(const Slice& key) {
  uint64_t hash = GetSliceHash64(key);
  return HashMayMatch(hash);
}

// TODO: COPY Rocksdb's approach for multi-keys to improve performance
// (prefetch blocks)
void SpdbPairedBloomBitsReader::MayMatch(int num_keys, Slice** keys,
                                         bool* may_match) {
  for (auto i = 0; i < num_keys; ++i) {
    may_match[i] = MayMatch(*keys[i]);
  }
}

}  // namespace ROCKSDB_NAMESPACE
