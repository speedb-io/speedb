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

#include "plugin/speedb/paired_filter/speedb_paired_bloom.h"

#include "plugin/speedb/paired_filter/speedb_paired_bloom_internal.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/filter_policy_internal.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
SpdbPairedBloomFilterPolicy::SpdbPairedBloomFilterPolicy(double bits_per_key) {
  constexpr double kMinBitsPerKey = speedb_filter::kMinMillibitsPerKey / 1000;

  // Sanitize bits_per_key
  if (bits_per_key < 0.5) {
    // Round down to no filter
    bits_per_key = 0;
  } else if (bits_per_key < kMinBitsPerKey) {
    // Minimum 1 bit per key (equiv) when creating filter
    bits_per_key = kMinBitsPerKey;
  } else if (!(bits_per_key < kMaxBitsPerKey)) {  // including NaN
    bits_per_key = kMaxBitsPerKey;
  }

  // Includes a nudge toward rounding up, to ensure on all platforms
  // that doubles specified with three decimal digits after the decimal
  // point are interpreted accurately.
  millibits_per_key_ = static_cast<int>(bits_per_key * 1000.0 + 0.500001);
}

FilterBitsBuilder* SpdbPairedBloomFilterPolicy::GetBuilderWithContext(
    const FilterBuildingContext& context) const {
  if (millibits_per_key_ == 0) {
    // "No filter" special case
    return nullptr;
  }

  // TODO: The code below is duplicates from
  // BloomLikeFilterPolicy::GetFastLocalBloomBuilderWithContext
  // TODO: See if it may be refactored to a static method

  // The paired bloom filter is not supporting the 'optimize_filters_for_memory'
  // option
  // => offm is set to false unconditionally instead of to the value of
  // context.table_options.optimize_filters_for_memory
  // https://github.com/speedb-io/speedb/issues/488
  bool offm = false;
  const auto options_overrides_iter =
      context.table_options.cache_usage_options.options_overrides.find(
          CacheEntryRole::kFilterConstruction);
  const auto filter_construction_charged =
      options_overrides_iter !=
              context.table_options.cache_usage_options.options_overrides.end()
          ? options_overrides_iter->second.charged
          : context.table_options.cache_usage_options.options.charged;

  // TODO: Refactor this to a static method of BloomLikeFilterPolicy
  std::shared_ptr<CacheReservationManager> cache_res_mgr;
  if (context.table_options.block_cache &&
      filter_construction_charged ==
          CacheEntryRoleOptions::Decision::kEnabled) {
    cache_res_mgr = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kFilterConstruction>>(
        context.table_options.block_cache);
  }

  return new SpdbPairedBloomBitsBuilder(
      millibits_per_key_, offm ? &aggregate_rounding_balance_ : nullptr,
      cache_res_mgr, context.table_options.detect_filter_construct_corruption,
      std::bind(&SpdbPairedBloomFilterPolicy::GetFilterBitsReader, this,
                std::placeholders::_1),
      context.is_bottommost);
}

FilterBitsReader* SpdbPairedBloomFilterPolicy::GetFilterBitsReader(
    const Slice& contents) const {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  const auto trailer_len = speedb_filter::FilterMetadata::kMetadataLen;
  if (len_with_meta <= trailer_len) {
    // filter is empty or broken. Treat like zero keys added.
    return new AlwaysFalseFilter();
  }

  const auto len = len_with_meta - trailer_len;
  const char* metadata_start = &contents.data()[len];

  auto trailer_data =
      speedb_filter::FilterMetadata::ReadMetadata(metadata_start);
  switch (trailer_data.filter_type) {
    case speedb_filter::FilterType::kPairedBlockBloom:
      return new SpdbPairedBloomBitsReader(contents.data(),
                                           trailer_data.num_probes, len);
      break;

    case speedb_filter::FilterType::kFutureUnknown:
      return new AlwaysTrueFilter();
      break;

    default:
      assert(0);
      return new AlwaysTrueFilter();
  }
}

std::string SpdbPairedBloomFilterPolicy::GetId() const {
  return Name() +
         BloomLikeFilterPolicy::GetBitsPerKeySuffix(millibits_per_key_);
}

bool SpdbPairedBloomFilterPolicy::IsInstanceOf(const std::string& name) const {
  if (name == kClassName()) {
    return true;
  } else {
    return FilterPolicy::IsInstanceOf(name);
  }
}

const char* SpdbPairedBloomFilterPolicy::kClassName() {
  return "speedb_paired_bloom_filter";
}

const char* SpdbPairedBloomFilterPolicy::kNickName() {
  return "speedb.PairedBloomFilter";
}

}  // namespace ROCKSDB_NAMESPACE
