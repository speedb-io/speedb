// TODO: ADD Speedb's Copyright Notice !!!!!

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
  bool offm = context.table_options.optimize_filters_for_memory;

  // TODO: Refactor this to a static method of BloomLikeFilterPolicy
  bool reserve_filter_construction_mem =
      (context.table_options.reserve_table_builder_memory &&
       context.table_options.block_cache);
  std::shared_ptr<CacheReservationManager> cache_res_mgr;
  if (reserve_filter_construction_mem) {
    cache_res_mgr = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kFilterConstruction>>(
        context.table_options.block_cache);
  }

  return new SpdbPairedBloomBitsBuilder(
      millibits_per_key_, offm ? &aggregate_rounding_balance_ : nullptr,
      cache_res_mgr, context.table_options.detect_filter_construct_corruption,
      std::bind(&SpdbPairedBloomFilterPolicy::GetFilterBitsReader, this,
                std::placeholders::_1));
}

FilterBitsReader* SpdbPairedBloomFilterPolicy::GetFilterBitsReader(
    const Slice& contents) const {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  const auto trailer_len = speedb_filter::FilterMetadata::MetadataLen;
  if (len_with_meta <= trailer_len) {
    // filter is empty or broken. Treat like zero keys added.
    return new AlwaysFalseFilter();
  }

  const auto len = len_with_meta - trailer_len;
  const char* metadata_start = &contents.data()[len];

  auto trailer_data =
      speedb_filter::FilterMetadata::ReadMetadata(metadata_start);
  switch (trailer_data.filter_type_) {
    case speedb_filter::FilterType::PairedBlockBloom:
      return new SpdbPairedBloomBitsReader(contents.data(),
                                           trailer_data.num_probes_, len);
      break;

    case speedb_filter::FilterType::FutureUnknown:
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
  return "spdb.PairedBloomFilter";
}

}  // namespace ROCKSDB_NAMESPACE
