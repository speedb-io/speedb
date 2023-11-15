//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "utilities/persistent_cache/persistent_cache_tier.h"

#include <cinttypes>
#include <sstream>
#include <string>

#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {
// OptionTypeInfo map for PersistentCacheConfig
static std::unordered_map<std::string, OptionTypeInfo>
    persistent_cache_config_options_type_info = {
        {"path",
         {offsetof(struct PersistentCacheConfig, path), OptionType::kString}},
        {"enable_direct_reads",
         {offsetof(struct PersistentCacheConfig, enable_direct_reads),
          OptionType::kBoolean}},
        {"enable_direct_writes",
         {offsetof(struct PersistentCacheConfig, enable_direct_writes),
          OptionType::kBoolean}},
        {"cache_size",
         {offsetof(struct PersistentCacheConfig, cache_size),
          OptionType::kUInt64T}},
        {"cache_file_size",
         {offsetof(struct PersistentCacheConfig, cache_file_size),
          OptionType::kUInt32T}},
        {"writer_qdepth",
         {offsetof(struct PersistentCacheConfig, writer_qdepth),
          OptionType::kUInt32T}},
        {"pipeline_writes",
         {offsetof(struct PersistentCacheConfig, pipeline_writes),
          OptionType::kBoolean}},
        {"max_write_pipeline_backlog_size",
         {offsetof(struct PersistentCacheConfig,
                   max_write_pipeline_backlog_size),
          OptionType::kUInt64T}},
        {"write_buffer_size",
         {offsetof(struct PersistentCacheConfig, write_buffer_size),
          OptionType::kUInt32T}},
        {"writer_dispatch_size",
         {offsetof(struct PersistentCacheConfig, writer_dispatch_size),
          OptionType::kUInt64T}},
        {"is_compressed",
         {offsetof(struct PersistentCacheConfig, is_compressed),
          OptionType::kBoolean}},
};

Status PersistentCacheConfig::SerializeOptions(
    const ConfigOptions& config_options, const std::string& prefix,
    OptionProperties* options) const {
  return OptionTypeInfo::SerializeType(
      config_options, prefix, persistent_cache_config_options_type_info, this,
      options);
}

std::string PersistentCacheConfig::ToString(
    const ConfigOptions& config_options) const {
  OptionProperties props;
  auto status = SerializeOptions(config_options, "", &props);
  assert(status.ok());
  if (status.ok()) {
    return config_options.ToString("", props);
  } else {
    return "";
  }
}

std::string PersistentCache::ToString(
    const ConfigOptions& config_options) const {
  //**TODO: This method is needed until PersistentCache is Customizable
  OptionProperties options;
  std::string id = Name();
  options.insert({OptionTypeInfo::kIdPropName(), id});
  Status s = SerializePrintableOptions(config_options, "", &options);
  assert(s.ok());
  if (s.ok()) {
    return config_options.ToString("", options);
  } else {
    return id;
  }
}
//
// PersistentCacheTier implementation
//
Status PersistentCacheTier::Open() {
  if (next_tier_) {
    return next_tier_->Open();
  }
  return Status::OK();
}

Status PersistentCacheTier::Close() {
  if (next_tier_) {
    return next_tier_->Close();
  }
  return Status::OK();
}

bool PersistentCacheTier::Reserve(const size_t /*size*/) {
  // default implementation is a pass through
  return true;
}

bool PersistentCacheTier::Erase(const Slice& /*key*/) {
  // default implementation is a pass through since not all cache tiers might
  // support erase
  return true;
}

std::string PersistentCacheTier::PrintStats() {
  std::ostringstream os;
  for (auto tier_stats : Stats()) {
    os << "---- next tier -----" << std::endl;
    for (auto stat : tier_stats) {
      os << stat.first << ": " << stat.second << std::endl;
    }
  }
  return os.str();
}

PersistentCache::StatsType PersistentCacheTier::Stats() {
  if (next_tier_) {
    return next_tier_->Stats();
  }
  return PersistentCache::StatsType{};
}

uint64_t PersistentCacheTier::NewId() {
  return last_id_.fetch_add(1, std::memory_order_relaxed);
}

//
// PersistentTieredCache implementation
//
PersistentTieredCache::~PersistentTieredCache() { assert(tiers_.empty()); }

Status PersistentTieredCache::Open() {
  assert(!tiers_.empty());
  return tiers_.front()->Open();
}

Status PersistentTieredCache::Close() {
  assert(!tiers_.empty());
  Status status = tiers_.front()->Close();
  if (status.ok()) {
    tiers_.clear();
  }
  return status;
}

bool PersistentTieredCache::Erase(const Slice& key) {
  assert(!tiers_.empty());
  return tiers_.front()->Erase(key);
}

PersistentCache::StatsType PersistentTieredCache::Stats() {
  assert(!tiers_.empty());
  return tiers_.front()->Stats();
}

std::string PersistentTieredCache::PrintStats() {
  assert(!tiers_.empty());
  return tiers_.front()->PrintStats();
}

Status PersistentTieredCache::Insert(const Slice& page_key, const char* data,
                                     const size_t size) {
  assert(!tiers_.empty());
  return tiers_.front()->Insert(page_key, data, size);
}

Status PersistentTieredCache::Lookup(const Slice& page_key,
                                     std::unique_ptr<char[]>* data,
                                     size_t* size) {
  assert(!tiers_.empty());
  return tiers_.front()->Lookup(page_key, data, size);
}

void PersistentTieredCache::AddTier(const Tier& tier) {
  if (!tiers_.empty()) {
    tiers_.back()->set_next_tier(tier);
  }
  tiers_.push_back(tier);
}

bool PersistentTieredCache::IsCompressed() {
  assert(tiers_.size());
  return tiers_.front()->IsCompressed();
}

}  // namespace ROCKSDB_NAMESPACE
