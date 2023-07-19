//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include "rocksdb/table_pinning_policy.h"

#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/recording_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class DefaultPinningPolicy : public RecordingPinningPolicy {
 public:
  DefaultPinningPolicy() {
    //**TODO: Register options?
  }

  DefaultPinningPolicy(const BlockBasedTableOptions& bbto)
      : DefaultPinningPolicy(bbto.metadata_cache_options,
                             bbto.pin_top_level_index_and_filter,
                             bbto.pin_l0_filter_and_index_blocks_in_cache) {}

  DefaultPinningPolicy(const MetadataCacheOptions& mdco, bool pin_top,
                       bool pin_l0)
      : cache_options_(mdco),
        pin_top_level_index_and_filter_(pin_top),
        pin_l0_index_and_filter_(pin_l0) {
    //**TODO: Register options?
  }
  static const char* kClassName() { return "DefaultPinningPolicy"; }
  const char* Name() const override { return kClassName(); }

 protected:
  bool CheckPin(const TablePinningOptions& tpo, uint8_t type, size_t /*size*/,
                size_t /*limit*/) const override {
    if (tpo.level < 0) {
      return false;
    } else if (type == kTopLevel) {
      return IsPinned(tpo, cache_options_.top_level_index_pinning,
                      pin_top_level_index_and_filter_ ? PinningTier::kAll
                                                      : PinningTier::kNone);
    } else if (type == kPartition) {
      return IsPinned(tpo, cache_options_.partition_pinning,
                      pin_l0_index_and_filter_ ? PinningTier::kFlushedAndSimilar
                                               : PinningTier::kNone);
    } else {
      return IsPinned(tpo, cache_options_.unpartitioned_pinning,
                      pin_l0_index_and_filter_ ? PinningTier::kFlushedAndSimilar
                                               : PinningTier::kNone);
    }
  }

 private:
  bool IsPinned(const TablePinningOptions& tpo, PinningTier pinning_tier,
                PinningTier fallback_pinning_tier) const {
    // Fallback to fallback would lead to infinite recursion. Disallow it.
    assert(fallback_pinning_tier != PinningTier::kFallback);

    switch (pinning_tier) {
      case PinningTier::kFallback:
        return IsPinned(tpo, fallback_pinning_tier,
                        PinningTier::kNone /* fallback_pinning_tier */);
      case PinningTier::kNone:
        return false;
      case PinningTier::kFlushedAndSimilar:
        return tpo.level == 0 &&
               tpo.file_size <= tpo.max_file_size_for_l0_meta_pin;
      case PinningTier::kAll:
        return true;
      default:
        assert(false);
        return false;
    }
  }

 private:
  const MetadataCacheOptions cache_options_;
  bool pin_top_level_index_and_filter_ = true;
  bool pin_l0_index_and_filter_ = false;
};
}  // namespace

TablePinningPolicy* NewDefaultPinningPolicy(
    const BlockBasedTableOptions& bbto) {
  return new DefaultPinningPolicy(bbto);
}

static const uint8_t kNumTypes = 7;
static const int kNumLevels = 7;

RecordingPinningPolicy::RecordingPinningPolicy()
    : usage_(0),
      attempts_counter_(0),
      pinned_counter_(0),
      active_counter_(0),
      usage_by_level_(kNumLevels + 1),
      usage_by_type_(kNumTypes) {
  for (int l = 0; l <= kNumLevels; l++) {
    usage_by_level_[l].store(0);
  }
  for (uint8_t t = 0; t < kNumTypes; t++) {
    usage_by_type_[t].store(0);
  }
}

bool RecordingPinningPolicy::MayPin(const TablePinningOptions& tpo,
                                    uint8_t type, size_t size) const {
  attempts_counter_++;
  return CheckPin(tpo, type, size, usage_);
}

bool RecordingPinningPolicy::PinData(const TablePinningOptions& tpo,
                                     uint8_t type, size_t size,
                                     std::unique_ptr<PinnedEntry>* pinned) {
  auto limit = usage_.fetch_add(size);
  if (CheckPin(tpo, type, size, limit)) {
    pinned_counter_++;
    pinned->reset(new PinnedEntry(tpo.level, type, size));
    RecordPinned(tpo.level, type, size, true);
    return true;
  } else {
    usage_.fetch_sub(size);
    return false;
  }
}

void RecordingPinningPolicy::UnPinData(std::unique_ptr<PinnedEntry>&& pinned) {
  RecordPinned(pinned->level, pinned->type, pinned->size, false);
  usage_ -= pinned->size;
  pinned.reset();
}

void RecordingPinningPolicy::RecordPinned(int level, uint8_t type, size_t size,
                                          bool pinned) {
  if (level < 0 || level > kNumLevels) level = kNumLevels;
  if (type >= kNumTypes) type = kNumTypes - 1;
  if (pinned) {
    usage_by_level_[level] += size;
    usage_by_type_[type] += size;
    active_counter_++;
  } else {
    usage_by_level_[level] -= size;
    usage_by_type_[type] -= size;
    active_counter_--;
  }
}

std::string RecordingPinningPolicy::ToString() const {
  std::string result;
  result.append("Pinned Memory=")
      .append(std::to_string(usage_.load()))
      .append("\n");
  result.append("Pinned Attempts=")
      .append(std::to_string(attempts_counter_.load()))
      .append("\n");
  result.append("Pinned Counter=")
      .append(std::to_string(pinned_counter_.load()))
      .append("\n");
  result.append("Active Counter=")
      .append(std::to_string(active_counter_.load()))
      .append("\n");
  return result;
}
size_t RecordingPinningPolicy::GetPinnedUsage() const { return usage_; }

size_t RecordingPinningPolicy::GetPinnedUsageByLevel(int level) const {
  if (level > kNumLevels) level = kNumLevels;
  return usage_by_level_[level];
}

size_t RecordingPinningPolicy::GetPinnedUsageByType(uint8_t type) const {
  if (type >= kNumTypes) type = kNumTypes - 1;
  return usage_by_type_[type];
}

#ifndef ROCKSDB_LITE
static int RegisterBuiltinPinningPolicies(ObjectLibrary& library,
                                          const std::string& /*arg*/) {
  library.AddFactory<TablePinningPolicy>(
      DefaultPinningPolicy::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<TablePinningPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(new DefaultPinningPolicy(BlockBasedTableOptions()));
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
#endif  // ROCKSDB_LITE

Status TablePinningPolicy::CreateFromString(
    const ConfigOptions& options, const std::string& value,
    std::shared_ptr<TablePinningPolicy>* policy) {
#ifndef ROCKSDB_LITE
  static std::once_flag loaded;
  std::call_once(loaded, [&]() {
    RegisterBuiltinPinningPolicies(*(ObjectLibrary::Default().get()), "");
  });
#endif  // ROCKSDB_LITE
  return LoadManagedObject<TablePinningPolicy>(options, value, policy);
}

}  // namespace ROCKSDB_NAMESPACE
