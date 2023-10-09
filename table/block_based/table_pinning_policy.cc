// Copyright (C) 2023 Speedb Ltd. All rights reserved.
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

#include <array>

#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/recording_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {

namespace {
std::array<std::string, kNumHierarchyCategories>
    kHierarchyCategoryToHyphenString{{"top-level", "partition", "other"}};
}  // Unnamed namespace

std::string GetHierarchyCategoryName(HierarchyCategory category) {
  return kHierarchyCategoryToHyphenString[static_cast<size_t>(category)];
}

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
  bool CheckPin(const TablePinningInfo& tpo, HierarchyCategory category,
                CacheEntryRole /*role*/, size_t /*size*/,
                size_t /*limit*/) const override {
    if (tpo.level < 0) {
      return false;
    } else if (category == HierarchyCategory::TOP_LEVEL) {
      return IsPinned(tpo, cache_options_.top_level_index_pinning,
                      pin_top_level_index_and_filter_ ? PinningTier::kAll
                                                      : PinningTier::kNone);
    } else if (category == HierarchyCategory::PARTITION) {
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
  bool IsPinned(const TablePinningInfo& tpo, PinningTier pinning_tier,
                PinningTier fallback_pinning_tier) const {
    // Fallback to fallback would lead to infinite recursion. Disallow it.
    assert(fallback_pinning_tier != PinningTier::kFallback);

    // printf("pinning_tier=%d\n", (int)pinning_tier);

    switch (pinning_tier) {
      case PinningTier::kFallback:
        return IsPinned(tpo, fallback_pinning_tier,
                        PinningTier::kNone /* fallback_pinning_tier */);
      case PinningTier::kNone:
        return false;
      case PinningTier::kFlushedAndSimilar: {
        bool answer = (tpo.level == 0 &&
                       tpo.file_size <= tpo.max_file_size_for_l0_meta_pin);
        // printf("kFlushedAndSimilar: level=%d, file_size=%d,
        // max_file_size_for_l0_meta_pin=%d\n", tpo.level, (int)tpo.file_size,
        // (int)tpo.max_file_size_for_l0_meta_pin); return tpo.level == 0 &&
        //        tpo.file_size <= tpo.max_file_size_for_l0_meta_pin;
        return answer;
      }
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
