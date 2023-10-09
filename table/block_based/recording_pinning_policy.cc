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

#include "table/block_based/recording_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {

static const uint8_t kNumTypes = 7;
static const int kNumLevels = 7;

RecordingPinningPolicy::RecordingPinningPolicy()
    : usage_(0), attempts_counter_(0), pinned_counter_(0), active_counter_(0) {
  // ,
  // usage_by_level_(kNumLevels + 1),
  // usage_by_type_(kNumTypes) {
  // for (int l = 0; l <= kNumLevels; l++) {
  //   usage_by_level_[l].store(0);
  // }
  // for (uint8_t t = 0; t < kNumTypes; t++) {
  //   usage_by_type_[t].store(0);
  // }
}

RecordingPinningPolicy::~RecordingPinningPolicy() {
  // fprintf(stderr, "%s\n", ToString().c_str());
}

bool RecordingPinningPolicy::MayPin(const TablePinningInfo& tpi,
                                    HierarchyCategory category,
                                    CacheEntryRole role, size_t size) const {
  attempts_counter_++;
  auto check_pin = CheckPin(tpi, category, role, size, usage_);
  // printf("MayPin: category=%s, role=%s, level=%d, check_pin=%d\n",
  // GetHierarchyCategoryName(category).c_str(),
  // GetCacheEntryRoleName(role).c_str(), tpi.level, check_pin); return
  // CheckPin(tpi, category, role, size, usage_);
  return check_pin;
}

bool RecordingPinningPolicy::PinData(const TablePinningInfo& tpi,
                                     HierarchyCategory category,
                                     CacheEntryRole role, size_t size,
                                     std::unique_ptr<PinnedEntry>* pinned) {
  auto limit = usage_.fetch_add(size);
  if (CheckPin(tpi, category, role, size, limit)) {
    // printf("PinData: category=%s, role=%s, level=%d\n",
    // GetHierarchyCategoryName(category).c_str(),
    // GetCacheEntryRoleName(role).c_str(), tpi.level);
    pinned_counter_++;
    pinned->reset(new PinnedEntry(tpi.level, tpi.is_last_level_with_data,
                                  category, tpi.item_owner_id, role, size));
    RecordPinned(tpi.level, category, tpi.item_owner_id, role, size, true);
    return true;
  } else {
    usage_.fetch_sub(size);
    return false;
  }
}

void RecordingPinningPolicy::UnPinData(std::unique_ptr<PinnedEntry>&& pinned) {
  RecordPinned(pinned->level, pinned->category, pinned->item_owner_id,
               pinned->role, pinned->size, false);
  usage_ -= pinned->size;
  pinned.reset();
}

void RecordingPinningPolicy::RecordPinned(int level,
                                          HierarchyCategory /*category*/,
                                          Cache::ItemOwnerId /*item_owner_id*/,
                                          CacheEntryRole /*role*/,
                                          size_t /*size*/, bool pinned) {
  if (level < 0 || level > kNumLevels) level = kNumLevels;
  // XXXXXX if (type >= kNumTypes) type = kNumTypes - 1;
  if (pinned) {
    // usage_by_level_[level] += size;
    // usage_by_type_[type] += size;
    active_counter_++;
  } else {
    // usage_by_level_[level] -= size;
    // usage_by_type_[type] -= size;
    assert(active_counter_ > 0U);
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

size_t RecordingPinningPolicy::GetPinnedUsageByLevel(int /*level*/) const {
  return 0U;
  // if (level > kNumLevels) level = kNumLevels;
  // return usage_by_level_[level];
}

size_t RecordingPinningPolicy::GetPinnedUsageByType(uint8_t /*type*/) const {
  return 0U;
  // if (type >= kNumTypes) type = kNumTypes - 1;
  // return usage_by_type_[type];
}

}  // namespace ROCKSDB_NAMESPACE
