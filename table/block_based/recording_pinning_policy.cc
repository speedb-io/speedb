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

#include <algorithm>
#include <numeric>
#include <utility>

#include "table/block_based/recording_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {

RecordingPinningPolicy::RecordingPinningPolicy()
    : usage_(0), attempts_counter_(0), pinned_counter_(0), active_counter_(0) {
}

RecordingPinningPolicy::~RecordingPinningPolicy() {
  // fprintf(stderr, "%s\n", ToString().c_str());
}

bool RecordingPinningPolicy::MayPin(const TablePinningInfo& tpi,
                                    pinning::HierarchyCategory category,
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
                                     pinning::HierarchyCategory category,
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
    RecordPinned(**pinned, true /* pinned */);
    return true;
  } else {
    usage_.fetch_sub(size);
    return false;
  }
}

void RecordingPinningPolicy::UnPinData(std::unique_ptr<PinnedEntry>&& pinned) {
  RecordPinned(*pinned, false /* pinned */);
  usage_ -= pinned->size;
  pinned.reset();
}

void RecordingPinningPolicy::RecordPinned(const PinnedEntry& pinned_entry, bool pinned) {
  auto owner_id_pinned_counters_iter = pinned_counters_.find(pinned_entry.item_owner_id);
  if (owner_id_pinned_counters_iter == pinned_counters_.end()) {
    assert(0);
    return;
  }

  OwnerIdPinnedCounters& owner_id_counters = owner_id_pinned_counters_iter->second;
  auto level_category_idx = static_cast<uint64_t>(pinning::GetLevelCategory(pinned_entry.level, pinned_entry.is_last_level_with_data));
  auto role_idx = static_cast<uint64_t>(pinned_entry.role);
  auto& usage = owner_id_counters[level_category_idx][role_idx];

  if (pinned) {
    usage._a += pinned_entry.size;
    active_counter_++;
  } else {
    assert(usage._a >= pinned_entry.size);
    usage._a -= pinned_entry.size;

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


size_t RecordingPinningPolicy::GetOwnerIdTotalPinnedUsage(Cache::ItemOwnerId item_owner_id) const {
  std::lock_guard<std::mutex> lock(counters_mutex_);
  return GetOwnerIdTotalPinnedUsageNonLocking(item_owner_id);
}

auto RecordingPinningPolicy::GetOwnerIdPinnedUsageCounters(Cache::ItemOwnerId item_owner_id) const -> OwnerIdPinnedCountersForQuery {
  std::lock_guard<std::mutex> lock(counters_mutex_);

  OwnerIdPinnedCountersForQuery query_counters;

  auto owner_id_pinned_counters_iter = pinned_counters_.find(item_owner_id);
  assert(owner_id_pinned_counters_iter != pinned_counters_.end());

  // The counters are a two-dimensional array of std::atomic<int> which in non-copyable.
  // Deep copy to a consistent, non-atomic two-dimensional array.
  const OwnerIdPinnedCounters& owner_id_counters = owner_id_pinned_counters_iter->second;
  for (auto level_category_idx = 0U; level_category_idx < owner_id_counters.size(); ++level_category_idx) {
    const PerRolePinnedCounters& role_counters = owner_id_counters[level_category_idx];
    for (auto role_idx = 0U; role_idx < role_counters.size(); ++role_idx) {
      query_counters[level_category_idx][role_idx] = role_counters[role_idx]._a;
    }
  }

  return query_counters;
}

void RecordingPinningPolicy::AddCacheItemOwnerId(Cache::ItemOwnerId item_owner_id) {
  std::lock_guard<std::mutex> lock(counters_mutex_);

  auto owner_id_pinned_counters_iter = pinned_counters_.find(item_owner_id);
  assert(owner_id_pinned_counters_iter == pinned_counters_.end());
  
  pinned_counters_[item_owner_id] = PerLevelAndRolePinnedCounters();
}

void RecordingPinningPolicy::RemoveCacheItemOwnerId(Cache::ItemOwnerId item_owner_id) {
  std::lock_guard<std::mutex> lock(counters_mutex_);

  auto owner_id_pinned_counters_iter = pinned_counters_.find(item_owner_id);
  if (owner_id_pinned_counters_iter == pinned_counters_.end()) {
    // This is a bug, detect during testing, don't crash in production
    assert(0);
    return;
  }
  // A removed owner must not have any pinned items
  assert(GetOwnerIdTotalPinnedUsageNonLocking(item_owner_id) == 0U);

  // Actually remove
  pinned_counters_.erase(owner_id_pinned_counters_iter);
}

// REQUIRED: counters_mutex_ is locked 
size_t RecordingPinningPolicy::GetOwnerIdTotalPinnedUsageNonLocking(Cache::ItemOwnerId item_owner_id) const {
  auto total_pinned_usage = 0U;

  auto owner_id_pinned_counters_iter = pinned_counters_.find(item_owner_id);
  assert(owner_id_pinned_counters_iter != pinned_counters_.end());

  const OwnerIdPinnedCounters& owner_id_counters = owner_id_pinned_counters_iter->second;
  for (auto level_category_idx = 0U; level_category_idx < owner_id_counters.size(); ++level_category_idx) {
    const PerRolePinnedCounters& role_counters = owner_id_counters[level_category_idx];
    for (const auto& role_counter: role_counters) {
      total_pinned_usage += role_counter._a;
    }
  }

  return total_pinned_usage;
}

}  // namespace ROCKSDB_NAMESPACE
