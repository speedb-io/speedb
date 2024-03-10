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
    : total_usage_(0),
      attempts_counter_(0),
      pinned_counter_(0),
      active_counter_(0) {}

bool RecordingPinningPolicy::MayPin(const TablePinningInfo& tpi,
                                    pinning::HierarchyCategory category,
                                    CacheEntryRole role, size_t size) const {
  attempts_counter_++;
  auto check_pin = CheckPin(tpi, category, role, size, total_usage_);
  return check_pin;
}

bool RecordingPinningPolicy::PinData(const TablePinningInfo& tpi,
                                     pinning::HierarchyCategory category,
                                     CacheEntryRole role, size_t size,
                                     std::unique_ptr<PinnedEntry>* pinned_entry) {
  auto curr_total_usage = total_usage_.fetch_add(size);
  auto new_total_usage = curr_total_usage + size;
  if (CheckPin(tpi, category, role, size, new_total_usage) == false) {
    // No actual pinning => Restore total usage
    total_usage_.store(curr_total_usage);
    return false;
  }

  pinned_counter_++;
  pinned_entry->reset(new PinnedEntry(tpi.level, tpi.is_last_level_with_data,
                                      category, tpi.item_owner_id, role, size));

  active_counter_++;

  if (tpi.item_owner_id == Cache::kUnknownItemOwnerId) {
    return true;
  }

  auto level_category_idx = static_cast<uint64_t>(
      pinning::GetLevelCategory(tpi.level, tpi.is_last_level_with_data));
  auto role_idx = static_cast<uint64_t>(role);

  std::lock_guard<std::mutex> lock(counters_mutex_);

  auto owner_id_pinned_counters_iter = pinned_counters_.find(tpi.item_owner_id);
  if (owner_id_pinned_counters_iter == pinned_counters_.end()) {
    auto insertion_result = pinned_counters_.insert(
        std::make_pair(tpi.item_owner_id, OwnerIdInfo()));
    assert(insertion_result.second);
    owner_id_pinned_counters_iter = insertion_result.first;
  }

  OwnerIdInfo& owner_id_info = owner_id_pinned_counters_iter->second;
  ++owner_id_info.ref_count;
  owner_id_info.counters[level_category_idx][role_idx]._a += size;

  return true;
}

void RecordingPinningPolicy::UnPinData(
    std::unique_ptr<PinnedEntry> pinned_entry) {
  auto curr_total_usage = total_usage_.fetch_sub(pinned_entry->size);
  assert(curr_total_usage >= pinned_entry->size);

  assert(active_counter_ > 0U);
  active_counter_--;

  if (pinned_entry->item_owner_id == Cache::kUnknownItemOwnerId) {
    return;
  }

  auto level_category_idx = static_cast<uint64_t>(pinning::GetLevelCategory(
      pinned_entry->level, pinned_entry->is_last_level_with_data));
  auto role_idx = static_cast<uint64_t>(pinned_entry->role);

  std::lock_guard<std::mutex> lock(counters_mutex_);

  auto owner_id_pinned_counters_iter =
      pinned_counters_.find(pinned_entry->item_owner_id);
  assert(owner_id_pinned_counters_iter != pinned_counters_.end());
  OwnerIdInfo& owner_id_info = owner_id_pinned_counters_iter->second;

  auto& entry_usage = owner_id_info.counters[level_category_idx][role_idx];
  auto old_entry_usage = entry_usage._a.fetch_sub(pinned_entry->size);
  assert(old_entry_usage >= pinned_entry->size);

  assert(owner_id_info.ref_count > 0U);
  --owner_id_info.ref_count;
  if (owner_id_info.ref_count == 0U) {
    auto is_total_entry_usage_zero = [this](Cache::ItemOwnerId item_owner_id) {
      auto total_entry_usage =
          this->GetOwnerIdTotalPinnedUsageNonLocking(item_owner_id);
      assert(total_entry_usage.has_value());
      return (total_entry_usage.value_or(0U) == 0U);
    };

    assert(is_total_entry_usage_zero(pinned_entry->item_owner_id));

    // Actually remove
    pinned_counters_.erase(owner_id_pinned_counters_iter);
  }
}

std::string RecordingPinningPolicy::ToString() const {
  std::string result;
  result.append("Pinned Memory=")
      .append(std::to_string(total_usage_.load()))
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
size_t RecordingPinningPolicy::GetPinnedUsage() const { return total_usage_; }

size_t RecordingPinningPolicy::GetOwnerIdTotalPinnedUsage(Cache::ItemOwnerId item_owner_id) const {
  std::lock_guard<std::mutex> lock(counters_mutex_);
  return GetOwnerIdTotalPinnedUsageNonLocking(item_owner_id).value_or(0U);
}

auto RecordingPinningPolicy::GetOwnerIdPinnedUsageCounters(Cache::ItemOwnerId item_owner_id) const -> OwnerIdPinnedCountersForQuery {
  std::lock_guard<std::mutex> lock(counters_mutex_);

  OwnerIdPinnedCountersForQuery query_counters;

  auto owner_id_pinned_counters_iter = pinned_counters_.find(item_owner_id);
  if (owner_id_pinned_counters_iter == pinned_counters_.end()) {
    return OwnerIdPinnedCountersForQuery();
  }

  // The counters are a two-dimensional array of std::atomic<int> which in non-copyable.
  // Deep copy to a consistent, non-atomic two-dimensional array.
  const OwnerIdInfo& owner_id_info = owner_id_pinned_counters_iter->second;
  const PerLevelCategoryAndRolePinnedCounters& owner_id_counters =
      owner_id_info.counters;
  for (auto level_category_idx = 0U; level_category_idx < owner_id_counters.size(); ++level_category_idx) {
    const PerRolePinnedCounters& role_counters = owner_id_counters[level_category_idx];
    for (auto role_idx = 0U; role_idx < role_counters.size(); ++role_idx) {
      query_counters[level_category_idx][role_idx] = role_counters[role_idx]._a;
    }
  }

  return query_counters;
}

// REQUIRED: counters_mutex_ is locked
std::optional<size_t>
RecordingPinningPolicy::GetOwnerIdTotalPinnedUsageNonLocking(
    Cache::ItemOwnerId item_owner_id) const {
  auto owner_id_pinned_info_iter = pinned_counters_.find(item_owner_id);
  if (owner_id_pinned_info_iter == pinned_counters_.end()) {
    return std::nullopt;
  }

  size_t total_pinned_usage = 0U;

  const OwnerIdInfo& owner_id_info = owner_id_pinned_info_iter->second;
  const PerLevelCategoryAndRolePinnedCounters& owner_id_counters =
      owner_id_info.counters;
  for (auto level_category_idx = 0U; level_category_idx < owner_id_counters.size(); ++level_category_idx) {
    const PerRolePinnedCounters& role_counters = owner_id_counters[level_category_idx];
    for (const auto& role_counter: role_counters) {
      total_pinned_usage += role_counter._a;
    }
  }

  return std::optional<size_t>(total_pinned_usage);
}

}  // namespace ROCKSDB_NAMESPACE
