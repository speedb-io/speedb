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
#pragma once

#include <atomic>
#include <unordered_map>
#include <array>
#include <mutex>

#include "rocksdb/cache.h"
#include "rocksdb/table_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {
template <typename T>
struct AtomicWrapper {
  std::atomic<T> _a;

  AtomicWrapper():_a(0) {}
  AtomicWrapper(const std::atomic<T> &a):_a(a.load()) {}
  AtomicWrapper(const AtomicWrapper &other):_a(other._a.load()) {}

  AtomicWrapper& operator=(const AtomicWrapper& other) {
    _a.store(other._a.load());
    return *this;
  }

  void IncrementByOne() {
    ++_a;
  }
};

// An abstract table pinning policy that records the pinned operations
class RecordingPinningPolicy : public TablePinningPolicy {
 public:
  // using PerRolePinnedCounters = std::array<std::atomic<size_t>, kNumCacheEntryRoles>;
  using PerRolePinnedCounters = std::array<AtomicWrapper<size_t>, kNumCacheEntryRoles>;
  using PerLevelAndRolePinnedCounters = std::array<PerRolePinnedCounters, pinning::kNumLevelCategories>;
  using OwnerIdPinnedCounters = PerLevelAndRolePinnedCounters;

  // Equivalent types used for querying, NOT using atomic.
  using PerRolePinnedCountersForQuery = std::array<size_t, kNumCacheEntryRoles>;
  using OwnerIdPinnedCountersForQuery = std::array<PerRolePinnedCountersForQuery, pinning::kNumLevelCategories>;

 public:
  RecordingPinningPolicy();
  ~RecordingPinningPolicy();

  bool MayPin(const TablePinningInfo& tpi, pinning::HierarchyCategory category,
              CacheEntryRole role, size_t size) const override;
  bool PinData(const TablePinningInfo& tpi, pinning::HierarchyCategory category,
               CacheEntryRole role, size_t size,
               std::unique_ptr<PinnedEntry>* pinned) override;
  void UnPinData(std::unique_ptr<PinnedEntry>&& pinned) override;
  std::string ToString() const override;

  void AddCacheItemOwnerId(Cache::ItemOwnerId item_owner_id) override;
  void RemoveCacheItemOwnerId(Cache::ItemOwnerId item_owner_id) override;

  // Returns the total pinned memory usage
  size_t GetPinnedUsage() const override;

  // Returns the pinned memory usage for the owner id
  OwnerIdPinnedCountersForQuery GetOwnerIdPinnedUsageCounters(Cache::ItemOwnerId item_owner_id) const;

  size_t GetOwnerIdTotalPinnedUsage(Cache::ItemOwnerId item_owner_id) const;

 protected:
  // Updates the statistics with the new pinned information.
  size_t RecordPinned(const PinnedEntry& pinned_entry, bool pinned);

  // Checks whether the data can be pinned.
  virtual bool CheckPin(const TablePinningInfo& tpi, 
                        pinning::HierarchyCategory category,
                        CacheEntryRole role, 
                        size_t size,
                        size_t limit) const = 0;

 private:
   size_t GetOwnerIdTotalPinnedUsageNonLocking(Cache::ItemOwnerId item_owner_id) const;

 private:
  std::atomic<size_t> usage_;
  mutable std::atomic<size_t> attempts_counter_;
  std::atomic<size_t> pinned_counter_;
  std::atomic<size_t> active_counter_;

  // Total pinned usage is kept per the following triplet: [owner-id, level, role];
  // Counters are maintained per level rather than per level-0 / middle-level / last-level-with-data
  // Since the last level with data 
  // Assumptions:
  // 1. An ItemOwnerId is used by the user only afer it has been added => AddCacheItemOwnerId() has been called
  //    and terminated before any call to MayPin() / PinData() / UnPinData() referencing that ItemOwnerId.
  // 2. When an ItemOwnerId is removed, all of its pinned items will have been unpinned.
  // 3. ItemOwnerId-s are added and removed infrequently.
  // 4. Counters are retrieved infrequently (for debugging / log reporting).
  // Consequently, the counters_mutex is locked only when owner id-s are added or removed, and when 
  // retrieving the counters.
  std::unordered_map<Cache::ItemOwnerId, OwnerIdPinnedCounters> pinned_counters_;

  // Mutable so it may be locked when querying the counters (the object remains constant)
  mutable std::mutex counters_mutex_;
};

}  // namespace ROCKSDB_NAMESPACE
