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
#include <vector>
#include <array>
#include <mutex>

#include "rocksdb/cache.h"
#include "rocksdb/table_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {
// An abstract table pinning policy that records the pinned operations
class RecordingPinningPolicy : public TablePinningPolicy {
 public:
  RecordingPinningPolicy();
  ~RecordingPinningPolicy();

  bool MayPin(const TablePinningInfo& tpi, HierarchyCategory category,
              CacheEntryRole role, size_t size) const override;
  bool PinData(const TablePinningInfo& tpi, HierarchyCategory category,
               CacheEntryRole role, size_t size,
               std::unique_ptr<PinnedEntry>* pinned) override;
  void UnPinData(std::unique_ptr<PinnedEntry>&& pinned) override;
  std::string ToString() const override;

  // Returns the total pinned memory usage
  size_t GetPinnedUsage() const override;

  // Returns the pinned memory usage for the input level
  size_t GetPinnedUsageByLevel(int level) const;

  // Returns the pinned memory usage for the input type
  size_t GetPinnedUsageByType(uint8_t type) const;

 protected:
  // Updates the statistics with the new pinned information.
  void RecordPinned(int level, HierarchyCategory category,
                    Cache::ItemOwnerId item_owner_id, CacheEntryRole role,
                    size_t size, bool pinned);

  // Checks whether the data can be pinned.
  virtual bool CheckPin(const TablePinningInfo& tpi, HierarchyCategory category,
                        CacheEntryRole role, size_t size,
                        size_t limit) const = 0;

  std::atomic<size_t> usage_;
  mutable std::atomic<size_t> attempts_counter_;
  std::atomic<size_t> pinned_counter_;
  std::atomic<size_t> active_counter_;

  // Total pinned usage is kept per the following triplet: [owner-id, level, role];
  // Counters are maintained per level rather than per level-0 / middle-level / last-level-with-data
  // Since the last level with data 
  using PerRolePinnedCounters = std::array<std::atomic<size_t>, kNumCacheEntryRoles>;
  using PerLevelAndRolePinnedCounters = std::vector<PerRolePinnedCounters>;
  std::unordered_map<Cache::ItemOwnerId, PerLevelAndRolePinnedCounters> pinned_counters;
};

}  // namespace ROCKSDB_NAMESPACE
