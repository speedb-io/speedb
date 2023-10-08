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

#include "rocksdb/advanced_cache.h"
#include "rocksdb/customizable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct BlockBasedTableOptions;
struct ConfigOptions;

enum class HierarchyCategory { TOP_LEVEL, PARTITION, OTHER };

// Struct that contains information about the table being evaluated for pinning
struct TablePinningOptions {
  TablePinningOptions() = default;

  TablePinningOptions(int _level, bool _is_last_level_with_data,
                      Cache::ItemOwnerId _item_owner_id, size_t _file_size, size_t _max_file_size_for_l0_meta_pin)
      : level(_level),
        is_last_level_with_data(_is_last_level_with_data),
        item_owner_id(_item_owner_id),
        file_size(_file_size),
        max_file_size_for_l0_meta_pin(_max_file_size_for_l0_meta_pin) {}
  int level = -1;
  bool is_last_level_with_data = false;
  Cache::ItemOwnerId item_owner_id = Cache::kUnknownItemOwnerId;
  size_t file_size = 0;
  size_t max_file_size_for_l0_meta_pin = 0;
};

// Struct containing information about an entry that has been pinned
struct PinnedEntry {
  PinnedEntry() = default;

  PinnedEntry(int _level, bool _is_last_level_with_data,
              HierarchyCategory _category, Cache::ItemOwnerId _item_owner_id,
              CacheEntryRole _role, size_t _size)
      : level(_level),
        is_last_level_with_data(_is_last_level_with_data),
        category(_category),
        item_owner_id(_item_owner_id),
        role(_role),
        size(_size) {}

  int level = -1;
  bool is_last_level_with_data = false;
  HierarchyCategory category = HierarchyCategory::OTHER;
  Cache::ItemOwnerId item_owner_id = Cache::kUnknownItemOwnerId;
  CacheEntryRole role = CacheEntryRole::kMisc;
  size_t size = 0U;
};

// TablePinningPolicy provides a configurable way to determine when blocks
// should be pinned in memory for the block based tables.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class TablePinningPolicy : public Customizable {
 public:
  static const char* Type() { return "TablePinningPolicy"; }

  // Creates/Returns a new TablePinningPolicy based in the input value
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<TablePinningPolicy>* policy);
  virtual ~TablePinningPolicy() = default;

  // Returns true if the block defined by type and size is a candidate for
  // pinning This method indicates that pinning might be possible, but does not
  // perform the pinning operation. Returns true if the data is a candidate for
  // pinning and false otherwise
  virtual bool MayPin(const TablePinningOptions& tpo,
                      HierarchyCategory category, CacheEntryRole role,
                      size_t size) const = 0;

  // Attempts to pin the block in memory.
  // If successful, pinned returns the pinned block
  // Returns true and updates pinned on success and false if the data cannot be
  // pinned
  virtual bool PinData(const TablePinningOptions& tpo,
                       HierarchyCategory category,
                       CacheEntryRole _role,
                       size_t size, std::unique_ptr<PinnedEntry>* pinned) = 0;

  // Releases and clears the pinned entry.
  virtual void UnPinData(std::unique_ptr<PinnedEntry>&& pinned) = 0;

  // Returns the amount of data currently pinned.
  virtual size_t GetPinnedUsage() const = 0;

  // Returns the info (e.g. statistics) associated with this policy.
  virtual std::string ToString() const = 0;
};

class TablePinningPolicyWrapper : public TablePinningPolicy {
 public:
  explicit TablePinningPolicyWrapper(
      const std::shared_ptr<TablePinningPolicy>& t)
      : target_(t) {}
  bool MayPin(const TablePinningOptions& tpo, HierarchyCategory category,
              CacheEntryRole role, size_t size) const override {
    return target_->MayPin(tpo, category, role, size);
  }

  bool PinData(const TablePinningOptions& tpo, HierarchyCategory category,
               CacheEntryRole role,
               size_t size, std::unique_ptr<PinnedEntry>* pinned) override {
    return target_->PinData(tpo, category, role, size, pinned);
  }

  void UnPinData(std::unique_ptr<PinnedEntry>&& pinned) override {
    target_->UnPinData(std::move(pinned));
  }

  size_t GetPinnedUsage() const override { return target_->GetPinnedUsage(); }

 protected:
  std::shared_ptr<TablePinningPolicy> target_;
};

TablePinningPolicy* NewDefaultPinningPolicy(const BlockBasedTableOptions& bbto);
}  // namespace ROCKSDB_NAMESPACE
