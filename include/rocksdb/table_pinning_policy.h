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

#include "rocksdb/customizable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct BlockBasedTableOptions;
struct ConfigOptions;

// Struct that contains information about the table being evaluated for pinning
struct TablePinningOptions {
  TablePinningOptions() = default;

  TablePinningOptions(int _level, bool _is_bottom, size_t _file_size,
                      size_t _max_file_size_for_l0_meta_pin)
      : level(_level),
        is_bottom(_is_bottom),
        file_size(_file_size),
        max_file_size_for_l0_meta_pin(_max_file_size_for_l0_meta_pin) {}
  int level = -1;
  bool is_bottom = false;
  size_t file_size = 0;
  size_t max_file_size_for_l0_meta_pin = 0;
};

// Struct containing information about an entry that has been pinned
struct PinnedEntry {
  PinnedEntry() {}
  PinnedEntry(int _level, uint8_t _type, size_t _size)
      : level(_level), type(_type), size(_size) {}

  int level = -1;
  uint8_t type = 0;
  size_t size = 0;
};

// TablePinningPolicy provides a configurable way to determine when blocks
// should be pinned in memory for the block based tables.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class TablePinningPolicy : public Customizable {
 public:
  static const uint8_t kTopLevel = 1;
  static const uint8_t kPartition = 2;
  static const uint8_t kIndex = 3;
  static const uint8_t kFilter = 4;
  static const uint8_t kDictionary = 5;
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
  virtual bool MayPin(const TablePinningOptions& tpo, uint8_t type,
                      size_t size) const = 0;

  // Attempts to pin the block in memory.
  // If successful, pinned returns the pinned block
  // Returns true and updates pinned on success and false if the data cannot be
  // pinned
  virtual bool PinData(const TablePinningOptions& tpo, uint8_t type,
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
  bool MayPin(const TablePinningOptions& tpo, uint8_t type,
              size_t size) const override {
    return target_->MayPin(tpo, type, size);
  }

  bool PinData(const TablePinningOptions& tpo, uint8_t type, size_t size,
               std::unique_ptr<PinnedEntry>* pinned) override {
    return target_->PinData(tpo, type, size, pinned);
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
