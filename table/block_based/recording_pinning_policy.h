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
#include <vector>

#include "rocksdb/table_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {
// An abstract table pinning policy that records the pinned operations
class RecordingPinningPolicy : public TablePinningPolicy {
 public:
  RecordingPinningPolicy();

  bool MayPin(const TablePinningOptions& tpo, uint8_t type,
              size_t size) const override;
  bool PinData(const TablePinningOptions& tpo, uint8_t type, size_t size,
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
  void RecordPinned(int level, uint8_t type, size_t size, bool pinned);

  // Checks whether the data can be pinned.
  virtual bool CheckPin(const TablePinningOptions& tpo, uint8_t type,
                        size_t size, size_t limit) const = 0;

  std::atomic<size_t> usage_;
  mutable std::atomic<size_t> attempts_counter_;
  std::atomic<size_t> pinned_counter_;
  std::atomic<size_t> active_counter_;
  std::vector<std::atomic<uint64_t>> usage_by_level_;
  std::vector<std::atomic<uint64_t>> usage_by_type_;
};

}  // namespace ROCKSDB_NAMESPACE
