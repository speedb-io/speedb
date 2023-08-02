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

#include "table/block_based/recording_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {

// The original RocksDB pinning policy
class DefaultPinningPolicy : public RecordingPinningPolicy {
 public:
  DefaultPinningPolicy();
  DefaultPinningPolicy(const BlockBasedTableOptions& bbto);

  DefaultPinningPolicy(const MetadataCacheOptions& mdco, bool pin_top,
                       bool pin_l0);

  static const char* kClassName() { return "DefaultPinningPolicy"; }
  const char* Name() const override { return kClassName(); }

 protected:
  bool CheckPin(const TablePinningOptions& tpo, uint8_t type, size_t /*size*/,
                size_t /*limit*/) const override;
  bool IsPinned(const TablePinningOptions& tpo, PinningTier pinning_tier,
                PinningTier fallback_pinning_tier) const;

 protected:
  const MetadataCacheOptions cache_options_;
  bool pin_top_level_index_and_filter_ = true;
  bool pin_l0_index_and_filter_ = false;
};
}  // namespace ROCKSDB_NAMESPACE
