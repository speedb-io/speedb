// Copyright (C) 2022 Speedb Ltd. All rights reserved.
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

#include <atomic>
#include <vector>

#include "rocksdb/table_pinning_policy.h"
#include "table/block_based/recording_pinning_policy.h"

namespace ROCKSDB_NAMESPACE {
struct TablePinningOptions;
struct ScopedPinningOptions {
  static const char* kName() { return "ScopedPinningOptions"; }

  static constexpr uint32_t kDefaultLastLevelWithDataPercent = 10;
  static constexpr uint32_t kDefaultMidPercent = 70;

  // Limit to how much data should be pinned
  size_t capacity = 1024 * 1024 * 1024;  // 1GB

  // Percent of capacity at which not to pin last-leve-with-data data
  uint32_t last_level_with_data_percent = kDefaultLastLevelWithDataPercent;

  // Percent of capacity at which not to pin non-L0 data
  uint32_t mid_percent = kDefaultMidPercent;
};

//
class ScopedPinningPolicy : public RecordingPinningPolicy {
 public:
  ScopedPinningPolicy();
  ScopedPinningPolicy(const ScopedPinningOptions& options);

  static const char* kClassName() { return "speedb_scoped_pinning_policy"; }
  static const char* kNickName() { return "scoped"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }
  std::string GetId() const override;

  std::string GetPrintableOptions() const override;

 protected:
  bool CheckPin(const TablePinningOptions& tpo, uint8_t type, size_t size,
                size_t limit) const override;

 private:
  ScopedPinningOptions options_;
};
}  // namespace ROCKSDB_NAMESPACE
