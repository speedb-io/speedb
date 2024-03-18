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

#include "table/block_based/recording_pinning_policy.h"

#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

namespace {
class MyPinningPolicy : public RecordingPinningPolicy {
 public:
  MyPinningPolicy(bool& check_pin_reply): check_pin_reply_(check_pin_reply) {}

  static const char* kClassName() { return "speedb_test_pinnning_policy"; }
  static const char* kNickName() { return "speedb.TestPinningPolicy"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }
  // std::string GetId() const override;

 protected:
  bool CheckPin(const TablePinningInfo& /* tpi */,
                pinning::HierarchyCategory /* category */,
                CacheEntryRole /* role */, 
                size_t /* size */, 
                size_t /* limit */) const override {
    return check_pin_reply_;
  }
  
  private:
    bool& check_pin_reply_;
};
}

class RecordingPinningPolicyTest : public testing::Test {
};

TEST_F(RecordingPinningPolicyTest, TestPinningUtils) {
  ASSERT_EQ(LevelCategory::UNKNOWN_LEVEL,
            GetLevelCategory(kUnknownLevel, false));
  ASSERT_EQ(LevelCategory::LEVEL_0, GetLevelCategory(0, false));
  ASSERT_EQ(LevelCategory::MIDDLE_LEVEL, GetLevelCategory(1, false));
  ASSERT_EQ(LevelCategory::LAST_LEVEL_WITH_DATA, GetLevelCategory(1, true));
}

TEST_F(RecordingPinningPolicyTest, Dummy) {
  bool check_pin_reply = true;
  MyPinningPolicy policy(check_pin_reply);

  Cache::ItemOwnerId item_owner_id = 1U;
  ASSERT_EQ(0U, policy.GetOwnerIdTotalPinnedUsage(item_owner_id));

  TablePinningInfo tpi( 0 /* level*/,
                        false /* is_last_level_with_data */,
                        item_owner_id,
                        100000U /* file_size */,
                        10000U /*max_file_size_for_l0_meta_pin */);

  auto role = CacheEntryRole::kIndexBlock;
  std::unique_ptr<PinnedEntry> pinned_entry;
  auto pin_result = policy.PinData( tpi, 
                                    pinning::HierarchyCategory::OTHER,
                                    role,
                                    1000U /* size */,
                                    &pinned_entry);
  ASSERT_TRUE(pin_result);
  ASSERT_EQ(1000U, policy.GetOwnerIdTotalPinnedUsage(item_owner_id));

  auto pinned_counters = policy.GetOwnerIdPinnedUsageCounters(item_owner_id);
  auto level_idx = static_cast<uint64_t>(LevelCategory::LEVEL_0);
  auto role_idx = static_cast<uint64_t>(role);
  ASSERT_EQ(1000U, pinned_counters[level_idx][role_idx]);

  policy.UnPinData(std::move(pinned_entry));
  ASSERT_EQ(0U, policy.GetOwnerIdTotalPinnedUsage(item_owner_id));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
