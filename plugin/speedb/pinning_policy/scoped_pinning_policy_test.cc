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

#include "plugin/speedb/pinning_policy/scoped_pinning_policy.h"

#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/table.h"
#include "rocksdb/table_pinning_policy.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {
// Tests related to Speedb's Scoped Pinning Policy.

class ScopedPinningPolicyTest : public testing::Test {
 public:
  ScopedPinningPolicy* GetScopedPolicy(
      const std::string id = ScopedPinningPolicy::kClassName()) {
    if (!pinning_policy_) {
      ConfigOptions options;
      options.ignore_unsupported_options = false;
      EXPECT_OK(
          TablePinningPolicy::CreateFromString(options, id, &pinning_policy_));
    }
    auto scoped = pinning_policy_->CheckedCast<ScopedPinningPolicy>();
    EXPECT_NE(scoped, nullptr);
    return scoped;
  }
  bool PinData(const TablePinningInfo& tpi, pinning::HierarchyCategory category,
               CacheEntryRole role, size_t size,
               std::vector<std::unique_ptr<PinnedEntry>>& entries) {
    std::unique_ptr<PinnedEntry> pinned_entry;
    if (pinning_policy_->PinData(tpi, category, role, size, &)) {
      EXPECT_NE(p.get(), nullptr);
      entries.emplace_back(std::move(p));
      return true;
    } else {
      return false;
    }
  }

 private:
  std::shared_ptr<TablePinningPolicy> pinning_policy_;
};

// XXXXXXXXXXXXXXXXXXXXXXxx
#if 0
TEST_F(ScopedPinningPolicyTest, GetOptions) {
  ConfigOptions cfg;
  cfg.ignore_unsupported_options = false;
  std::shared_ptr<TablePinningPolicy> policy;

  std::string id = std::string("id=") + ScopedPinningPolicy::kClassName();
  ASSERT_OK(TablePinningPolicy::CreateFromString(cfg, id, &policy));
  auto opts = policy->GetOptions<ScopedPinningOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->capacity, ScopedPinningOptions().capacity);
  ASSERT_EQ(opts->last_level_with_data_percent,
            ScopedPinningOptions().last_level_with_data_percent);
  ASSERT_EQ(opts->mid_percent, ScopedPinningOptions().mid_percent);
  ASSERT_TRUE(policy->IsInstanceOf(ScopedPinningPolicy::kClassName()));

  ASSERT_OK(TablePinningPolicy::CreateFromString(
      cfg,
      id + "; capacity=2048; last_level_with_data_percent=22; mid_percent=33",
      &policy));
  opts = policy->GetOptions<ScopedPinningOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->capacity, 2048);
  ASSERT_EQ(opts->last_level_with_data_percent, 22);
  ASSERT_EQ(opts->mid_percent, 33);
  ASSERT_TRUE(policy->IsInstanceOf(ScopedPinningPolicy::kClassName()));
}

TEST_F(ScopedPinningPolicyTest, GetManaged) {
  ConfigOptions cfg;
  cfg.ignore_unsupported_options = false;
  std::shared_ptr<TablePinningPolicy> policy;

  std::string id = std::string("id=") + ScopedPinningPolicy::kClassName();
  ASSERT_OK(TablePinningPolicy::CreateFromString(
      cfg,
      id + "; capacity=2048; last_level_with_data_percent=22; mid_percent=33",
      &policy));
  auto opts = policy->GetOptions<ScopedPinningOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->capacity, 2048);
  ASSERT_EQ(opts->last_level_with_data_percent, 22);
  ASSERT_EQ(opts->mid_percent, 33);
  ASSERT_TRUE(policy->IsInstanceOf(ScopedPinningPolicy::kClassName()));
  std::shared_ptr<TablePinningPolicy> copy;
  ASSERT_OK(TablePinningPolicy::CreateFromString(cfg, policy->GetId(), &copy));
  ASSERT_EQ(copy, policy);

  ASSERT_OK(TablePinningPolicy::CreateFromString(
      cfg,
      "id= " + policy->GetId() +
          "; capacity=4096; last_level_with_data_percent=11; mid_percent=44",
      &copy));
  ASSERT_EQ(copy, policy);
  opts = policy->GetOptions<ScopedPinningOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->capacity, 2048);
  ASSERT_EQ(opts->last_level_with_data_percent, 22);
  ASSERT_EQ(opts->mid_percent, 33);
}

TEST_F(ScopedPinningPolicyTest, TestLimits) {
  auto policy = GetScopedPolicy();
  auto opts = policy->GetOptions<ScopedPinningOptions>();
  ASSERT_NE(opts, nullptr);
  auto capacity = opts->capacity;
  size_t bottom = capacity * opts->last_level_with_data_percent / 100;
  size_t mid = capacity * opts->mid_percent / 100;

  TablePinningInfo l0(0, false, Cache::kUnkonwnItemOwnerId, 0, 0);  // Level 0
  TablePinningInfo lm(1, false, Cache::kUnkonwnItemOwnerId, 0, 0);  // Mid level
  TablePinningInfo lb(2, true, Cache::kUnkonwnItemOwnerId, 0, 0);   // Bottom level

  std::vector<std::unique_ptr<PinnedEntry>> pinned_entries;
  std::unique_ptr<PinnedEntry> pinned_entry;

  // Make sure we cannot pin more than capacity
  ASSERT_FALSE(policy->MayPin(l0, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity + 1));
  ASSERT_FALSE(policy->MayPin(lm, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity + 1));
  ASSERT_FALSE(policy->MayPin(lb, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity + 1));
  ASSERT_FALSE(
      policy->PinData(l0, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity + 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);
  ASSERT_FALSE(
      policy->PinData(lm, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity + 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);
  ASSERT_FALSE(
      policy->PinData(lb, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity + 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);

  // Mid and last-level-with-data levels cannot pin more than their limits
  ASSERT_FALSE(policy->MayPin(lm, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, mid + 1));
  ASSERT_FALSE(
      policy->PinData(lm, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, mid + 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);
  ASSERT_FALSE(policy->MayPin(lb, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, bottom + 1));
  ASSERT_FALSE(
      policy->PinData(lb, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, bottom + 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);

  ASSERT_TRUE(PinData(l0, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, 2, pinned_entries));
  ASSERT_FALSE(policy->MayPin(l0, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity - 1));
  ASSERT_FALSE(policy->MayPin(lm, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity - 1));
  ASSERT_FALSE(policy->MayPin(lb, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity - 1));
  ASSERT_FALSE(
      policy->PinData(l0, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity - 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);
  ASSERT_FALSE(
      policy->PinData(lm, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity - 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);
  ASSERT_FALSE(
      policy->PinData(lb, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, capacity - 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);
  ASSERT_FALSE(policy->MayPin(lm, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, mid - 1));
  ASSERT_FALSE(
      policy->PinData(lm, pinning::HierarchyCategory::OTHER, CacheEntryRole::kIndexBlock, mid - 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);
  ASSERT_FALSE(policy->MayPin(lb, pinning::HierarchyCategory::TOP_LEVEL, CacheEntryRole::kMisc, bottom - 1));
  ASSERT_FALSE(
      policy->PinData(lb, pinning::HierarchyCategory::TOP_LEVEL, CacheEntryRole::kMisc, bottom - 1, &pinned_entry));
  ASSERT_EQ(pinned_entry, nullptr);

  ASSERT_TRUE(
      PinData(lb, pinning::HierarchyCategory::TOP_LEVEL, CacheEntryRole::kMisc, bottom - 3, pinned_entries));
  ASSERT_EQ(policy->GetPinnedUsage(), bottom - 1);
  // ASSERT_EQ(policy->GetPinnedUsageByLevel(0), 2);
  // ASSERT_EQ(policy->GetPinnedUsageByLevel(lb.level), bottom - 3);
  // XXXXX ASSERT_EQ(policy->GetPinnedUsageByType(TablePinningPolicy::kIndex), 2);
  // XXXXX ASSERT_EQ(policy->GetPinnedUsageByType(TablePinningPolicy::kTopLevel),
  // XXXXX           bottom - 3);

  policy->UnPinData(pinned_entries.back());
  pinned_entries.pop_back();
  // ASSERT_EQ(policy->GetPinnedUsage(), 2);
  // ASSERT_EQ(policy->GetPinnedUsageByLevel(0), 2);
  // ASSERT_EQ(policy->GetPinnedUsageByLevel(lb.level), 0);
  // ASSERT_EQ(policy->GetPinnedUsageByType(TablePinningPolicy::kIndex), 2);
  // ASSERT_EQ(policy->GetPinnedUsageByType(TablePinningPolicy::kTopLevel), 0);
}

// XXXXXXXXXXXXXXXXXXXXXXXXXXXXX
#endif
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
