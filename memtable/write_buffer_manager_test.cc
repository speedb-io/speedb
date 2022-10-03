//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include "rocksdb/cache.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
class WriteBufferManagerTest : public testing::Test {};

#ifndef ROCKSDB_LITE
const size_t kSizeDummyEntry = 256 * 1024;

namespace {
void BeginAndFree(WriteBufferManager& wbf, size_t size) {
  wbf.FreeMemBegin(size);
  wbf.FreeMem(size);
}

void ScheduleBeginAndFreeMem(WriteBufferManager& wbf, size_t size) {
  wbf.ScheduleFreeMem(size);
  BeginAndFree(wbf, size);
}
}  // namespace
TEST_F(WriteBufferManagerTest, ShouldFlush) {
  // A write buffer manager of size 10MB
  std::unique_ptr<WriteBufferManager> wbf(
      new WriteBufferManager(10 * 1024 * 1024));

  wbf->ReserveMem(8 * 1024 * 1024);
  ASSERT_FALSE(wbf->ShouldFlush());
  // 90% of the hard limit will hit the condition
  wbf->ReserveMem(1 * 1024 * 1024);
  ASSERT_TRUE(wbf->ShouldFlush());
  // Scheduling for freeing will release the condition
  wbf->ScheduleFreeMem(1 * 1024 * 1024);
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(2 * 1024 * 1024);
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(4 * 1024 * 1024);
  // 11MB total, 6MB mutable. hard limit still hit
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(2 * 1024 * 1024);
  // 11MB total, 4MB mutable. hard limit stills but won't flush because more
  // than half data is already being flushed.
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(4 * 1024 * 1024);
  // 15 MB total, 8MB mutable.
  ASSERT_TRUE(wbf->ShouldFlush());

  BeginAndFree(*wbf, 7 * 1024 * 1024);
  // 8MB total, 8MB mutable.
  ASSERT_FALSE(wbf->ShouldFlush());

  // change size: 8M limit, 7M mutable limit
  wbf->SetBufferSize(8 * 1024 * 1024);
  // 8MB total, 8MB mutable.
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(2 * 1024 * 1024);
  // 8MB total, 6MB mutable.
  ASSERT_TRUE(wbf->ShouldFlush());

  BeginAndFree(*wbf, 2 * 1024 * 1024);
  // 6MB total, 6MB mutable.
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(1 * 1024 * 1024);
  // 7MB total, 7MB mutable.
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(1 * 1024 * 1024);
  // 8MB total, 8MB mutable.
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(1 * 1024 * 1024);
  BeginAndFree(*wbf, 1 * 1024 * 1024);
  // 7MB total, 7MB mutable.
  ASSERT_FALSE(wbf->ShouldFlush());
}

TEST_F(WriteBufferManagerTest, CacheCost) {
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions co;
  // 1GB cache
  co.capacity = 1024 * 1024 * 1024;
  co.num_shard_bits = 4;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = NewLRUCache(co);
  // A write buffer manager of size 50MB
  std::unique_ptr<WriteBufferManager> wbf(
      new WriteBufferManager(50 * 1024 * 1024, cache));

  // Allocate 333KB will allocate 512KB, memory_used_ = 333KB
  wbf->ReserveMem(333 * 1024);
  // 2 dummy entries are added for size 333 KB
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 2 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 2 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 2 * 256 * 1024 + kMetaDataChargeOverhead);

  // Allocate another 512KB, memory_used_ = 845KB
  wbf->ReserveMem(512 * 1024);
  // 2 more dummy entries are added for size 512 KB
  // since ceil((memory_used_ - dummy_entries_in_cache_usage) % kSizeDummyEntry)
  // = 2
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 4 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 4 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 4 * 256 * 1024 + kMetaDataChargeOverhead);

  // Allocate another 10MB, memory_used_ = 11085KB
  wbf->ReserveMem(10 * 1024 * 1024);
  // 40 more entries are added for size 10 * 1024 * 1024 KB
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);

  // Free 1MB, memory_used_ = 10061KB
  // It will not cause any change in cache cost
  // since memory_used_ > dummy_entries_in_cache_usage * (3/4)
  ScheduleBeginAndFreeMem(*wbf, 1 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);
  ASSERT_FALSE(wbf->ShouldFlush());

  // Allocate another 41MB, memory_used_ = 52045KB
  wbf->ReserveMem(41 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 204 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 204 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(),
            204 * 256 * 1024 + kMetaDataChargeOverhead);
  ASSERT_TRUE(wbf->ShouldFlush());

  ASSERT_TRUE(wbf->ShouldFlush());

  // Schedule free 20MB, memory_used_ = 52045KB
  // It will not cause any change in memory_used and cache cost
  wbf->ScheduleFreeMem(20 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 204 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 204 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(),
            204 * 256 * 1024 + kMetaDataChargeOverhead);
  // Still need flush as the hard limit hits
  ASSERT_TRUE(wbf->ShouldFlush());

  // Free 20MB, memory_used_ = 31565KB
  // It will releae 80 dummy entries from cache since
  // since memory_used_ < dummy_entries_in_cache_usage * (3/4)
  // and floor((dummy_entries_in_cache_usage - memory_used_) % kSizeDummyEntry)
  // = 80
  BeginAndFree(*wbf, 20 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 124 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 124 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(),
            124 * 256 * 1024 + kMetaDataChargeOverhead);

  ASSERT_FALSE(wbf->ShouldFlush());

  // Free 16KB, memory_used_ = 31549KB
  // It will not release any dummy entry since memory_used_ >=
  // dummy_entries_in_cache_usage * (3/4)
  ScheduleBeginAndFreeMem(*wbf, 16 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 124 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 124 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(),
            124 * 256 * 1024 + kMetaDataChargeOverhead);

  // Free 20MB, memory_used_ = 11069KB
  // It will releae 80 dummy entries from cache
  // since memory_used_ < dummy_entries_in_cache_usage * (3/4)
  // and floor((dummy_entries_in_cache_usage - memory_used_) % kSizeDummyEntry)
  // = 80
  ScheduleBeginAndFreeMem(*wbf, 20 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);

  // Free 1MB, memory_used_ = 10045KB
  // It will not cause any change in cache cost
  // since memory_used_ > dummy_entries_in_cache_usage * (3/4)
  ScheduleBeginAndFreeMem(*wbf, 1 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);

  // Reserve 512KB, memory_used_ = 10557KB
  // It will not casue any change in cache cost
  // since memory_used_ > dummy_entries_in_cache_usage * (3/4)
  // which reflects the benefit of saving dummy entry insertion on memory
  // reservation after delay decrease
  wbf->ReserveMem(512 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);

  // Destory write buffer manger should free everything
  wbf.reset();
  ASSERT_EQ(cache->GetPinnedUsage(), 0);
}

TEST_F(WriteBufferManagerTest, NoCapCacheCost) {
  constexpr std::size_t kMetaDataChargeOverhead = 10000;
  // 1GB cache
  std::shared_ptr<Cache> cache = NewLRUCache(1024 * 1024 * 1024, 4);
  // A write buffer manager of size 256MB
  std::unique_ptr<WriteBufferManager> wbf(new WriteBufferManager(0, cache));

  // Allocate 10MB,  memory_used_ = 10240KB
  // It will allocate 40 dummy entries
  wbf->ReserveMem(10 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 40 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 40 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 40 * 256 * 1024 + kMetaDataChargeOverhead);

  ASSERT_FALSE(wbf->ShouldFlush());

  // Free 9MB,  memory_used_ = 1024KB
  // It will free 36 dummy entries
  ScheduleBeginAndFreeMem(*wbf, 9 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 4 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 4 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 4 * 256 * 1024 + kMetaDataChargeOverhead);

  // Free 160KB gradually, memory_used_ = 864KB
  // It will not cause any change
  // since memory_used_ > dummy_entries_in_cache_usage * 3/4
  for (int i = 0; i < 40; i++) {
    ScheduleBeginAndFreeMem(*wbf, 4 * 1024);
  }
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 4 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 4 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 4 * 256 * 1024 + kMetaDataChargeOverhead);
}

TEST_F(WriteBufferManagerTest, CacheFull) {
  constexpr std::size_t kMetaDataChargeOverhead = 20000;

  // 12MB cache size with strict capacity
  LRUCacheOptions lo;
  lo.capacity = 12 * 1024 * 1024;
  lo.num_shard_bits = 0;
  lo.strict_capacity_limit = true;
  std::shared_ptr<Cache> cache = NewLRUCache(lo);
  std::unique_ptr<WriteBufferManager> wbf(new WriteBufferManager(0, cache));

  // Allocate 10MB, memory_used_ = 10240KB
  wbf->ReserveMem(10 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 40 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 40 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            40 * kSizeDummyEntry + kMetaDataChargeOverhead);

  // Allocate 10MB, memory_used_ = 20480KB
  // Some dummy entry insertion will fail due to full cache
  wbf->ReserveMem(10 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 40 * kSizeDummyEntry);
  ASSERT_LE(cache->GetPinnedUsage(), 12 * 1024 * 1024);
  ASSERT_LT(wbf->dummy_entries_in_cache_usage(), 80 * kSizeDummyEntry);

  // Free 15MB after encoutering cache full, memory_used_ = 5120KB
  ScheduleBeginAndFreeMem(*wbf, 15 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 20 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 20 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            20 * kSizeDummyEntry + kMetaDataChargeOverhead);

  // Reserve 15MB, creating cache full again, memory_used_ = 20480KB
  wbf->ReserveMem(15 * 1024 * 1024);
  ASSERT_LE(cache->GetPinnedUsage(), 12 * 1024 * 1024);
  ASSERT_LT(wbf->dummy_entries_in_cache_usage(), 80 * kSizeDummyEntry);

  // Increase capacity so next insert will fully succeed
  cache->SetCapacity(40 * 1024 * 1024);

  // Allocate 10MB, memory_used_ = 30720KB
  wbf->ReserveMem(10 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 120 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 120 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            120 * kSizeDummyEntry + kMetaDataChargeOverhead);

  // Gradually release 20 MB
  // It ended up sequentially releasing 32, 24, 18 dummy entries when
  // memory_used_ decreases to 22528KB, 16384KB, 11776KB.
  // In total, it releases 74 dummy entries
  for (int i = 0; i < 40; i++) {
    ScheduleBeginAndFreeMem(*wbf, 512 * 1024);
  }

  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 46 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 46 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            46 * kSizeDummyEntry + kMetaDataChargeOverhead);
}

#endif  // ROCKSDB_LITE

#define VALIDATE_USAGE_STATE(memory_change_size, expected_state,   \
                             expected_factor)                      \
  ValidateUsageState(__LINE__, memory_change_size, expected_state, \
                     expected_factor)

class WriteBufferManagerTestWithParams
    : public WriteBufferManagerTest,
      public ::testing::WithParamInterface<std::tuple<bool, bool, bool>> {
 public:
  void SetUp() override {
    wbm_enabled_ = std::get<0>(GetParam());
    cost_cache_ = std::get<1>(GetParam());
    allow_delays_and_stalls_ = std::get<2>(GetParam());
  }

  bool wbm_enabled_;
  bool cost_cache_;
  bool allow_delays_and_stalls_;
};
// Test that the write buffer manager sends the expected usage notifications
TEST_P(WriteBufferManagerTestWithParams, UsageNotifications) {
  constexpr size_t kQuota = 10 * 1000;
  constexpr size_t kStepSize = kQuota / 100;
  constexpr size_t kDelayThreshold =
      WriteBufferManager::kStartDelayPercentThreshold * kQuota / 100;
  constexpr size_t kMaxUsed = kQuota - kDelayThreshold;

  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);

  std::unique_ptr<WriteBufferManager> wbf;

  auto wbm_quota = (wbm_enabled_ ? kQuota : 0U);
  if (cost_cache_) {
    wbf.reset(
        new WriteBufferManager(wbm_quota, cache, allow_delays_and_stalls_));
  } else {
    wbf.reset(
        new WriteBufferManager(wbm_quota, nullptr, allow_delays_and_stalls_));
  }
  ASSERT_EQ(wbf->enabled(), wbm_enabled_);

  size_t expected_usage = 0U;
  auto ExpectedDelayFactor = [&](uint64_t extra_used) {
    return (extra_used * WriteBufferManager::kMaxDelayedWriteFactor) / kMaxUsed;
  };

  auto ValidateUsageState = [&](unsigned long line, size_t memory_change_size,
                                WriteBufferManager::UsageState expected_state,
                                uint64_t expected_factor) {
    const auto location_str =
        "write_buffer_manager_test.cc:" + std::to_string(line) + "\n";

    if (wbm_enabled_ || cost_cache_) {
      expected_usage += memory_change_size;
    }
    ASSERT_EQ(wbf->memory_usage(), expected_usage) << location_str;

    if (wbm_enabled_ && allow_delays_and_stalls_) {
      auto [actual_state, actual_delay_factor] = wbf->GetUsageStateInfo();
      ASSERT_EQ(actual_state, expected_state) << location_str;
      ASSERT_EQ(actual_delay_factor, expected_factor) << location_str;
    }
  };

  // Initial state
  VALIDATE_USAGE_STATE(0, WriteBufferManager::UsageState::kNone,
                       WriteBufferManager::kNoneDelayedWriteFactor);

  auto FreeMem = [&, this](size_t mem) {
    wbf->ScheduleFreeMem(mem);
    wbf->FreeMemBegin(mem);
    wbf->FreeMem(mem);
  };

  // Jump straight to quota
  wbf->ReserveMem(kQuota);
  VALIDATE_USAGE_STATE(kQuota, WriteBufferManager::UsageState::kStop,
                       WriteBufferManager::kStopDelayedWriteFactor);

  // And back to 0 again
  FreeMem(kQuota);
  VALIDATE_USAGE_STATE(-kQuota, WriteBufferManager::UsageState::kNone,
                       WriteBufferManager::kNoneDelayedWriteFactor);

  // Small reservations, below soft limit
  wbf->ReserveMem(1000);
  VALIDATE_USAGE_STATE(1000, WriteBufferManager::UsageState::kNone,
                       WriteBufferManager::kNoneDelayedWriteFactor);

  wbf->ReserveMem(2000);
  VALIDATE_USAGE_STATE(2000, WriteBufferManager::UsageState::kNone,
                       WriteBufferManager::kNoneDelayedWriteFactor);

  FreeMem(3000);
  VALIDATE_USAGE_STATE(-3000, WriteBufferManager::UsageState::kNone,
                       WriteBufferManager::kNoneDelayedWriteFactor);

  // 0 => soft limit
  wbf->ReserveMem(kDelayThreshold);
  VALIDATE_USAGE_STATE(kDelayThreshold, WriteBufferManager::UsageState::kDelay,
                       1U);

  // A bit more, but still within the same "step" => same delay factor
  wbf->ReserveMem(kStepSize - 1);
  VALIDATE_USAGE_STATE(kStepSize - 1, WriteBufferManager::UsageState::kDelay,
                       1U);

  // Cross the step => Delay factor updated
  wbf->ReserveMem(1);
  VALIDATE_USAGE_STATE(1, WriteBufferManager::UsageState::kDelay,
                       ExpectedDelayFactor(kStepSize));

  // Free all => None
  FreeMem(expected_usage);
  VALIDATE_USAGE_STATE(-expected_usage, WriteBufferManager::UsageState::kNone,
                       WriteBufferManager::kNoneDelayedWriteFactor);

  // None -> Stop (usage == quota)
  wbf->ReserveMem(kQuota);
  VALIDATE_USAGE_STATE(kQuota, WriteBufferManager::UsageState::kStop,
                       WriteBufferManager::kMaxDelayedWriteFactor);

  // Increasing the quota, usage as is => Now in the none
  wbf->SetBufferSize(wbm_quota * 2);
  VALIDATE_USAGE_STATE(0, WriteBufferManager::UsageState::kNone,
                       WriteBufferManager::kNoneDelayedWriteFactor);

  // Restoring the quota
  wbf->SetBufferSize(wbm_quota);
  VALIDATE_USAGE_STATE(0, WriteBufferManager::UsageState::kStop,
                       WriteBufferManager::kMaxDelayedWriteFactor);

  // 1 byte below quota => Delay with max factor
  FreeMem(1);
  VALIDATE_USAGE_STATE(-1, WriteBufferManager::UsageState::kDelay,
                       ExpectedDelayFactor(kMaxUsed - 1));

  // An entire step below => delay factor updated
  FreeMem(kStepSize);
  VALIDATE_USAGE_STATE(-kStepSize, WriteBufferManager::UsageState::kDelay,
                       ExpectedDelayFactor(kMaxUsed - 1 - kStepSize));

  // Again in the top "step"
  wbf->ReserveMem(1);
  VALIDATE_USAGE_STATE(1, WriteBufferManager::UsageState::kDelay,
                       ExpectedDelayFactor(kMaxUsed - kStepSize));

  // And back to 0 to wrap it up
  FreeMem(expected_usage);
  VALIDATE_USAGE_STATE(-expected_usage, WriteBufferManager::UsageState::kNone,
                       WriteBufferManager::kNoneDelayedWriteFactor);
}

INSTANTIATE_TEST_CASE_P(WriteBufferManagerTestWithParams,
                        WriteBufferManagerTestWithParams,
                        ::testing::Combine(testing::Bool(), testing::Bool(),
                                           testing::Bool()));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
