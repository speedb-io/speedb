//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <string>

#include "rocksdb/advanced_cache.h"
#include "rocksdb/cache.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
class WriteBufferManagerTest : public testing::Test {};

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
  std::unique_ptr<WriteBufferManager> wbf(new WriteBufferManager(
      10 * 1024 * 1024, {} /* cache */, WriteBufferManager::kDfltAllowStall,
      false /* initiate_flushes */));

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

class ChargeWriteBufferTest : public testing::Test {};

TEST_F(ChargeWriteBufferTest, Basic) {
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions co;
  // 1GB cache
  co.capacity = 1024 * 1024 * 1024;
  co.num_shard_bits = 4;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = NewLRUCache(co);
  // A write buffer manager of size 50MB
  std::unique_ptr<WriteBufferManager> wbf(new WriteBufferManager(
      50 * 1024 * 1024, cache, WriteBufferManager::kDfltAllowStall,
      false /* initiate_flushes */));

  // Allocate 333KB will allocate 512KB, memory_used_ = 333KB
  wbf->ReserveMem(333 * 1024);
  // 2 dummy entries are added for size 333 KB
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 2 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 2 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 2 * 256 * 1024 + kMetaDataChargeOverhead);

  // Allocate another 512KB, memory_used_ = 845KB
  wbf->ReserveMem(512 * 1024);
  // 2 more dummy entries are added for size 512 KB
  // since ceil((memory_used_ - dummy_entries_in_cache_usage) %
  // kSizeDummyEntry) = 2
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
  // and floor((dummy_entries_in_cache_usage - memory_used_) %
  // kSizeDummyEntry) = 80
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
  // and floor((dummy_entries_in_cache_usage - memory_used_) %
  // kSizeDummyEntry) = 80
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

  // Destroy write buffer manger should free everything
  wbf.reset();
  ASSERT_EQ(cache->GetPinnedUsage(), 0);
}

TEST_F(ChargeWriteBufferTest, BasicWithNoBufferSizeLimit) {
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

TEST_F(ChargeWriteBufferTest, BasicWithCacheFull) {
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
    allow_stall_ = std::get<2>(GetParam());
  }

  bool wbm_enabled_;
  bool cost_cache_;
  bool allow_stall_;
};

// ==========================================================================
#define CALL_WRAPPER(func) \
  func;                    \
  ASSERT_FALSE(HasFailure());

// #1: Quota (size_t). 0 == WBM disabled
// #2: Cost to cache (Boolean)
class WriteBufferManagerFlushInitiationTest
    : public WriteBufferManagerTest,
      public ::testing::WithParamInterface<std::tuple<size_t, bool, bool>> {
 public:
  void SetUp() override {
    quota_ = std::get<0>(GetParam());
    cost_cache_ = std::get<1>(GetParam());
    allow_stall_ = std::get<2>(GetParam());

    wbm_enabled_ = (quota_ > 0U);
    cache_ = NewLRUCache(4 * 1024 * 1024, 2);
    max_num_parallel_flushes_ =
        WriteBufferManager::FlushInitiationOptions().max_num_parallel_flushes;

    CreateWbm();
    SetupAndEnableTestPoints();

    actual_num_cbs_ = 0U;
    expected_num_cbs_ = 0U;
    validation_num_ = 0U;
    expected_num_flushes_to_initiate_ = 0U;
    expected_num_running_flushes_ = 0U;
  }

  void TearDown() override {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_TRUE(expected_cb_initiators_.empty());
    ASSERT_TRUE(expected_cb_min_size_to_flush_.empty());
    ASSERT_TRUE(flush_cb_results_.empty());

    initiators_.clear();
  }

  bool IsWbmDisabled() const { return (wbm_enabled_ == false); }

  void CreateWbm() {
    auto wbm_quota = (wbm_enabled_ ? quota_ : 0U);
    WriteBufferManager::FlushInitiationOptions initiation_options;
    initiation_options.max_num_parallel_flushes = max_num_parallel_flushes_;

    ASSERT_GT(max_num_parallel_flushes_, 0U);
    flush_step_size_ = quota_ / max_num_parallel_flushes_;

    if (cost_cache_) {
      wbm_.reset(new WriteBufferManager(wbm_quota, cache_, allow_stall_, true,
                                        initiation_options));
    } else {
      wbm_.reset(new WriteBufferManager(wbm_quota, nullptr, allow_stall_, true,
                                        initiation_options));
    }
    ASSERT_EQ(wbm_->enabled(), wbm_enabled_);
    ASSERT_TRUE(wbm_->IsInitiatingFlushes());
  }

  uint64_t CreateInitiator() {
    auto initiator = std::make_unique<uint64_t>(++next_initiator_id_);
    auto initiator_id = *initiator;
    initiators_.push_back(std::move(initiator));
    return initiator_id;
  }

  void RegisterInitiator(uint64_t initiator_id) {
    auto initiator = FindInitiator(initiator_id);
    ASSERT_NE(initiator, nullptr);
    if (initiator != nullptr) {
      auto cb =
          std::bind(&WriteBufferManagerFlushInitiationTest::FlushRequestCb,
                    this, std::placeholders::_1, initiator);
      wbm_->RegisterFlushInitiator(initiator, cb);
    }
  }

  uint64_t CreateAndRegisterInitiator() {
    auto initiator_id = CreateInitiator();
    RegisterInitiator(initiator_id);
    return initiator_id;
  }

  std::optional<uint64_t> FindInitiatorIdx(uint64_t initiator_id) {
    for (auto i = 0U; i < initiators_.size(); ++i) {
      if (*initiators_[i] == initiator_id) {
        return i;
      }
    }

    return {};
  }

  uint64_t* FindInitiator(uint64_t initiator_id) {
    auto initiator_idx = FindInitiatorIdx(initiator_id);
    if (initiator_idx.has_value()) {
      return initiators_[initiator_idx.value()].get();
    } else {
      ADD_FAILURE();
      return nullptr;
    }
  }

  void DeregisterInitiator(uint64_t initiator_id) {
    auto initiator_idx = FindInitiatorIdx(initiator_id);
    ASSERT_TRUE(initiator_idx.has_value());

    if (initiator_idx.has_value()) {
      wbm_->DeregisterFlushInitiator(initiators_[initiator_idx.value()].get());
      initiators_.erase(initiators_.begin() + initiator_idx.value());
    }
  }

  struct ExpectedCbInfo {
    uint64_t initiator_id;
    size_t min_size_to_flush;
    bool flush_cb_result;
  };

  void AddExpectedCbsInfos(const std::vector<ExpectedCbInfo>& cbs_infos) {
    ASSERT_TRUE(expected_cb_initiators_.empty());
    ASSERT_TRUE(expected_cb_min_size_to_flush_.empty());
    ASSERT_TRUE(flush_cb_results_.empty());

    if (IsWbmDisabled()) {
      return;
    }

    for (const auto& cb_info : cbs_infos) {
      auto initiator = FindInitiator(cb_info.initiator_id);
      ASSERT_NE(initiator, nullptr);
      expected_cb_initiators_.push_back(initiator);

      expected_cb_min_size_to_flush_.push_back(cb_info.min_size_to_flush);
      flush_cb_results_.push_back(cb_info.flush_cb_result);
    }
    actual_num_cbs_ = 0U;
    expected_num_cbs_ = cbs_infos.size();

    ++validation_num_;
    std::string test_point_name_suffix = std::to_string(validation_num_);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
        {{"DoneInitiationsAttemptTestPointCb::ExpectedNumAttempts:" +
              test_point_name_suffix,
          "ValidateState::WaitUntilValidtionPossible:" +
              test_point_name_suffix}});
  }

  // Flush initiaion callback registered with the WBM
  bool FlushRequestCb(size_t min_size_to_flush, void* initiator) {
    EXPECT_TRUE(wbm_enabled_);

    ++actual_num_cbs_;

    if (expected_cb_min_size_to_flush_.empty() == false) {
      EXPECT_EQ(expected_cb_min_size_to_flush_[0], min_size_to_flush);
      expected_cb_min_size_to_flush_.erase(
          expected_cb_min_size_to_flush_.begin());
    } else {
      EXPECT_FALSE(expected_cb_min_size_to_flush_.empty());
    }

    if (expected_cb_initiators_.empty() == false) {
      EXPECT_EQ(expected_cb_initiators_[0], initiator);
      expected_cb_initiators_.erase(expected_cb_initiators_.begin());
    } else {
      EXPECT_FALSE(expected_cb_initiators_.empty());
    }

    if (flush_cb_results_.empty() == false) {
      bool result = flush_cb_results_[0];
      flush_cb_results_.erase(flush_cb_results_.begin());
      return result;
    } else {
      EXPECT_FALSE(flush_cb_results_.empty());
      // Arbitrarily return true as we must return a bool to compile
      return true;
    }
  };

  // Sync Test Point callback called when the flush initiation thread
  // completes initating all flushes and resumes waiting for the condition
  // variable to be signalled again
  void DoneInitiationsAttemptTestPointCb(void* /*  arg */) {
    if (actual_num_cbs_ == expected_num_cbs_) {
      auto sync_point_name =
          "DoneInitiationsAttemptTestPointCb::ExpectedNumAttempts:" +
          std::to_string(validation_num_);
      TEST_SYNC_POINT(sync_point_name);
    }
  }

  void SetupAndEnableTestPoints() {
    if (IsWbmDisabled()) {
      return;
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WriteBufferManager::InitiateFlushesThread::DoneInitiationsAttempt",
        [&](void* arg) { DoneInitiationsAttemptTestPointCb(arg); });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  }

  void ValidateState(bool wait_on_sync_point) {
    if (wbm_enabled_ && wait_on_sync_point) {
      auto sync_point_name = "ValidateState::WaitUntilValidtionPossible:" +
                             std::to_string(validation_num_);
      TEST_SYNC_POINT(sync_point_name);
    }

    ASSERT_EQ(wbm_->TEST_GetNumFlushesToInitiate(),
              expected_num_flushes_to_initiate_);
    ASSERT_EQ(wbm_->TEST_GetNumRunningFlushes(), expected_num_running_flushes_);

    ASSERT_TRUE(expected_cb_initiators_.empty())
        << "Num entries:" << expected_cb_initiators_.size();
    ASSERT_TRUE(expected_cb_min_size_to_flush_.empty())
        << "Num entries:" << expected_cb_min_size_to_flush_.size();
    ASSERT_TRUE(flush_cb_results_.empty())
        << "Num entries:" << flush_cb_results_.size();
  }

  void EndFlush(bool wbm_initiated, size_t released_size,
                bool wait_on_sync_point = false) {
    wbm_->FreeMem(released_size);
    wbm_->FlushEnded(wbm_initiated /* wbm_initiated */);
    DecNumRunningFlushes();
    ValidateState(wait_on_sync_point);
  }

  void StartAndEndFlush(bool wbm_initiated, size_t released_size) {
    wbm_->ScheduleFreeMem(released_size);
    wbm_->FreeMemBegin(released_size);

    // "Run" the flush to completion & release the memory
    wbm_->FlushStarted(wbm_initiated /* wbm_initiated */);
    if ((wbm_initiated == false) && wbm_enabled_) {
      ++expected_num_running_flushes_;
    }
    EndFlush(wbm_initiated, released_size);
  }

  void IncNumRunningFlushes() {
    if (wbm_enabled_) {
      ++expected_num_running_flushes_;
    }
  }

  void DecNumRunningFlushes() {
    if (wbm_enabled_) {
      --expected_num_running_flushes_;
    }
  }

  void IncNumFlushesToInitiate() {
    if (wbm_enabled_) {
      ++expected_num_flushes_to_initiate_;
    }
  }

  void DecNumFlushesToInitiate() {
    if (wbm_enabled_) {
      --expected_num_flushes_to_initiate_;
    }
  }

 protected:
  size_t CalcExpectedMinSizeToFlush() {
    return std::min<size_t>(quota_ / (2 * max_num_parallel_flushes_),
                            64 * (1 << 20));
  }

 protected:
  std::unique_ptr<WriteBufferManager> wbm_;

  size_t quota_ = 0U;
  bool wbm_enabled_;
  bool cost_cache_;
  std::shared_ptr<Cache> cache_;
  bool allow_stall_ = false;
  size_t max_num_parallel_flushes_;
  size_t flush_step_size_ = 0U;

  std::vector<std::unique_ptr<uint64_t>> initiators_;
  uint64_t next_initiator_id_ = 0U;
  std::vector<void*> expected_cb_initiators_;
  std::vector<size_t> expected_cb_min_size_to_flush_;
  std::vector<bool> flush_cb_results_;
  size_t actual_num_cbs_ = 0;
  size_t expected_num_cbs_ = 0U;
  size_t expected_num_flushes_to_initiate_ = 0U;
  size_t expected_num_running_flushes_ = 0U;
  size_t validation_num_ = 0U;
};

TEST_P(WriteBufferManagerFlushInitiationTest, Basic) {
  // Register a single initiator
  auto initiator_id = CreateAndRegisterInitiator();

  CALL_WRAPPER(AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                                     true /* flush_cb_result */}}));

  // Reach the 1st step => expecting a single flush to be initiated
  wbm_->ReserveMem(flush_step_size_);
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));

  // "Run" the flush to completion & release the memory
  CALL_WRAPPER(StartAndEndFlush(true, flush_step_size_));

  DeregisterInitiator(initiator_id);
}

TEST_P(WriteBufferManagerFlushInitiationTest, NonWbmInitiatedFlush) {
  // Register a single initiator
  auto initiator_id = CreateAndRegisterInitiator();

  wbm_->FlushStarted(false /* wbm_initiated */);
  IncNumRunningFlushes();

  // Reach the 1st step => No need to initiate a flush (one is already
  // running)
  wbm_->ReserveMem(flush_step_size_);
  CALL_WRAPPER(ValidateState(false));

  CALL_WRAPPER(AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                                     true /* flush_cb_result */}}));

  // End the non-wbm flush without releasing memory, just for testing purposes
  // Expecting a wbm-initiated flush request since we are still over the step
  wbm_->FlushEnded(false /* wbm_initiated */);
  CALL_WRAPPER(ValidateState(true));

  // "Run" the wbm-initiated flush to completion & release the memory
  CALL_WRAPPER(StartAndEndFlush(true, flush_step_size_));

  DeregisterInitiator(initiator_id);
}

TEST_P(WriteBufferManagerFlushInitiationTest, MaxNumParallelFlushes) {
  // Replace the WBM with a new WBM that is configured with our max num of
  // parallel flushes
  max_num_parallel_flushes_ = 3U;
  ASSERT_NE(max_num_parallel_flushes_,
            wbm_->GetFlushInitiationOptions().max_num_parallel_flushes);
  CreateWbm();
  ASSERT_EQ(wbm_->GetFlushInitiationOptions().max_num_parallel_flushes,
            max_num_parallel_flushes_);

  // Register a single initiator
  auto initiator_id = CreateAndRegisterInitiator();

  // Start 3 (max) number of non-wbm flushes
  for (auto i = 0U; i < max_num_parallel_flushes_; ++i) {
    wbm_->FlushStarted(false /* wbm_initiated */);
    IncNumRunningFlushes();
  }

  // Reserve memory to allow for up to 3 (max) wbm-initiated flushes
  // However, 3 (max) are already running => no wbm-initaited flush expected
  wbm_->ReserveMem(max_num_parallel_flushes_ * flush_step_size_);
  CALL_WRAPPER(ValidateState(false));

  // Start another (total of 4 > max) non-wbm flush
  wbm_->ReserveMem(2 * flush_step_size_);

  wbm_->ScheduleFreeMem(flush_step_size_);
  wbm_->FreeMemBegin(flush_step_size_);
  wbm_->FlushStarted(false /* wbm_initiated */);
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(false));

  // End one of the non-wbm flushes 3 (max) still running, and usage requires
  // max flushes
  CALL_WRAPPER(EndFlush(false /* wbm_initiated */, flush_step_size_));

  // End another one of the non-wbm flushes => 2 (< max) running =>
  // Expecting one wbm-initiated
  CALL_WRAPPER(AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                                     true /* flush_cb_result */}}));
  // Increasing since expecteing wbm to initiate it
  IncNumRunningFlushes();
  wbm_->ScheduleFreeMem(flush_step_size_);
  wbm_->FreeMemBegin(flush_step_size_);
  CALL_WRAPPER(EndFlush(false /* wbm_initiated */, flush_step_size_,
                        true /* wait_on_sync_point */));

  wbm_->ReserveMem(2 * flush_step_size_);
  CALL_WRAPPER(ValidateState(false));

  // End a wbm-initiated flushes => 2 (< max) running => Expecting one
  // wbm-initiated
  CALL_WRAPPER(AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                                     true /* flush_cb_result */}}));
  // Increasing since expecteing wbm to initiate it
  IncNumRunningFlushes();
  wbm_->ScheduleFreeMem(flush_step_size_);
  wbm_->FreeMemBegin(flush_step_size_);
  CALL_WRAPPER(EndFlush(true /* wbm_initiated */, flush_step_size_,
                        true /* wait_on_sync_point */));

  DeregisterInitiator(initiator_id);
}

TEST_P(WriteBufferManagerFlushInitiationTest, JumpToQuota) {
  // Register a single initiator
  auto initiator_id = CreateAndRegisterInitiator();

  CALL_WRAPPER(AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                                     true /* flush_cb_result */}}));

  // Reach the 1st step => expecting a single flush to be initiated
  wbm_->ReserveMem(quota_);
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));

  // "Run" the flush to completion & release the memory
  CALL_WRAPPER(StartAndEndFlush(true, quota_));

  DeregisterInitiator(initiator_id);
}

TEST_P(WriteBufferManagerFlushInitiationTest,
       FailureToStartFlushWhenRequested) {
  // Register a single initiator
  auto initiator_id = CreateAndRegisterInitiator();

  // Setup two cb-s to fail to start the flush (flush_cb_result == false)
  // First with CalcExpectedMinSizeToFlush() size, Second with 0
  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                            false /* flush_cb_result */},
                           {initiator_id, 0U, false /* flush_cb_result */}}));

  // Reach the 1st step => expecting the 2 requests set up above
  wbm_->ReserveMem(flush_step_size_);
  IncNumFlushesToInitiate();
  CALL_WRAPPER(ValidateState(true));

  // Setup another two identical cb-s
  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                            false /* flush_cb_result */},
                           {initiator_id, 0U, false /* flush_cb_result */}}));

  // Reserve a bit more, but still within the same step. This will initiate
  // the next 2 request set up just above
  wbm_->TEST_WakeupFlushInitiationThread();
  CALL_WRAPPER(ValidateState(true));

  // Now, allow the second request to succeed
  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                            false /* flush_cb_result */},
                           {initiator_id, 0U, true /* flush_cb_result */}}));

  // Reserve a bit more, but still within the same step. This will initiate
  // the next 2 request set up just above
  wbm_->TEST_WakeupFlushInitiationThread();
  DecNumFlushesToInitiate();
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));

  DeregisterInitiator(initiator_id);
}

// TODO - Update the test - Currently fails
TEST_P(WriteBufferManagerFlushInitiationTest, DISABLED_FlushInitiationSteps) {
  // Too much (useless) effort to adapt to the disabled case so just skipping
  if (IsWbmDisabled()) {
    return;
  }
  auto initiator_id = CreateAndRegisterInitiator();

  // Increase the usage gradually in half-steps, each time expecting another
  // flush to be initiated
  for (auto i = 0U; i < max_num_parallel_flushes_; ++i) {
    wbm_->ReserveMem(flush_step_size_ / 2);
    CALL_WRAPPER(ValidateState(true));

    CALL_WRAPPER(
        AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                              true /* flush_cb_result */}}));
    IncNumRunningFlushes();
    wbm_->ReserveMem(flush_step_size_ / 2);
    CALL_WRAPPER(ValidateState(true));
  }
  ASSERT_EQ(wbm_->memory_usage(), quota_);
  ASSERT_EQ(wbm_->TEST_GetNumRunningFlushes(), max_num_parallel_flushes_);

  // Increase the usage over the quota. Not expecting any initiation activity
  wbm_->ReserveMem(flush_step_size_ / 2);
  wbm_->ReserveMem(flush_step_size_ / 2);
  CALL_WRAPPER(ValidateState(false));

  // Start all of the WBM flushes + some more that are NOT WBM flushes.
  // No new flush should initiate
  auto wbm_initiated = true;
  size_t num_non_wbm_running_flushes = 0U;
  for (auto i = 0U; i < 2 * max_num_parallel_flushes_; ++i) {
    wbm_->FlushStarted(wbm_initiated);
    if (wbm_initiated == false) {
      IncNumRunningFlushes();
      ++num_non_wbm_running_flushes;
    }
    wbm_initiated = !wbm_initiated;
  }
  ASSERT_EQ(expected_num_running_flushes_ - num_non_wbm_running_flushes,
            max_num_parallel_flushes_);
  CALL_WRAPPER(ValidateState(false));

  // Release flushes + memory so that we are at the quota with max num
  // of parallel flushes
  while (expected_num_running_flushes_ > max_num_parallel_flushes_) {
    EndFlush(wbm_initiated, 0U /* released_size */);
    wbm_initiated = !wbm_initiated;
  }
  wbm_->FreeMem(flush_step_size_);
  ASSERT_EQ(wbm_->memory_usage(), quota_);
  ASSERT_EQ(wbm_->TEST_GetNumRunningFlushes(), max_num_parallel_flushes_);
  CALL_WRAPPER(ValidateState(false));

  // Decrease just below the current flush step size
  wbm_->FreeMem(1U);

  while (wbm_->memory_usage() >= flush_step_size_) {
    EndFlush(true, 0U /* released_size */);
    CALL_WRAPPER(ValidateState(false));

    CALL_WRAPPER(
        AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                              true /* flush_cb_result */}}));
    IncNumRunningFlushes();
    EndFlush(false, 0U /* released_size */, true /* wait_on_sync_point */);

    wbm_->FreeMem(flush_step_size_);
  }
  ASSERT_EQ(wbm_->memory_usage(), flush_step_size_ - 1);
  ASSERT_EQ(wbm_->TEST_GetNumRunningFlushes(), 1U);

  // End the last remaining flush and release all used memory
  EndFlush(true, flush_step_size_ - 1 /* released_size */);
  ASSERT_EQ(wbm_->memory_usage(), 0U);
  ASSERT_EQ(wbm_->TEST_GetNumRunningFlushes(), 0U);

  DeregisterInitiator(initiator_id);
}

TEST_P(WriteBufferManagerFlushInitiationTest, RegisteringLate) {
  // Reach the 1st step, but no registered initiators
  wbm_->ReserveMem(flush_step_size_);
  IncNumFlushesToInitiate();
  CALL_WRAPPER(ValidateState(false));

  // Register an initiator and expect it to receive the initiation request
  auto initiator_id = CreateInitiator();
  CALL_WRAPPER(AddExpectedCbsInfos({{initiator_id, CalcExpectedMinSizeToFlush(),
                                     true /* flush_cb_result */}}));
  RegisterInitiator(initiator_id);
  DecNumFlushesToInitiate();
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));

  // "Run" the flush to completion & release the memory
  CALL_WRAPPER(StartAndEndFlush(true, flush_step_size_));

  DeregisterInitiator(initiator_id);
}

TEST_P(WriteBufferManagerFlushInitiationTest, Deregistering) {
  // Register a single initiator
  auto initiator_id1 = CreateAndRegisterInitiator();

  // initiator1 fails to initiate
  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id1, CalcExpectedMinSizeToFlush(),
                            false /* flush_cb_result */},
                           {initiator_id1, 0U, false /* flush_cb_result */}}));

  // Reach the 1st step => expecting a single flush to be initiated
  wbm_->ReserveMem(flush_step_size_);
  IncNumFlushesToInitiate();
  CALL_WRAPPER(ValidateState(true));

  // Deregisters and comes initiator2
  DeregisterInitiator(initiator_id1);
  auto initiator_id2 = CreateInitiator();

  // Set initiator2 to initiate the flush
  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id2, CalcExpectedMinSizeToFlush(),
                            true /* flush_cb_result */}}));
  RegisterInitiator(initiator_id2);

  DecNumFlushesToInitiate();
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));

  // "Run" the flush to completion & release the memory
  CALL_WRAPPER(StartAndEndFlush(true, flush_step_size_));

  DeregisterInitiator(initiator_id2);
}

TEST_P(WriteBufferManagerFlushInitiationTest, TwoInitiatorsBasic) {
  // Register two initiators
  auto initiator_id1 = CreateAndRegisterInitiator();
  auto initiator_id2 = CreateAndRegisterInitiator();

  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id1, CalcExpectedMinSizeToFlush(),
                            true /* flush_cb_result */}}));

  // Expect the 1st request to reach initiator1
  wbm_->ReserveMem(flush_step_size_);
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));

  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id2, CalcExpectedMinSizeToFlush(),
                            true /* flush_cb_result */}}));

  // Expect the 2nd request to reach initiator2
  wbm_->ReserveMem(flush_step_size_);
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));

  // "Run" the flush of initiator1 to completion & release the memory
  CALL_WRAPPER(StartAndEndFlush(true, flush_step_size_));

  // "Run" the flush of initiator2 to completion & release the memory
  CALL_WRAPPER(StartAndEndFlush(true, flush_step_size_));

  DeregisterInitiator(initiator_id2);
  DeregisterInitiator(initiator_id1);
}

TEST_P(WriteBufferManagerFlushInitiationTest,
       TwoInitiatorsFirstFailsToInitiate) {
  // Register two initiators
  auto initiator_id1 = CreateAndRegisterInitiator();
  auto initiator_id2 = CreateAndRegisterInitiator();

  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id1, CalcExpectedMinSizeToFlush(),
                            false /* flush_cb_result */},
                           {initiator_id2, CalcExpectedMinSizeToFlush(),
                            false /* flush_cb_result */},
                           {initiator_id1, 0U, false /* flush_cb_result */},
                           {initiator_id2, 0U, true /* flush_cb_result */}}));

  // Expect the 1st request to reach initiator2
  wbm_->ReserveMem(flush_step_size_);
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));

  // "Run" the flush of initiator1 to completion & release the memory
  CALL_WRAPPER(StartAndEndFlush(true, flush_step_size_));

  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id1, CalcExpectedMinSizeToFlush(),
                            true /* flush_cb_result */}}));

  // Expect the 2nd request to reach initiator1
  wbm_->ReserveMem(flush_step_size_);
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));

  // "Run" the flush of initiator2 to completion & release the memory
  CALL_WRAPPER(StartAndEndFlush(true, flush_step_size_));

  DeregisterInitiator(initiator_id2);
  DeregisterInitiator(initiator_id1);
}

TEST_P(WriteBufferManagerFlushInitiationTest,
       TwoInitiatorsDeregisteringWhileBeingNextToFlush) {
  // Register two initiators
  auto initiator_id1 = CreateAndRegisterInitiator();
  auto initiator_id2 = CreateAndRegisterInitiator();

  // Initiator1 initiates, initiator2 is next
  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id1, CalcExpectedMinSizeToFlush(),
                            true /* flush_cb_result */}}));

  wbm_->ReserveMem(flush_step_size_);
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));
  if (wbm_enabled_) {
    ASSERT_EQ(wbm_->TEST_GetNextCandidateInitiatorIdx(), 1U);
  }

  // Initiator2 will be deregistered => prepare another initiation for
  // initiator1
  CALL_WRAPPER(
      AddExpectedCbsInfos({{initiator_id1, CalcExpectedMinSizeToFlush(),
                            true /* flush_cb_result */}}));

  DeregisterInitiator(initiator_id2);
  ASSERT_EQ(wbm_->TEST_GetNextCandidateInitiatorIdx(), 0U);

  wbm_->ReserveMem(flush_step_size_);
  IncNumRunningFlushes();
  CALL_WRAPPER(ValidateState(true));
  ASSERT_EQ(wbm_->TEST_GetNextCandidateInitiatorIdx(), 0U);

  // "Run" both flushes to completion & release the memory
  for (auto i = 0U; i < 2; ++i) {
    CALL_WRAPPER(StartAndEndFlush(true, flush_step_size_));
  }

  DeregisterInitiator(initiator_id1);
}

INSTANTIATE_TEST_CASE_P(WriteBufferManagerTestWithParams,
                        WriteBufferManagerTestWithParams,
                        ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool()));

// Run the flush initiation tests in all combinations of:
// 1. WBM Enabled (buffer size > 0) / WBM Disabled (0 buffer size)
// 2. With and without costing to cache
// 3. Allow / Disallow delays and stalls
INSTANTIATE_TEST_CASE_P(WriteBufferManagerFlushInitiationTest,
                        WriteBufferManagerFlushInitiationTest,
                        ::testing::Combine(::testing::Values(10 * 1000, 0),
                                           ::testing::Bool(),
                                           ::testing::Bool()));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
