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

#include "db/db_test_util.h"
#include "db/write_controller.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {

class GlobalWriteControllerTest : public DBTestBase {
 public:
  GlobalWriteControllerTest()
      : DBTestBase("global_wc_test", /*env_do_fsync=*/true) {}

  ~GlobalWriteControllerTest() { CloseAndDeleteDBs(); }

  void OpenDBsAndSetUp(int num_dbs, Options& options, bool add_wbm = false,
                       uint64_t buffer_size = 40_kb) {
    db_names_.clear();
    for (int i = 0; i < num_dbs; i++) {
      dbs_.push_back(nullptr);
      db_names_.push_back(
          test::PerThreadDBPath("db_shared_wc_db" + std::to_string(i)));
    }

    options.level0_slowdown_writes_trigger = 10;
    options.level0_stop_writes_trigger = 20;
    options.delayed_write_rate = 16_mb;
    options.use_dynamic_delay = true;
    options.write_controller.reset(new WriteController(
        options.use_dynamic_delay, options.delayed_write_rate));
    if (add_wbm) {
      options.write_buffer_manager.reset(new WriteBufferManager(
          buffer_size, {}, true /*allow_stall*/, false /*initiate_flushes*/,
          WriteBufferManager::FlushInitiationOptions(),
          WriteBufferManager::kDfltStartDelayPercentThreshold));
    }

    for (int i = 0; i < num_dbs; i++) {
      ASSERT_OK(DestroyDB(db_names_[i], options));
      ASSERT_OK(DB::Open(options, db_names_[i], &(dbs_[i])));
    }

    dbimpls_.clear();
    for (int i = 0; i < num_dbs; i++) {
      dbimpls_.push_back(static_cast_with_check<DBImpl>(dbs_[i]));
    }

    cfds_.clear();
    vstorages_.clear();
    for (int i = 0; i < num_dbs; i++) {
      ColumnFamilyData* cfd =
          static_cast<ColumnFamilyHandleImpl*>(dbs_[i]->DefaultColumnFamily())
              ->cfd();
      cfds_.push_back(cfd);
      vstorages_.push_back(cfd->current()->storage_info());
    }

    mutable_cf_options_ = MutableCFOptions(options);
    destroy_options_ = options;
  }

  void CloseAndDeleteDBs() {
    for (size_t i = 0; i < dbs_.size(); i++) {
      ASSERT_OK(dbs_[i]->Close());
      ASSERT_OK(DestroyDB(db_names_[i], destroy_options_));
      delete dbs_[i];
    }
  }

  void SetL0delayAndRecalcConditions(int db_idx, int l0_files) {
    vstorages_[db_idx]->set_l0_delay_trigger_count(l0_files);
    RecalculateWriteStallConditions(dbimpls_[db_idx], cfds_[db_idx],
                                    mutable_cf_options_);
  }

  uint64_t CalcWBMDelay(uint64_t max_write_rate, size_t quota,
                        size_t updated_memory_used,
                        uint16_t start_delay_percent) {
    auto usage_start_delay_threshold = (start_delay_percent * quota) / 100;
    double extra_used_memory =
        updated_memory_used - usage_start_delay_threshold;
    double max_used_memory = quota - usage_start_delay_threshold;

    uint64_t delay_factor = (extra_used_memory / max_used_memory) *
                            WriteBufferManager::kMaxDelayedWriteFactor;
    if (delay_factor < 1U) {
      delay_factor = 1U;
    }
    auto wbm_write_rate = max_write_rate;
    if (max_write_rate >= WriteController::kMinWriteRate) {
      // If user gives rate less than kMinWriteRate, don't adjust it.
      assert(delay_factor <= WriteBufferManager::kMaxDelayedWriteFactor);
      auto write_rate_factor =
          static_cast<double>(WriteBufferManager::kMaxDelayedWriteFactor -
                              delay_factor) /
          WriteBufferManager::kMaxDelayedWriteFactor;
      wbm_write_rate = max_write_rate * write_rate_factor;
      if (wbm_write_rate < WriteController::kMinWriteRate) {
        wbm_write_rate = WriteController::kMinWriteRate;
      }
    }
    return wbm_write_rate;
  }

  uint64_t CalcL0Delay(int l0_files, Options& options, uint64_t max_rate) {
    double l0_range = options.level0_stop_writes_trigger -
                      options.level0_slowdown_writes_trigger;
    auto extra_l0 = l0_files - options.level0_slowdown_writes_trigger;
    uint64_t rate = max_rate * ((l0_range - extra_l0) / l0_range);
    return rate;
  }

  Options destroy_options_;
  MutableCFOptions mutable_cf_options_;
  std::vector<std::string> db_names_;
  std::vector<DB*> dbs_;
  std::vector<DBImpl*> dbimpls_;
  std::vector<ColumnFamilyData*> cfds_;
  std::vector<VersionStorageInfo*> vstorages_;
};

// test GetMapMinRate()
// insert different delay requests into 2 dbs
TEST_F(GlobalWriteControllerTest, TestGetMinRate) {
  Options options = CurrentOptions();
  int num_dbs = 2;
  // one set of dbs with one Write Controller(WC)
  OpenDBsAndSetUp(num_dbs, options);

  // sets db0 to 16Mbs
  SetL0delayAndRecalcConditions(0 /*db_idx*/, 10 /*l0_files*/);

  ASSERT_TRUE(options.write_controller->delayed_write_rate() == 16_mb);
  ASSERT_TRUE(options.write_controller->TEST_GetMapMinRate() == 16_mb);

  // sets db1 to 8Mbs
  SetL0delayAndRecalcConditions(1 /*db_idx*/, 15 /*l0_files*/);

  ASSERT_TRUE(options.write_controller->delayed_write_rate() == 8_mb);
  ASSERT_TRUE(options.write_controller->TEST_GetMapMinRate() == 8_mb);

  // sets db0 to 8Mbs
  SetL0delayAndRecalcConditions(0 /*db_idx*/, 15 /*l0_files*/);
  ASSERT_TRUE(options.write_controller->delayed_write_rate() == 8_mb);
  ASSERT_TRUE(options.write_controller->TEST_GetMapMinRate() == 8_mb);

  // removes delay requirement from both dbs
  SetL0delayAndRecalcConditions(0 /*db_idx*/, 9 /*l0_files*/);
  SetL0delayAndRecalcConditions(1 /*db_idx*/, 9 /*l0_files*/);
  uint64_t max_rate = options.write_controller->max_delayed_write_rate();
  ASSERT_TRUE(options.write_controller->delayed_write_rate() == max_rate);
  ASSERT_TRUE(options.write_controller->TEST_GetMapMinRate() == max_rate);
  ASSERT_FALSE(options.write_controller->NeedsDelay());
}

// test scenario 0:
// make sure 2 dbs_ opened with the same write controller object also use it
TEST_F(GlobalWriteControllerTest, SharedWriteControllerAcrossDB) {
  Options options = CurrentOptions();
  int num_dbs = 2;

  OpenDBsAndSetUp(num_dbs, options);

  ASSERT_TRUE(dbimpls_[0]->write_controller() == options.write_controller);
  ASSERT_TRUE(dbimpls_[0]->write_controller() ==
              dbimpls_[1]->write_controller());
}

// test scenario 1:
// make sure 2 dbs opened with a different write controller dont use the same.
TEST_F(GlobalWriteControllerTest, NonSharedWriteControllerAcrossDB) {
  Options options = CurrentOptions();
  int num_dbs = 2;
  // one set of dbs with one Write Controller(WC)
  OpenDBsAndSetUp(num_dbs, options);

  // second db with a different WC
  Options options2 = CurrentOptions();
  DB* db2 = nullptr;
  std::string db2_name = test::PerThreadDBPath("db_shared_wc_db2");
  ASSERT_OK(DestroyDB(db2_name, options));
  ASSERT_OK(DB::Open(options2, db2_name, &db2));
  DBImpl* dbimpl2 = static_cast_with_check<DBImpl>(db2);

  ASSERT_FALSE(dbimpl2->write_controller() == options.write_controller);

  ASSERT_FALSE(dbimpls_[0]->write_controller() == dbimpl2->write_controller());

  // Clean up db2.
  ASSERT_OK(db2->Close());
  ASSERT_OK(DestroyDB(db2_name, options2));
  delete db2;
}

// test scenario 2:
// setting up 2 dbs, put one into delay and verify that the other is also
// delayed. then remove the delay condition and verify that they're not delayed.
TEST_F(GlobalWriteControllerTest, SharedWriteControllerAcrossDB2) {
  Options options = CurrentOptions();
  int num_dbs = 2;
  OpenDBsAndSetUp(num_dbs, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[i]));
  }

  SetL0delayAndRecalcConditions(0 /*db_idx*/, 10 /*l0_files*/);
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[i]));
  }

  SetL0delayAndRecalcConditions(0 /*db_idx*/, 5 /*l0_files*/);
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[i]));
  }

  SetL0delayAndRecalcConditions(1 /*db_idx*/, 15 /*l0_files*/);
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[i]));
  }

  SetL0delayAndRecalcConditions(0 /*db_idx*/, 20 /*l0_files*/);
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_TRUE(IsDbWriteStopped(dbimpls_[i]));
  }

  SetL0delayAndRecalcConditions(0 /*db_idx*/, 9 /*l0_files*/);
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[i]));
  }

  SetL0delayAndRecalcConditions(1 /*db_idx*/, 9 /*l0_files*/);
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[i]));
  }
}

// test scenario 3:
// setting up 2 dbs, put one into stop and verify that the other is also
// stopped. then remove the stop condition and verify that they're both
// proceeding with the writes.
TEST_F(GlobalWriteControllerTest, SharedWriteControllerAcrossDB3) {
  Options options = CurrentOptions();
  int num_dbs = 2;
  OpenDBsAndSetUp(num_dbs, options);

  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteController::WaitOnCV", [&](void*) {
        {
          InstrumentedMutexLock lock(&mutex);
          wait_count_db++;
          if (wait_count_db == num_dbs) {
            cv.Signal();
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[i]));
  }

  // put db0 into stop state. which means db1 is also in stop state.
  SetL0delayAndRecalcConditions(0 /*db_idx*/, 20 /*l0_files*/);
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_TRUE(IsDbWriteStopped(dbimpls_[i]));
  }

  // write to both dbs from 2 different threads.
  bool s = true;
  WriteOptions wo;

  std::function<void(DB*)> write_db = [&](DB* db) {
    Status tmp = db->Put(wo, "foo", "bar");
    InstrumentedMutexLock lock(&mutex);
    s = s && tmp.ok();
  };

  for (int i = 0; i < num_dbs; i++) {
    threads.emplace_back(write_db, dbs_[i]);
  }
  // verify they are waiting on the controller cv (WriteController::WaitOnCV)
  // use a call back with counter to make sure both threads entered the cv wait.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != num_dbs) {
      cv.Wait();
    }
  }
  // verify keys are not yet in the db as data has not yet being flushed.
  ReadOptions ropt;
  std::string value;
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_TRUE(dbs_[i]->Get(ropt, "foo", &value).IsNotFound());
  }

  // remove stop condition and verify write.
  SetL0delayAndRecalcConditions(0 /*db_idx*/, 0 /*l0_files*/);
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_FALSE(IsDbWriteStopped(dbimpls_[i]));
  }

  for (auto& t : threads) {
    t.join();
  }
  ASSERT_TRUE(s);

  // get the keys.
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs_[i]->Get(ropt, "foo", &value));
    ASSERT_EQ(value, "bar");
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// make sure 2 dbs_ opened with the same WBM object also use it
TEST_F(GlobalWriteControllerTest, GlobalAndWBMBasic) {
  Options options = CurrentOptions();
  int num_dbs = 2;

  OpenDBsAndSetUp(num_dbs, options, true);

  ASSERT_TRUE(dbimpls_[0]->write_buffer_manager() ==
              options.write_buffer_manager.get());
  ASSERT_TRUE(dbimpls_[0]->write_buffer_manager() ==
              dbimpls_[1]->write_buffer_manager());

  DBImpl* default_db = static_cast_with_check<DBImpl>(db_);
  ASSERT_FALSE(dbimpls_[0]->write_buffer_manager() ==
               default_db->write_buffer_manager());
}

// setup 2 dbs using the same WC and WBM
// increase memory usage on WBM and verify that theres a delay req
TEST_F(GlobalWriteControllerTest, GlobalAndWBMSetupDelay) {
  Options options = CurrentOptions();
  // memory quota is 40k.
  options.arena_block_size =
      4_kb;  // this is the smallest unit of memory change
  int num_dbs = 2;
  OpenDBsAndSetUp(num_dbs, options, true);
  WriteOptions wo;

  // verify that theres no delay
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[i]));
  }

  std::string value(4_kb, 'x');
  // insert into db1 just into the threshold - buffer size is 40k and
  // start_delay_percent is 70.
  // need to allocate more than 0.7 * 40k = 28k
  // since theres 2k memtable allocation, plus key sizes, the 6th insert should
  // call for the 7th allocation and cross the 28k limit.
  // memtable will not be flushed yet since:
  // 1. initiate_flushes = false
  // 2. memory_used < 7/8 of memory quota (35840 bytes)
  // 3. memtable isn't full (64MB default)
  for (int i = 0; i < 6; i++) {
    ASSERT_OK(dbs_[0]->Put(wo, Key(i), value));
  }
  ASSERT_GT(options.write_buffer_manager->memory_usage(), 28_kb);
  ASSERT_LT(options.write_buffer_manager->memory_usage(), 32_kb);

  // verify that both dbs are in a delay
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[i]));
  }

  // clear the memory usage
  ASSERT_OK(dbs_[0]->Flush(FlushOptions()));
  // there should only be 2k per memtable left
  ASSERT_TRUE(options.write_buffer_manager->memory_usage() < 5_kb);

  // verify that theres no delay
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[i]));
  }
}

// set delay requirements from WBM and verify the rate can be calculated and
// its the rate that the WC receives.
TEST_F(GlobalWriteControllerTest, GlobalAndWBMCalcDelay) {
  Options options = CurrentOptions();
  int num_dbs = 2;
  // memory quota is 40k.
  OpenDBsAndSetUp(num_dbs, options, true);
  WriteBufferManager* wbm = options.write_buffer_manager.get();
  WriteController* wc = options.write_controller.get();
  // initial default value
  ASSERT_EQ(wc->delayed_write_rate(), 16_mb);

  // reset memory usage to get an exact change
  wbm->TEST_reset_memory_usage();
  size_t mem_to_set = 28_kb;
  wbm->ReserveMem(mem_to_set);

  // verify that both dbs are in a delay
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[i]));
  }

  // calculating delay is done as follows:
  // max_rate * (100 - factor) / 100
  // factor = (extra_used_memory / max_used_memory) * kMaxDelayedWriteFactor
  // factor min value is 1
  // kMaxDelayedWriteFactor = 100;
  uint64_t max_rate = wc->max_delayed_write_rate();
  size_t mem_quota = wbm->buffer_size();
  auto start_delay_percent = wbm->get_start_delay_percent();
  // since factor is 0 -> sanitized to 1
  uint64_t wbm_delay_req =
      CalcWBMDelay(max_rate, mem_quota, mem_to_set, start_delay_percent);
  ASSERT_EQ(wc->delayed_write_rate(), wbm_delay_req);

  // there are 12kb of memory from start of delay to max delay. reach halfway
  wbm->ReserveMem(6_kb);
  // rate should be half since we're decreasing linearly
  ASSERT_EQ(wc->delayed_write_rate(), max_rate / 2);

  // total memory used == 28 + 6. reserve just below the last step to reach max
  // delay. there are 100 steps (kMaxDelayedWriteFactor) from 28 to 40 kb.
  //
  // the last step is from (99 / 100) * (40 - 28 kb) until (40 - 28 kb)
  // from 12165.12 until 12288. so need to reserve 12288 - 6kb - 1
  mem_to_set = 12288 - 6_kb - 1;
  wbm->ReserveMem(mem_to_set);
  ASSERT_EQ(wc->delayed_write_rate(),
            static_cast<uint64_t>(max_rate * (1.0 / 100)));

  // reserving more memory than quota should also reset delay since we're now in
  // a stop state which will induce flushes and stop during the write phase.
  wbm->ReserveMem(1);
  // delay request should be deleted from rate map.
  ASSERT_EQ(wc->max_delayed_write_rate(), wc->TEST_GetMapMinRate());
  ASSERT_EQ(wc->max_delayed_write_rate(), wc->delayed_write_rate());

  // verify that both dbs are not in a delay
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[i]));
  }
}

// setup competing delay requests from both the dbs and the wbm and verify the
// wc always sets the smallest rate.
TEST_F(GlobalWriteControllerTest, GlobalAndWBMCompetingRequests) {
  Options options = CurrentOptions();
  int num_dbs = 2;
  // memory quota is 40k.
  OpenDBsAndSetUp(num_dbs, options, true);
  WriteBufferManager* wbm = options.write_buffer_manager.get();
  WriteController* wc = options.write_controller.get();
  uint64_t max_rate = wc->max_delayed_write_rate();

  // reset memory usage to get an exact change
  wbm->TEST_reset_memory_usage();
  // reserve to be halfway through [slowdown, stop] range.
  size_t mem_to_set = 34_kb;
  wbm->ReserveMem(mem_to_set);

  // verify that both dbs are in a delay
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[i]));
  }

  // rate should be half since we're decreasing linearly
  ASSERT_EQ(wc->delayed_write_rate(), max_rate / 2);
  // l0 slowdown is 10 and stop is 20. set delay requirement below the wbm
  auto db0_l0_files = 12;
  SetL0delayAndRecalcConditions(0 /*db_idx*/, db0_l0_files);
  ASSERT_EQ(wc->TEST_total_delayed_count(), 2);
  ASSERT_EQ(wc->delayed_write_rate(), max_rate / 2);

  // setup a bigger delay from db1
  auto db1_l0_files = 16;
  SetL0delayAndRecalcConditions(1 /*db_idx*/, db1_l0_files);
  ASSERT_EQ(wc->TEST_total_delayed_count(), 3);
  auto db1_l0_delay = CalcL0Delay(db1_l0_files, options, max_rate);
  ASSERT_EQ(wc->delayed_write_rate(), db1_l0_delay);

  // setup a bigger delay from wbm (currently at 34k) need factor > 60
  wbm->ReserveMem(4_kb);
  ASSERT_EQ(wc->TEST_total_delayed_count(), 3);
  // calculating in both ways to make sure they match
  auto start_delay_percent = wbm->get_start_delay_percent();
  uint64_t wbm_delay_req = CalcWBMDelay(max_rate, wbm->buffer_size(),
                                        mem_to_set + 4_kb, start_delay_percent);
  ASSERT_EQ(wc->delayed_write_rate(), wbm_delay_req);
  // we're 10kb from 12 kb range. so factor is (10/12)*100 which is 83 (decimal
  // truncated). final rate is max_rate * (max_factor - 83 / max_factor)
  double max_factor = WriteBufferManager::kMaxDelayedWriteFactor;
  uint64_t factor = (10.0 / 12) * max_factor;
  ASSERT_EQ(
      static_cast<uint64_t>(max_rate * ((max_factor - factor) / max_factor)),
      wbm_delay_req);

  // remove all delay requests and make sure they clean up
  wbm->TEST_reset_memory_usage();
  wbm->ReserveMem(12_kb);
  ASSERT_EQ(wc->TEST_total_delayed_count(), 2);
  ASSERT_EQ(wc->delayed_write_rate(), db1_l0_delay);

  SetL0delayAndRecalcConditions(1 /*db_idx*/, 5 /*l0_files*/);
  ASSERT_EQ(wc->TEST_total_delayed_count(), 1);
  auto db0_l0_delay = CalcL0Delay(db0_l0_files, options, max_rate);
  ASSERT_EQ(wc->delayed_write_rate(), db0_l0_delay);

  SetL0delayAndRecalcConditions(0 /*db_idx*/, 5 /*l0_files*/);
  ASSERT_EQ(wc->TEST_total_delayed_count(), 0);
}

// stress the system with many threads doing writes and various sized values.
// until stress test tool can handle more than 1 db
TEST_F(GlobalWriteControllerTest, GlobalAndWBMStressTest) {
  Options options = CurrentOptions();
  int num_dbs = 8;
  auto memory_quota = 10_mb;
  OpenDBsAndSetUp(num_dbs, options, true, memory_quota);
  const int num_threads = 16;
  const int memory_to_ingest = 200_mb;
  const int mul = 64;
  const int num_keys =
      memory_to_ingest / ((1_kb + (mul * num_threads / 2)) * num_threads);
  // total estimated ingest is:
  // (1 kb + mul * (num_threads/2)) * num_keys * num_threads

  std::vector<port::Thread> threads;
  WriteOptions wo;

  std::function<void(DB*, int)> write_db = [&](DB* db, int seed) {
    auto var = mul * seed;
    std::string value(1_kb + var, 'x');
    for (int i = 0; i < num_keys; i++) {
      Status s = db->Put(wo, Key(i), value);
      if (!s.ok()) {
        fprintf(stderr, "Failed to insert. status: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
  };

  for (int i = 0; i < num_threads; i++) {
    auto dbidx = i % num_dbs;
    threads.emplace_back(write_db, dbs_[dbidx], i);
  }

  for (auto& t : threads) {
    t.join();
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
