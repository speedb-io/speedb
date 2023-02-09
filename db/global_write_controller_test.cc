//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "db/write_controller.h"

namespace ROCKSDB_NAMESPACE {

// The param is whether dynamic_delay is used or not
class GlobalWriteControllerTest : public DBTestBase {
 public:
  GlobalWriteControllerTest()
      : DBTestBase("global_wc_test", /*env_do_fsync=*/true) {}

  ~GlobalWriteControllerTest() { CloseAndDeleteDBs(); }

  void OpenDBsAndSetWC(int num_dbs, Options& options) {
    db_names_.clear();
    for (int i = 0; i < num_dbs; i++) {
      dbs_.push_back(nullptr);
      db_names_.push_back(
          test::PerThreadDBPath("db_shared_wc_db" + std::to_string(i)));
    }

    options.level0_slowdown_writes_trigger = 10;
    options.level0_stop_writes_trigger = 20;
    options.delayed_write_rate = 16 * MB;
    options.use_dynamic_delay = true;
    options.write_controller.reset(new WriteController(
        options.use_dynamic_delay, options.delayed_write_rate));

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

  uint64_t const MB = 1024 * 1024;
  Options destroy_options_;
  MutableCFOptions mutable_cf_options_;
  std::vector<std::string> db_names_;
  std::vector<DB*> dbs_;
  std::vector<DBImpl*> dbimpls_;
  std::vector<ColumnFamilyData*> cfds_;
  std::vector<VersionStorageInfo*> vstorages_;
};

// test GetMinRate()
// insert different delay requests into 2 dbs
TEST_F(GlobalWriteControllerTest, TestGetMinRate) {
  Options options = CurrentOptions();
  int num_dbs = 3;
  // one set of dbs with one Write Controller(WC)
  OpenDBsAndSetWC(num_dbs, options);

  // sets db0 to 16Mbs
  SetL0delayAndRecalcConditions(0 /*db_idx*/, 10 /*l0_files*/);

  ASSERT_TRUE(options.write_controller->delayed_write_rate() == 16 * MB);
  ASSERT_TRUE(options.write_controller->GetMinRate() == 16 * MB);

  // sets db1 to 8Mbs
  SetL0delayAndRecalcConditions(1 /*db_idx*/, 15 /*l0_files*/);

  ASSERT_TRUE(options.write_controller->delayed_write_rate() == 8 * MB);
  ASSERT_TRUE(options.write_controller->GetMinRate() == 8 * MB);

  // sets db0 to 8Mbs
  SetL0delayAndRecalcConditions(0 /*db_idx*/, 15 /*l0_files*/);
  ASSERT_TRUE(options.write_controller->delayed_write_rate() == 8 * MB);
  ASSERT_TRUE(options.write_controller->GetMinRate() == 8 * MB);

  // sets db0 to 8Mbs
  SetL0delayAndRecalcConditions(0 /*db_idx*/, 9 /*l0_files*/);
  SetL0delayAndRecalcConditions(1 /*db_idx*/, 9 /*l0_files*/);
  uint64_t max_rate = options.write_controller->max_delayed_write_rate();
  ASSERT_TRUE(options.write_controller->delayed_write_rate() == max_rate);
  ASSERT_TRUE(options.write_controller->GetMinRate() == max_rate);
  ASSERT_FALSE(options.write_controller->NeedsDelay());
}

// test scenario 0:
// make sure 2 dbs_ opened with the same write controller object also use it
TEST_F(GlobalWriteControllerTest, SharedWriteControllerAcrossDB) {
  Options options = CurrentOptions();
  int num_dbs = 2;

  OpenDBsAndSetWC(num_dbs, options);

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
  OpenDBsAndSetWC(num_dbs, options);

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
// setting up 2 dbs, put one into delay and check that the other is also
// delayed. then remove the delay condition and check that they're not delayed.
TEST_F(GlobalWriteControllerTest, SharedWriteControllerAcrossDB2) {
  Options options = CurrentOptions();
  int num_dbs = 2;
  OpenDBsAndSetWC(num_dbs, options);

  ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[0]));
  ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[1]));

  SetL0delayAndRecalcConditions(0 /*db_idx*/, 10 /*l0_files*/);
  ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[0]));
  ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[1]));

  SetL0delayAndRecalcConditions(0 /*db_idx*/, 5 /*l0_files*/);
  ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[0]));
  ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[1]));

  SetL0delayAndRecalcConditions(1 /*db_idx*/, 15 /*l0_files*/);
  ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[0]));
  ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[1]));

  SetL0delayAndRecalcConditions(0 /*db_idx*/, 20 /*l0_files*/);
  ASSERT_TRUE(IsDbWriteStopped(dbimpls_[0]));
  ASSERT_TRUE(IsDbWriteStopped(dbimpls_[1]));

  SetL0delayAndRecalcConditions(0 /*db_idx*/, 9 /*l0_files*/);
  ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[0]));
  ASSERT_TRUE(IsDbWriteDelayed(dbimpls_[1]));

  SetL0delayAndRecalcConditions(1 /*db_idx*/, 9 /*l0_files*/);
  ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[0]));
  ASSERT_FALSE(IsDbWriteDelayed(dbimpls_[1]));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
