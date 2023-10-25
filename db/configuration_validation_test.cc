#include <set>

#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/db_crashtest_use_case.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/use_case.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
class ConfigurationValidationTest : public testing::Test {
 public:
  ConfigurationValidationTest() {
    env_ = Env::Default();
    db_name_ = test::PerThreadDBPath("configuration_validation_test");
  }

  std::string db_name_;
  Env* env_;
};

TEST_F(ConfigurationValidationTest, DBCrashtestValidConfiguration) {
  Options options;
  options.create_if_missing = true;
  // setting options according to the default_params config in db_crashtest
  // TODO: set table options
  options.memtable_protection_bytes_per_key = 4;
  options.max_background_compactions = 20;
  options.max_bytes_for_level_base = 10485760;
  options.max_write_buffer_number = 3;
  options.max_open_files = -1;
  options.recycle_log_file_num = 1;
  options.max_subcompactions = 4;
  options.target_file_size_base = 2097152;
  options.target_file_size_multiplier = 2;
  options.write_buffer_size = 128 * 1024 * 1024;
  options.periodic_compaction_seconds = 100;
  options.stats_dump_period_sec = 600;
  options.max_manifest_file_size = 2 * 16384;
  options.bytes_per_sync = 0;
  options.wal_bytes_per_sync = 0;
  options.db_write_buffer_size = 1024 * 1024;
  options.max_write_batch_group_size_bytes = 16 * 1024 * 1024;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_write_buffer_size_to_maintain = 2 * 1024 * 1024;
  options.memtable_prefix_bloom_size_ratio = 0.5;
  options.wal_compression = CompressionType::kZSTD;
  options.verify_sst_unique_id_in_manifest = true;
  options.allow_data_in_errors = true;
  options.min_write_buffer_number_to_merge = 2;
  options.preserve_internal_time_seconds = 3600;
  DBOptions db_options(options);
  ConfigOptions cfg_opts(db_options);
  std::set<std::string> valid_opts;
  std::set<std::string> invalid_opts;
  DBCrashtestUseCase db_crashtest_use_case;
  Status valid = UseCase::ValidateOptions(cfg_opts, "rocksdb.DBCrashtestUseCase", options, valid_opts, invalid_opts);
  ASSERT_EQ(valid, Status::OK());
  ASSERT_TRUE(invalid_opts.empty());
}

TEST_F(ConfigurationValidationTest, DBCrashtestInvalidConfiguration) {
  Options options;
  options.create_if_missing = true;
  // using default rocksdb options (unless changed) should be invalid
  DBOptions db_options(options);
  ConfigOptions cfg_opts(db_options);
  std::set<std::string> valid_opts;
  std::set<std::string> invalid_opts;
  DBCrashtestUseCase db_crashtest_use_case;
  Status valid = UseCase::ValidateOptions(cfg_opts, "rocksdb.DBCrashtestUseCase", options, valid_opts, invalid_opts);
  ASSERT_EQ(valid, Status::InvalidArgument());
  ASSERT_FALSE(invalid_opts.empty());
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
