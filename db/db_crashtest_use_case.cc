#include <memory>

#include "rocksdb/db_crashtest_use_case.h"

#include "rocksdb/advanced_options.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/customizable_util.h"

namespace ROCKSDB_NAMESPACE {
static std::unordered_map<std::string, UseCaseConfig>
    crashtest_valid_db_options_configurations = {
        {"max_background_compactions", UseCaseConfig::Equals<int>(20)},
        {"max_open_files", UseCaseConfig::Choice<int>({-1, 100, 500000})},
        {"recycle_log_file_num", UseCaseConfig::Choice<size_t>({0, 1})},
        {"max_subcompactions", UseCaseConfig::Range<int>(1, 4)},
        {"stats_dump_period_sec",
         UseCaseConfig::Choice<unsigned int>({0, 10, 600})},
        {"max_manifest_file_size",
         UseCaseConfig::Choice<uint64_t>(
             {1 * 16384, 2 * 16384, 1024 * 1024 * 1024})},
        {"bytes_per_sync", UseCaseConfig::Choice<uint64_t>({0, 262144})},
        {"wal_bytes_per_sync", UseCaseConfig::Choice<uint64_t>({0, 524288})},
        {"db_write_buffer_size", UseCaseConfig::Choice<size_t>(
                                     {0, 1024 * 1024, 8 * 1024 * 1024,
                                      128 * 1024 * 1024, 1024 * 1024 * 1024})},
        {"max_write_batch_group_size_bytes",
         UseCaseConfig::Choice<uint64_t>(
             {16, 64, 1024 * 1024, 16 * 1024 * 1024})},
        {"wal_compression",
         UseCaseConfig::Choice<CompressionType>(
             {CompressionType::kNoCompression, CompressionType::kZSTD})},
        {"verify_sst_unique_id_in_manifest", UseCaseConfig::Equals<bool>(true)},
        {"allow_data_in_errors", UseCaseConfig::Equals<bool>(true)}};

static std::unordered_map<std::string, UseCaseConfig>
    crashtest_valid_cf_options_configurations = {
        {"memtable_protection_bytes_per_key",
         UseCaseConfig::Choice<uint32_t>({0, 1, 2, 4, 8})},
        {"max_bytes_for_level_base", UseCaseConfig::Equals<uint64_t>(10485760)},
        {"max_write_buffer_number", UseCaseConfig::Equals<int>(3)},
        {"target_file_size_base", UseCaseConfig::Equals<uint64_t>(2097152)},
        {"target_file_size_multiplier", UseCaseConfig::Equals<int>(2)},
        {"write_buffer_size", UseCaseConfig::Choice<size_t>(
                                  {1024 * 1024, 8 * 1024 * 1024,
                                   128 * 1024 * 1024, 1024 * 1024 * 1024})},
        {"periodic_compaction_seconds",
         UseCaseConfig::Choice<uint64_t>({0, 1, 2, 10, 100, 1000})},
        {"level_compaction_dynamic_level_bytes",
         UseCaseConfig::Equals<bool>(true)},
        {"max_write_buffer_size_to_maintain",
         UseCaseConfig::Choice<int64_t>({0, 1024 * 1024, 2 * 1024 * 1024,
                                         4 * 1024 * 1024, 8 * 1024 * 1024})},
        {"memtable_prefix_bloom_size_ratio",
         UseCaseConfig::Choice<double>({0.001, 0.01, 0.1, 0.5})},
        {"min_write_buffer_number_to_merge",
         UseCaseConfig::Choice<int>({1, 2})},
        {"preserve_internal_time_seconds",
         UseCaseConfig::Choice<uint64_t>({0, 60, 3600, 36000})}};

static std::unordered_map<std::string, UseCaseConfig>
    crashtest_simple_valid_db_options_configurations = {
        {"max_background_compactions", UseCaseConfig::Equals<int>(1)}};

static std::unordered_map<std::string, UseCaseConfig>
    crashtest_simple_valid_cf_options_configurations = {
        {"max_bytes_for_level_base", UseCaseConfig::Equals<uint64_t>(67108864)},
        {"target_file_size_base", UseCaseConfig::Equals<uint64_t>(16777216)},
        {"target_file_size_multiplier", UseCaseConfig::Equals<int>(1)},
        {"write_buffer_size",
         UseCaseConfig::Choice<size_t>({32 * 1024 * 1024})},
        {"level_compaction_dynamic_level_bytes",
         UseCaseConfig::Equals<bool>(false)}};

static std::unordered_map<std::string, UseCaseConfig>
    crashtest_txn_valid_db_options_configurations = {
        {"enable_pipelined_write", UseCaseConfig::Equals<bool>(false)}};

static std::unordered_map<std::string, UseCaseConfig>
    crashtest_ber_valid_db_options_configurations = {
        {"best_efforts_recovery", UseCaseConfig::Equals<bool>(true)},
        {"atomic_flush", UseCaseConfig::Equals<bool>(false)}};

static std::unordered_map<std::string, UseCaseConfig>
    crashtest_blob_valid_cf_options_configurations = {
        {"min_blob_size", UseCaseConfig::Choice<uint64_t>({0, 8, 16})},
        {"blob_file_size", UseCaseConfig::Choice<uint64_t>(
                               {1048576, 16777216, 268435456, 1073741824})},
        {"blob_compression_type",
         UseCaseConfig::Choice<CompressionType>(
             {CompressionType::kNoCompression,
              CompressionType::kSnappyCompression,
              CompressionType::kLZ4Compression, CompressionType::kZSTD})},
        {"blob_garbage_collection_age_cutoff",
         UseCaseConfig::Choice<double>({0.0, 0.25, 0.5, 0.75, 1.0})},
        {"blob_garbage_collection_force_threshold",
         UseCaseConfig::Choice<double>({0.5, 0.75, 1.0})},
        {"blob_compaction_readahead_size",
         UseCaseConfig::Choice<uint64_t>({0, 1048576, 4194304})},
        {"blob_file_starting_level", UseCaseConfig::Choice<int>({0, 1, 2, 3})}};

static std::unordered_map<std::string, UseCaseConfig>
    crashtest_tiered_valid_cf_options_configurations = {
        {"preclude_last_level_data_seconds",
         UseCaseConfig::Choice<uint64_t>({60, 3600, 36000})},
        {"compaction_style", UseCaseConfig::Equals<CompactionStyle>(
                                 CompactionStyle::kCompactionStyleUniversal)},
        {"enable_blob_files", UseCaseConfig::Equals<bool>(false)}};

static std::unordered_map<std::string, UseCaseConfig>
    crashtest_multiops_txn_valid_cf_options_configurations = {
        {"write_buffer_size", UseCaseConfig::Choice<size_t>({65536})}};

DBCrashtestUseCase::DBCrashtestUseCase() {
  RegisterUseCaseDBOptionsConfig(&crashtest_valid_db_options_configurations);
  RegisterUseCaseCFOptionsConfig(&crashtest_valid_cf_options_configurations);
}

SimpleDefaultParams::SimpleDefaultParams() {
  RegisterUseCaseDBOptionsConfig(
      &crashtest_simple_valid_db_options_configurations);
  RegisterUseCaseCFOptionsConfig(
      &crashtest_simple_valid_cf_options_configurations);
}

TxnParams::TxnParams() {
  RegisterUseCaseDBOptionsConfig(
      &crashtest_txn_valid_db_options_configurations);
}

BestEffortsRecoveryParams::BestEffortsRecoveryParams() {
  RegisterUseCaseDBOptionsConfig(
      &crashtest_ber_valid_db_options_configurations);
}

BlobParams::BlobParams() {
  RegisterUseCaseCFOptionsConfig(
      &crashtest_blob_valid_cf_options_configurations);
}

TieredParams::TieredParams() {
  RegisterUseCaseCFOptionsConfig(
      &crashtest_tiered_valid_cf_options_configurations);
}

MultiopsTxnDefaultParams::MultiopsTxnDefaultParams() {
  RegisterUseCaseCFOptionsConfig(
      &crashtest_multiops_txn_valid_cf_options_configurations);
}
}  // namespace ROCKSDB_NAMESPACE
