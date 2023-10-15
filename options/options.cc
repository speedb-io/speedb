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
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/options.h"

#include <cinttypes>
#include <limits>

#include "logging/logging.h"
#include "monitoring/statistics.h"
#include "options/db_options.h"
#include "options/options_formatter_impl.h"
#include "options/options_helper.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/wal_filter.h"
#include "rocksdb/write_buffer_manager.h"
#include "rocksdb/write_controller.h"
#include "table/block_based/block_based_table_factory.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions() {
  assert(memtable_factory.get() != nullptr);
}

AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions(const Options& options)
    : max_write_buffer_number(options.max_write_buffer_number),
      min_write_buffer_number_to_merge(
          options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          options.max_write_buffer_number_to_maintain),
      max_write_buffer_size_to_maintain(
          options.max_write_buffer_size_to_maintain),
      inplace_update_support(options.inplace_update_support),
      inplace_update_num_locks(options.inplace_update_num_locks),
      experimental_mempurge_threshold(options.experimental_mempurge_threshold),
      inplace_callback(options.inplace_callback),
      memtable_prefix_bloom_size_ratio(
          options.memtable_prefix_bloom_size_ratio),
      memtable_whole_key_filtering(options.memtable_whole_key_filtering),
      memtable_huge_page_size(options.memtable_huge_page_size),
      memtable_insert_with_hint_prefix_extractor(
          options.memtable_insert_with_hint_prefix_extractor),
      bloom_locality(options.bloom_locality),
      arena_block_size(options.arena_block_size),
      compression_per_level(options.compression_per_level),
      num_levels(options.num_levels),
      level0_slowdown_writes_trigger(options.level0_slowdown_writes_trigger),
      level0_stop_writes_trigger(options.level0_stop_writes_trigger),
      target_file_size_base(options.target_file_size_base),
      target_file_size_multiplier(options.target_file_size_multiplier),
      level_compaction_dynamic_level_bytes(
          options.level_compaction_dynamic_level_bytes),
      max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
      max_bytes_for_level_multiplier_additional(
          options.max_bytes_for_level_multiplier_additional),
      max_compaction_bytes(options.max_compaction_bytes),
      ignore_max_compaction_bytes_for_input(
          options.ignore_max_compaction_bytes_for_input),
      soft_pending_compaction_bytes_limit(
          options.soft_pending_compaction_bytes_limit),
      hard_pending_compaction_bytes_limit(
          options.hard_pending_compaction_bytes_limit),
      compaction_style(options.compaction_style),
      compaction_pri(options.compaction_pri),
      compaction_options_universal(options.compaction_options_universal),
      compaction_options_fifo(options.compaction_options_fifo),
      max_sequential_skip_in_iterations(
          options.max_sequential_skip_in_iterations),
      memtable_factory(options.memtable_factory),
      table_properties_collector_factories(
          options.table_properties_collector_factories),
      max_successive_merges(options.max_successive_merges),
      optimize_filters_for_hits(options.optimize_filters_for_hits),
      paranoid_file_checks(options.paranoid_file_checks),
      force_consistency_checks(options.force_consistency_checks),
      report_bg_io_stats(options.report_bg_io_stats),
      ttl(options.ttl),
      periodic_compaction_seconds(options.periodic_compaction_seconds),
      sample_for_compression(options.sample_for_compression),
      preclude_last_level_data_seconds(
          options.preclude_last_level_data_seconds),
      preserve_internal_time_seconds(options.preserve_internal_time_seconds),
      enable_blob_files(options.enable_blob_files),
      min_blob_size(options.min_blob_size),
      blob_file_size(options.blob_file_size),
      blob_compression_type(options.blob_compression_type),
      enable_blob_garbage_collection(options.enable_blob_garbage_collection),
      blob_garbage_collection_age_cutoff(
          options.blob_garbage_collection_age_cutoff),
      blob_garbage_collection_force_threshold(
          options.blob_garbage_collection_force_threshold),
      blob_compaction_readahead_size(options.blob_compaction_readahead_size),
      blob_file_starting_level(options.blob_file_starting_level),
      blob_cache(options.blob_cache),
      prepopulate_blob_cache(options.prepopulate_blob_cache) {
  assert(memtable_factory.get() != nullptr);
  if (max_bytes_for_level_multiplier_additional.size() <
      static_cast<unsigned int>(num_levels)) {
    max_bytes_for_level_multiplier_additional.resize(num_levels, 1);
  }
}

ColumnFamilyOptions::ColumnFamilyOptions()
    : compression(Snappy_Supported() ? kSnappyCompression : kNoCompression),
      table_factory(
          std::shared_ptr<TableFactory>(new BlockBasedTableFactory())) {}

ColumnFamilyOptions::ColumnFamilyOptions(const Options& options)
    : ColumnFamilyOptions(*static_cast<const ColumnFamilyOptions*>(&options)) {}

DBOptions::DBOptions() {}
DBOptions::DBOptions(const Options& options)
    : DBOptions(*static_cast<const DBOptions*>(&options)) {}

void DBOptions::Dump(Logger* log) const {
  ConfigOptions config_options;
  config_options.depth = ConfigOptions::kDepthPrintable;
  config_options.formatter.reset(new LogOptionsFormatter());
  auto db_cfg = DBOptionsAsConfigurable(*this);
  auto db_str = db_cfg->ToString(config_options, "Options");
  ROCKS_LOG_HEADER(log, "%s", db_str.c_str());
}  // DBOptions::Dump

void ColumnFamilyOptions::Dump(Logger* log) const {
  ConfigOptions config_options;
  config_options.depth = ConfigOptions::kDepthPrintable;
  config_options.formatter.reset(new LogOptionsFormatter());
  auto cf_cfg = CFOptionsAsConfigurable(*this);
  auto cf_str = cf_cfg->ToString(config_options, "Options");
  ROCKS_LOG_HEADER(log, "%s", cf_str.c_str());
}  // ColumnFamilyOptions::Dump

void Options::Dump(Logger* log) const {
  DBOptions::Dump(log);
  ColumnFamilyOptions::Dump(log);
}  // Options::Dump

void Options::DumpCFOptions(Logger* log) const {
  ColumnFamilyOptions::Dump(log);
}  // Options::DumpCFOptions

//
// The goal of this method is to create a configuration that
// allows an application to write all files into L0 and
// then do a single compaction to output all files into L1.
Options*
Options::PrepareForBulkLoad()
{
  // never slowdown ingest.
  level0_file_num_compaction_trigger = (1<<30);
  level0_slowdown_writes_trigger = (1<<30);
  level0_stop_writes_trigger = (1<<30);
  soft_pending_compaction_bytes_limit = 0;
  hard_pending_compaction_bytes_limit = 0;

  // no auto compactions please. The application should issue a
  // manual compaction after all data is loaded into L0.
  disable_auto_compactions = true;
  // A manual compaction run should pick all files in L0 in
  // a single compaction run.
  max_compaction_bytes = (static_cast<uint64_t>(1) << 60);

  // It is better to have only 2 levels, otherwise a manual
  // compaction would compact at every possible level, thereby
  // increasing the total time needed for compactions.
  num_levels = 2;

  // Need to allow more write buffers to allow more parallism
  // of flushes.
  max_write_buffer_number = 6;
  min_write_buffer_number_to_merge = 1;

  // When compaction is disabled, more parallel flush threads can
  // help with write throughput.
  max_background_flushes = 4;

  // Prevent a memtable flush to automatically promote files
  // to L1. This is helpful so that all files that are
  // input to the manual compaction are all at L0.
  max_background_compactions = 2;

  // The compaction would create large files in L1.
  target_file_size_base = 256 * 1024 * 1024;
  return this;
}

Options* Options::OptimizeForSmallDb() {
  // 16MB block cache
  std::shared_ptr<Cache> cache = NewLRUCache(16 << 20);

  ColumnFamilyOptions::OptimizeForSmallDb(&cache);
  DBOptions::OptimizeForSmallDb(&cache);
  return this;
}

Options* Options::DisableExtraChecks() {
  // See https://github.com/facebook/rocksdb/issues/9354
  force_consistency_checks = false;
  // Considered but no clear performance impact seen:
  // * check_flush_compaction_key_order
  // * paranoid_checks
  // * flush_verify_memtable_count
  // By current API contract, not including
  // * verify_checksums
  // because checking storage data integrity is a more standard practice.
  return this;
}

Options* Options::OldDefaults(int rocksdb_major_version,
                              int rocksdb_minor_version) {
  ColumnFamilyOptions::OldDefaults(rocksdb_major_version,
                                   rocksdb_minor_version);
  DBOptions::OldDefaults(rocksdb_major_version, rocksdb_minor_version);
  return this;
}

Options* Options::EnableSpeedbFeatures(SharedOptions& shared_options) {
  EnableSpeedbFeaturesDB(shared_options);
  EnableSpeedbFeaturesCF(shared_options);
  return this;
}

SharedOptions::SharedOptions(size_t total_ram_size_bytes, size_t total_threads,
                             size_t delayed_write_rate) {
  total_threads_ = total_threads;
  total_ram_size_bytes_ = total_ram_size_bytes;
  delayed_write_rate_ = delayed_write_rate;
  // initial_write_buffer_size_ is initialized to 1 to avoid from empty memory
  // which might cause some problems
  int initial_write_buffer_size_ = 1;
  cache = NewLRUCache(total_ram_size_bytes_);
  write_controller.reset(
      new WriteController(true /*dynamic_delay*/, delayed_write_rate_));
  write_buffer_manager.reset(new WriteBufferManager(
      initial_write_buffer_size_, cache, true /*allow_stall*/));
}

void SharedOptions::IncreaseWriteBufferSize(size_t increase_by) {
  // Max write_buffer_manager->buffer_size()
  size_t wbm_max_buf_size = total_ram_size_bytes_ / 4;
  size_t current_buffer_size = write_buffer_manager->buffer_size();
  size_t set_buf_res = 0;

  if (current_buffer_size == 1 && increase_by > 1) {
    set_buf_res = increase_by;
    if (wbm_max_buf_size < increase_by) {
      set_buf_res = wbm_max_buf_size;
    }
  } else if (wbm_max_buf_size > current_buffer_size + increase_by) {
    set_buf_res = current_buffer_size + increase_by;
  } else if (wbm_max_buf_size <= current_buffer_size + increase_by) {
    set_buf_res = wbm_max_buf_size;
  }
  if (set_buf_res != 0) {
    write_buffer_manager->SetBufferSize(set_buf_res);
  }
}

DBOptions* DBOptions::EnableSpeedbFeaturesDB(SharedOptions& shared_options) {
  env = shared_options.env;
  IncreaseParallelism((int)shared_options.GetTotalThreads());
  delayed_write_rate = shared_options.GetDelayedWriteRate();
  bytes_per_sync = 1ul << 20;
  use_dynamic_delay = true;
  write_buffer_manager = shared_options.write_buffer_manager;
  write_controller = shared_options.write_controller;
  return this;
}

DBOptions* DBOptions::OldDefaults(int rocksdb_major_version,
                                  int rocksdb_minor_version) {
  if (rocksdb_major_version < 4 ||
      (rocksdb_major_version == 4 && rocksdb_minor_version < 7)) {
    max_file_opening_threads = 1;
    table_cache_numshardbits = 4;
  }
  if (rocksdb_major_version < 5 ||
      (rocksdb_major_version == 5 && rocksdb_minor_version < 2)) {
    delayed_write_rate = 2 * 1024U * 1024U;
  } else if (rocksdb_major_version < 5 ||
             (rocksdb_major_version == 5 && rocksdb_minor_version < 6)) {
    delayed_write_rate = 16 * 1024U * 1024U;
  }
  max_open_files = 5000;
  wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::EnableSpeedbFeaturesCF(
    SharedOptions& shared_options) {
  // to disable flush due to write buffer full
  // each new column family will ask the write buffer manager to increase the
  // write buffer size by 512 * 1024 * 1024ul
  shared_options.IncreaseWriteBufferSize(512 * 1024 * 1024ul);
  auto db_wbf_size = shared_options.write_buffer_manager->buffer_size();
  // cf write_buffer_size
  write_buffer_size = std::min<size_t>(db_wbf_size / 4, 64ul << 20);
  max_write_buffer_number = 4;
  min_write_buffer_number_to_merge = 1;
  // set the pinning option for indexes and filters
  {
    ConfigOptions config_options;
    config_options.ignore_unknown_options = false;
    config_options.ignore_unsupported_options = false;
    BlockBasedTableOptions block_based_table_options;
    Status s = FilterPolicy::CreateFromString(
        config_options, "speedb.PairedBloomFilter:10",
        &block_based_table_options.filter_policy);
    assert(s.ok());
    block_based_table_options.cache_index_and_filter_blocks = true;
    block_based_table_options.cache_index_and_filter_blocks_with_high_priority =
        true;
    block_based_table_options.pin_l0_filter_and_index_blocks_in_cache = false;
    block_based_table_options.metadata_cache_options.unpartitioned_pinning =
        PinningTier::kAll;
    block_based_table_options.metadata_cache_options.partition_pinning =
        PinningTier::kAll;
    block_based_table_options.block_cache = shared_options.cache;
    auto& cache_usage_options = block_based_table_options.cache_usage_options;
    CacheEntryRoleOptions role_options;
    role_options.charged = CacheEntryRoleOptions::Decision::kEnabled;
    cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kFilterConstruction, role_options});
    cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kBlockBasedTableReader, role_options});
    cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kCompressionDictionaryBuildingBuffer, role_options});
    cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kFileMetadata, role_options});
    table_factory.reset(NewBlockBasedTableFactory(block_based_table_options));
  }
  if (prefix_extractor) {
    memtable_factory.reset(NewHashSkipListRepFactory());
  } else {
    memtable_factory.reset(NewHashSpdbRepFactory());
  }
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OldDefaults(
    int rocksdb_major_version, int rocksdb_minor_version) {
  if (rocksdb_major_version < 5 ||
      (rocksdb_major_version == 5 && rocksdb_minor_version <= 18)) {
    compaction_pri = CompactionPri::kByCompensatedSize;
  }
  if (rocksdb_major_version < 4 ||
      (rocksdb_major_version == 4 && rocksdb_minor_version < 7)) {
    write_buffer_size = 4 << 20;
    target_file_size_base = 2 * 1048576;
    max_bytes_for_level_base = 10 * 1048576;
    soft_pending_compaction_bytes_limit = 0;
    hard_pending_compaction_bytes_limit = 0;
  }
  if (rocksdb_major_version < 5) {
    level0_stop_writes_trigger = 24;
  } else if (rocksdb_major_version == 5 && rocksdb_minor_version < 2) {
    level0_stop_writes_trigger = 30;
  }

  return this;
}

// Optimization functions
DBOptions* DBOptions::OptimizeForSmallDb(std::shared_ptr<Cache>* cache) {
  max_file_opening_threads = 1;
  max_open_files = 5000;

  // Cost memtable to block cache too.
  std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager> wbm =
      std::make_shared<ROCKSDB_NAMESPACE::WriteBufferManager>(
          0, (cache != nullptr) ? *cache : std::shared_ptr<Cache>());
  write_buffer_manager = wbm;

  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForSmallDb(
    std::shared_ptr<Cache>* cache) {
  write_buffer_size = 2 << 20;
  target_file_size_base = 2 * 1048576;
  max_bytes_for_level_base = 10 * 1048576;
  soft_pending_compaction_bytes_limit = 256 * 1048576;
  hard_pending_compaction_bytes_limit = 1073741824ul;

  BlockBasedTableOptions table_options;
  table_options.block_cache =
      (cache != nullptr) ? *cache : std::shared_ptr<Cache>();
  table_options.cache_index_and_filter_blocks = true;
  // Two level iterator to avoid LRU cache imbalance
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_factory.reset(new BlockBasedTableFactory(table_options));

  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForPointLookup(
    uint64_t block_cache_size_mb) {
  BlockBasedTableOptions block_based_options;
  block_based_options.data_block_index_type =
      BlockBasedTableOptions::kDataBlockBinaryAndHash;
  block_based_options.data_block_hash_table_util_ratio = 0.75;
  block_based_options.filter_policy.reset(NewBloomFilterPolicy(10));
  block_based_options.block_cache =
      NewLRUCache(static_cast<size_t>(block_cache_size_mb * 1024 * 1024));
  table_factory.reset(new BlockBasedTableFactory(block_based_options));
  memtable_prefix_bloom_size_ratio = 0.02;
  memtable_whole_key_filtering = true;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeLevelStyleCompaction(
    uint64_t memtable_memory_budget) {
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  // merge two memtables when flushing to L0
  min_write_buffer_number_to_merge = 2;
  // this means we'll use 50% extra memory in the worst case, but will reduce
  // write stalls.
  max_write_buffer_number = 6;
  // start flushing L0->L1 as soon as possible. each file on level0 is
  // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
  // memtable_memory_budget.
  level0_file_num_compaction_trigger = 2;
  // doesn't really matter much, but we don't want to create too many files
  target_file_size_base = memtable_memory_budget / 8;
  // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
  max_bytes_for_level_base = memtable_memory_budget;

  // level style compaction
  compaction_style = kCompactionStyleLevel;

  // only compress levels >= 2
  compression_per_level.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    if (i < 2) {
      compression_per_level[i] = kNoCompression;
    } else {
      compression_per_level[i] =
          LZ4_Supported()
              ? kLZ4Compression
              : (Snappy_Supported() ? kSnappyCompression : kNoCompression);
    }
  }
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeUniversalStyleCompaction(
    uint64_t memtable_memory_budget) {
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  // merge two memtables when flushing to L0
  min_write_buffer_number_to_merge = 2;
  // this means we'll use 50% extra memory in the worst case, but will reduce
  // write stalls.
  max_write_buffer_number = 6;
  // universal style compaction
  compaction_style = kCompactionStyleUniversal;
  compaction_options_universal.compression_size_percent = 80;
  return this;
}

DBOptions* DBOptions::IncreaseParallelism(int total_threads) {
  max_background_jobs = total_threads;
  env->SetBackgroundThreads(total_threads, Env::LOW);
  env->SetBackgroundThreads(1, Env::HIGH);
  return this;
}


ReadOptions::ReadOptions()
    : snapshot(nullptr),
      iterate_lower_bound(nullptr),
      iterate_upper_bound(nullptr),
      readahead_size(0),
      max_skippable_internal_keys(0),
      read_tier(kReadAllTier),
      verify_checksums(true),
      fill_cache(true),
      tailing(false),
      managed(false),
      total_order_seek(false),
      auto_prefix_mode(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      ignore_range_deletions(false),
      timestamp(nullptr),
      iter_start_ts(nullptr),
      deadline(std::chrono::microseconds::zero()),
      io_timeout(std::chrono::microseconds::zero()),
      value_size_soft_limit(std::numeric_limits<uint64_t>::max()),
      adaptive_readahead(false),
      async_io(false),
      optimize_multiget_for_io(true) {}

ReadOptions::ReadOptions(bool cksum, bool cache)
    : snapshot(nullptr),
      iterate_lower_bound(nullptr),
      iterate_upper_bound(nullptr),
      readahead_size(0),
      max_skippable_internal_keys(0),
      read_tier(kReadAllTier),
      verify_checksums(cksum),
      fill_cache(cache),
      tailing(false),
      managed(false),
      total_order_seek(false),
      auto_prefix_mode(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      ignore_range_deletions(false),
      timestamp(nullptr),
      iter_start_ts(nullptr),
      deadline(std::chrono::microseconds::zero()),
      io_timeout(std::chrono::microseconds::zero()),
      value_size_soft_limit(std::numeric_limits<uint64_t>::max()),
      adaptive_readahead(false),
      async_io(false),
      optimize_multiget_for_io(true) {}

}  // namespace ROCKSDB_NAMESPACE
