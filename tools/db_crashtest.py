#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import time
import random
import re
import tempfile
import signal
import subprocess
import shutil
import argparse
import datetime

# params overwrite priority:
#   for default:
#       default_params < {blackbox,whitebox}_default_params < args
#   for simple:
#       default_params < {blackbox,whitebox}_default_params <
#       simple_default_params <
#       {blackbox,whitebox}_simple_default_params < args
#   for cf_consistency:
#       default_params < {blackbox,whitebox}_default_params <
#       cf_consistency_params < args
#   for txn:
#       default_params < {blackbox,whitebox}_default_params < txn_params < args
#   for ts:
#       default_params < {blackbox,whitebox}_default_params < ts_params < args
#   for multiops_txn:
#       default_params < {blackbox,whitebox}_default_params < multiops_txn_params < args


supplied_ops = {
    "writepercent": -1,
    "delpercent": -1,
    "prefixpercent": -1,
    "delrangepercent": -1,
    "readpercent": -1,
    "iterpercent": -1,
    "customopspercent": -1,
}

default_params = {
    "acquire_snapshot_one_in": 10000,
    "backup_max_size": 100 * 1024 * 1024,
    # Consider larger number when backups considered more stable
    "backup_one_in": 100000,
    "batch_protection_bytes_per_key": lambda: random.choice([0, 8]),
    "block_size": random.choice([16384, 4096]),
    "bloom_bits": lambda: random.choice([random.randint(0,19),
                                         random.lognormvariate(2.3, 1.3)]),
    "cache_index_and_filter_blocks": lambda: random.randint(0, 1),
    "cache_size": 8388608,
    "reserve_table_reader_memory": lambda: random.choice([0, 1]),
    "checkpoint_one_in": 1000000,
    "compression_type": lambda: random.choice(
        ["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]),
    "bottommost_compression_type": lambda:
        "disable" if random.randint(0, 1) == 0 else
        random.choice(
            ["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]),
    "checksum_type" : lambda: random.choice(["kCRC32c", "kxxHash", "kxxHash64", "kXXH3"]),
    "compression_max_dict_bytes": lambda: 16384 * random.randint(0, 1),
    "compression_zstd_max_train_bytes": lambda: 65536 * random.randint(0, 1),
    # Disabled compression_parallel_threads as the feature is not stable
    # lambda: random.choice([1] * 9 + [4])
    "compression_parallel_threads": 1,
    "compression_max_dict_buffer_bytes": lambda: (1 << random.randint(0, 40)) - 1,
    "clear_column_family_one_in": 0,
    "compact_files_one_in": 1000000,
    "compact_range_one_in": 1000000,
    "destroy_db_initially": 0,
    "enable_pipelined_write": lambda: random.choice([0, 0, 0, 0, 1]),
    "enable_compaction_filter": lambda: random.choice([0, 0, 0, 1]),
    "expected_values_dir": lambda: setup_expected_values_dir(),
    "fail_if_options_file_error": lambda: random.randint(0, 1),
    "flush_one_in": 1000000,
    "file_checksum_impl": lambda: random.choice(["none", "crc32c", "xxh64", "big"]),
    "get_live_files_one_in": 100000,
    # Note: the following two are intentionally disabled as the corresponding
    # APIs are not guaranteed to succeed.
    "get_sorted_wal_files_one_in": 0,
    "get_current_wal_file_one_in": 0,
    # Temporarily disable hash index
    "index_type": lambda: random.choice([0, 0, 0, 2, 2, 3]),
    "mark_for_compaction_one_file_in": lambda: 10 * random.randint(0, 1),
    "max_background_compactions": 20,
    "max_bytes_for_level_base": 10485760,
    "max_key": random.choice([100 * 1024, 1024 * 1024, 10 * 1024 * 1024]),
    "max_write_buffer_number": 3,
    "mmap_read": lambda: random.randint(0, 1),
    # Setting `nooverwritepercent > 0` is only possible because we do not vary
    # the random seed between runs, so the same keys are chosen by every run 
    # for disallowing overwrites.
    "nooverwritepercent": random.choice([0, 5, 20, 30, 40, 50, 95]),
    "open_files": lambda : random.choice([-1, -1, 100, 500000]),
    "optimize_filters_for_memory": lambda: random.randint(0, 1),
    "partition_filters": lambda: random.randint(0, 1),
    "partition_pinning": lambda: random.randint(0, 3),
    "pause_background_one_in": 1000000,
    "prefix_size" : lambda: random.choice([-1, 1, 5, 7, 8]),
    "progress_reports": 0,
    "recycle_log_file_num": lambda: random.randint(0, 1),
    "snapshot_hold_ops": 100000,
    "sst_file_manager_bytes_per_sec": lambda: random.choice([0, 104857600]),
    "sst_file_manager_bytes_per_truncate": lambda: random.choice([0, 1048576]),
    "long_running_snapshots": lambda: random.randint(0, 1),
    "subcompactions": lambda: random.randint(1, 4),
    "target_file_size_base": 2097152,
    "target_file_size_multiplier": 2,
    "test_batches_snapshots": random.choice([0, 0, 0, 1]),
    "top_level_index_pinning": lambda: random.randint(0, 3),
    "unpartitioned_pinning": lambda: random.randint(0, 3),
    "use_direct_reads": lambda: random.randint(0, 1),
    "use_direct_io_for_flush_and_compaction": lambda: random.randint(0, 1),
    "mock_direct_io": False,
    "use_clock_cache": 0, # currently broken
    "use_full_merge_v1": lambda: random.randrange(10) == 0,
    "use_merge": lambda: random.randint(0, 1),
    # 999 -> use Bloom API
    "ribbon_starting_level": lambda: random.choice([random.randint(-1, 10), 999]),
    "use_block_based_filter": lambda: random.randint(0, 1),
    "value_size_mult": 32,
    "verify_checksum": 1,
    "write_buffer_size": lambda: random.choice(
        [1024 * 1024, 8 * 1024 * 1024, 128 * 1024 * 1024, 1024 * 1024 * 1024]),
    "format_version": lambda: random.choice([2, 3, 4, 5, 5, 5, 5, 5, 5]),
    "index_block_restart_interval": lambda: random.choice(range(1, 16)),
    "use_multiget" : lambda: random.randint(0, 1),
    "periodic_compaction_seconds" :
        lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    "compaction_ttl" : lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    # Test small max_manifest_file_size in a smaller chance, as most of the
    # time we wnat manifest history to be preserved to help debug
    "max_manifest_file_size" : lambda : random.choice(
        [t * 16384 if t < 3 else 1024 * 1024 * 1024 for t in range(1, 30)]),
    # Sync mode might make test runs slower so running it in a smaller chance
    "sync" : lambda : random.randrange(20) == 0,
    # Disable compaction_readahead_size because the test is not passing.
    #"compaction_readahead_size" : lambda : random.choice(
    #    [0, 0, 1024 * 1024]),
    "db_write_buffer_size" : lambda: random.choice(
        [0, 0, 0, 1024 * 1024, 8 * 1024 * 1024, 128 * 1024 * 1024, 1024 * 1024 * 1024]),
    "avoid_unnecessary_blocking_io" : random.randint(0, 1),
    "write_dbid_to_manifest" : random.randint(0, 1),
    "avoid_flush_during_recovery" : lambda: random.choice(
        [1 if t == 0 else 0 for t in range(0, 8)]),
    "max_write_batch_group_size_bytes" : lambda: random.choice(
        [16, 64, 1024 * 1024, 16 * 1024 * 1024]),
    "level_compaction_dynamic_level_bytes" : True,
    "verify_checksum_one_in": 1000000,
    "verify_db_one_in": 100000,
    "continuous_verification_interval" : 0,
    "max_key_len": 0,
    "key_len_percent_dist": "0",
    "read_fault_one_in": lambda: random.choice([0, 32, 1000]),
    "open_metadata_write_fault_one_in": lambda: random.choice([0, 0, 8]),
    "open_write_fault_one_in": lambda: random.choice([0, 0, 16]),
    "open_read_fault_one_in": lambda: random.choice([0, 0, 32]),
    "sync_fault_injection": False,
    "get_property_one_in": 1000000,
    "paranoid_file_checks": lambda: random.choice([0, 1, 1, 1]),
    "max_write_buffer_size_to_maintain": lambda: random.choice(
        [0, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024]),
    "user_timestamp_size": 0,
    "secondary_cache_fault_one_in" : lambda: random.choice([0, 0, 32]),
    "prepopulate_block_cache" : lambda: random.choice([0, 1]),
    "memtable_prefix_bloom_size_ratio": lambda: random.choice([0.001, 0.01, 0.1, 0.5]),
    "memtable_whole_key_filtering": lambda: random.randint(0, 1),
    "detect_filter_construct_corruption": lambda: random.choice([0, 1]),
    "adaptive_readahead": lambda: random.choice([0, 1]),
    "async_io": lambda: random.choice([0, 1]),
    "wal_compression": lambda: random.choice(["none", "zstd"]),
    # cannot change seed between runs because the seed decides which keys are nonoverwrittenable
    "seed": int(time.time() * 1000000) & 0xffffffff,
    "verify_before_write": lambda: random.randrange(20) == 0,
    "allow_concurrent_memtable_write": lambda: random.randint(0, 1),
    # only done when thread#0 does TestAcquireSnapshot. 
    "compare_full_db_state_snapshot": lambda: random.choice([0, 0, 0, 1]),
    "num_iterations": lambda: random.randint(0, 100),
    "sync_wal_one_in": 100000,
    "data_block_index_type": random.randint(0, 1),
    "data_block_hash_table_util_ratio": random.randint(0, 100) / 100.0,
    "customopspercent": 0,
}

_TEST_DIR_ENV_VAR = 'TEST_TMPDIR'
_DEBUG_LEVEL_ENV_VAR = 'DEBUG_LEVEL'

stress_cmd = "./db_stress"

def is_release_mode():
    return os.environ.get(_DEBUG_LEVEL_ENV_VAR) == "0"


def get_dbname(test_name):
    test_dir_name = "rocksdb_crashtest_" + test_name
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        dbname = tempfile.mkdtemp(prefix=test_dir_name)
    else:
        dbname = test_tmpdir + "/" + test_dir_name
        shutil.rmtree(dbname, True)
        os.mkdir(dbname)
    return dbname


expected_values_dir = None
def setup_expected_values_dir():
    global expected_values_dir
    if expected_values_dir is not None:
        return expected_values_dir
    expected_dir_prefix = "rocksdb_crashtest_expected_"
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        expected_values_dir = tempfile.mkdtemp(
            prefix=expected_dir_prefix)
    else:
        # if tmpdir is specified, store the expected_values_dir under that dir
        expected_values_dir = test_tmpdir + "/rocksdb_crashtest_expected"
        if os.path.exists(expected_values_dir):
            shutil.rmtree(expected_values_dir)
        os.mkdir(expected_values_dir)
    return expected_values_dir

multiops_txn_key_spaces_file = None
def setup_multiops_txn_key_spaces_file():
    global multiops_txn_key_spaces_file
    if multiops_txn_key_spaces_file is not None:
        return multiops_txn_key_spaces_file
    key_spaces_file_prefix = "rocksdb_crashtest_multiops_txn_key_spaces"
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        multiops_txn_key_spaces_file = tempfile.mkstemp(
                prefix=key_spaces_file_prefix)[1]
    else:
        if not os.path.exists(test_tmpdir):
            os.mkdir(test_tmpdir)
        multiops_txn_key_spaces_file = tempfile.mkstemp(
                prefix=key_spaces_file_prefix, dir=test_tmpdir)[1]
    return multiops_txn_key_spaces_file


def is_direct_io_supported(dbname):
    with tempfile.NamedTemporaryFile(dir=dbname) as f:
        try:
            os.open(f.name, os.O_DIRECT)
        except BaseException:
            return False
        return True


def generate_key_dist_and_len(params):
    # check if user supplied key dist or len
    if params["max_key_len"] == 0 and params["key_len_percent_dist"] != "0":
        params["max_key_len"] = params["key_len_percent_dist"].count(",") + 1
        return
    
    if params["max_key_len"] == 0 and params["key_len_percent_dist"] == "0":
        params["max_key_len"] = random.randint(1, 10)
    
    dist = random_distribution(params["max_key_len"] - 1)
    params["key_len_percent_dist"] = ",".join(str(i) for i in dist)


# Randomly select unique points (cut_points) on the distribution range
# and set the distribution to the differences between these points.
# Inspired by the following post, with changes to disallow 0:
# https://math.stackexchange.com/questions/1276206/method-of-generating-random-numbers-that-sum-to-100-is-this-truly-random/1276225#1276225
def random_distribution(cuts_count):
    cut_points = set()
    while len(cut_points) < cuts_count:
        cut_points.add(random.randint(1, 100 - 1))
    dist = []
    for x in sorted(cut_points):
        dist.append(x - sum(dist))
    dist.append(100 - sum(dist))
    return dist


blackbox_default_params = {
    "disable_wal": lambda: random.choice([0, 0, 0, 1]),
    # total time for this script to test db_stress
    "duration": 4000,
    # time for one db_stress instance to run
    "interval": 240,
    # since we will be killing anyway, use large value for ops_per_thread
    "ops_per_thread": 100000000,
    "reopen": 0,
    "set_options_one_in": 10000,
}

whitebox_default_params = {
    # TODO: enable this once we figure out how to adjust kill odds for WAL-
    # disabled runs, and either (1) separate full `db_stress` runs out of
    # whitebox crash or (2) support verification at end of `db_stress` runs
    # that ran with WAL disabled.
    "disable_wal": 0,
    "duration": 10000,
    "disable_kill_points": False,
    "ops_per_thread": 200000,
    "random_kill_odd": 888887,
    "reopen": 20,
}

simple_default_params = {
    "column_families": 1,
    "experimental_mempurge_threshold": lambda: 10.0*random.random(),
    "max_background_compactions": 1,
    "max_bytes_for_level_base": 67108864,
    "memtablerep": lambda: random.choice(["skip_list", "speedb.HashSpdRepFactory"]),
    "target_file_size_base": 16777216,
    "target_file_size_multiplier": 1,
    "test_batches_snapshots": 0,
    "write_buffer_size": 32 * 1024 * 1024,
    "level_compaction_dynamic_level_bytes": False,
    "paranoid_file_checks": lambda: random.choice([0, 1, 1, 1]),
}

blackbox_simple_default_params = {
    "open_files": -1,
    "set_options_one_in": 0,
}

whitebox_simple_default_params = {}

cf_consistency_params = {
    "disable_wal": lambda: random.randint(0, 1),
    "reopen": 0,
    "test_cf_consistency": 1,
    # use small value for write_buffer_size so that RocksDB triggers flush
    # more frequently
    "write_buffer_size": 1024 * 1024,
    "enable_pipelined_write": lambda: random.randint(0, 1),
    # Snapshots are used heavily in this test mode, while they are incompatible
    # with compaction filter.
    "enable_compaction_filter": 0,
    "test_batches_snapshots": 0,
}

txn_params = {
    "use_txn" : 1,
    # Avoid lambda to set it once for the entire test
    "txn_write_policy": random.randint(0, 2),
    "unordered_write": random.randint(0, 1),
    # TODO: there is such a thing as transactions with WAL disabled. We should
    # cover that case.
    "disable_wal": 0,
    # OpenReadOnly after checkpoint is not currnetly compatible with WritePrepared txns
    "checkpoint_one_in": 0,
    # pipeline write is not currnetly compatible with WritePrepared txns
    "enable_pipelined_write": 0,
}

best_efforts_recovery_params = {
    "best_efforts_recovery": True,
    "skip_verifydb": True,
    "verify_db_one_in": 0,
    "continuous_verification_interval": 0,
}

blob_params = {
    "allow_setting_blob_options_dynamically": 1,
    # Enable blob files and GC with a 75% chance initially; note that they might still be
    # enabled/disabled during the test via SetOptions
    "enable_blob_files": lambda: random.choice([0] + [1] * 3),
    "min_blob_size": lambda: random.choice([0, 8, 16]),
    "blob_file_size": lambda: random.choice([1048576, 16777216, 268435456, 1073741824]),
    "blob_compression_type": lambda: random.choice(["none", "snappy", "lz4", "zstd"]),
    "enable_blob_garbage_collection": lambda: random.choice([0] + [1] * 3),
    "blob_garbage_collection_age_cutoff": lambda: random.choice([0.0, 0.25, 0.5, 0.75, 1.0]),
    "blob_garbage_collection_force_threshold": lambda: random.choice([0.5, 0.75, 1.0]),
    "blob_compaction_readahead_size": lambda: random.choice([0, 1048576, 4194304]),
}

ts_params = {
    "test_cf_consistency": 0,
    "test_batches_snapshots": 0,
    "user_timestamp_size": 8,
    "use_merge": 0,
    "use_full_merge_v1": 0,
    "use_txn": 0,
    "read_only": 0,
    "backup_one_in": 0,
    "secondary_catch_up_one_in": 0,
    "continuous_verification_interval": 0,
    "checkpoint_one_in": 0,
    "enable_blob_files": 0,
    "use_blob_db": 0,
    "enable_compaction_filter": 0,
    "ingest_external_file_one_in": 0,
    "use_block_based_filter": 0,
}

multiops_txn_default_params = {
    "test_cf_consistency": 0,
    "test_batches_snapshots": 0,
    "test_multi_ops_txns": 1,
    "use_txn": 1,
    "two_write_queues": lambda: random.choice([0, 1]),
    # TODO: enable write-prepared
    "disable_wal": 0,
    "use_only_the_last_commit_time_batch_for_recovery": lambda: random.choice([0, 1]),
    "clear_column_family_one_in": 0,
    "column_families": 1,
    "enable_pipelined_write": lambda: random.choice([0, 1]),
    # This test already acquires snapshots in reads
    "acquire_snapshot_one_in": 0,
    "backup_one_in": 0,
    "writepercent": 0,
    "delpercent": 0,
    "delrangepercent": 0,
    "customopspercent": 80,
    "readpercent": 5,
    "iterpercent": 15,
    "prefixpercent": 0,
    "verify_db_one_in": 1000,
    "continuous_verification_interval": 1000,
    "delay_snapshot_read_one_in": 3,
    # 65536 is the smallest possible value for write_buffer_size. Smaller
    # values will be sanitized to 65536 during db open. SetOptions currently
    # does not sanitize options, but very small write_buffer_size may cause
    # assertion failure in
    # https://github.com/facebook/rocksdb/blob/7.0.fb/db/memtable.cc#L117.
    "write_buffer_size": 65536,
    # flush more frequently to generate more files, thus trigger more
    # compactions.
    "flush_one_in": 1000,
    "key_spaces_path": setup_multiops_txn_key_spaces_file(),
}

multiops_wc_txn_params = {
    "txn_write_policy": 0,
    # TODO re-enable pipelined write. Not well tested atm
    "enable_pipelined_write": 0,
}

multiops_wp_txn_params = {
    "txn_write_policy": 1,
    "wp_snapshot_cache_bits": 1,
    # try small wp_commit_cache_bits, e.g. 0 once we explore storing full
    # commit sequence numbers in commit cache
    "wp_commit_cache_bits": 10,
    # pipeline write is not currnetly compatible with WritePrepared txns
    "enable_pipelined_write": 0,
    # OpenReadOnly after checkpoint is not currnetly compatible with WritePrepared txns
    "checkpoint_one_in": 0,
}

narrow_ops_per_thread = 50000

narrow_params = {
    "duration": 1800,
    "expected_values_dir": lambda: setup_expected_values_dir(),
    "max_key_len": 8,
    "value_size_mult": 8,
    "fail_if_options_file_error": True,
    "allow_concurrent_memtable_write": True,
    "reopen": 2,
    "log2_keys_per_lock": 1,
    "prefixpercent": 0,
    "prefix_size": -1,
    "ops_per_thread": narrow_ops_per_thread,
    "get_live_files_one_in": narrow_ops_per_thread,
    "acquire_snapshot_one_in": int(narrow_ops_per_thread / 4),
    "sync_wal_one_in": int(narrow_ops_per_thread / 2),
    "verify_db_one_in": int(narrow_ops_per_thread),
    "use_multiget": lambda: random.choice([0, 0, 0, 1]),
    "enable_compaction_filter": lambda: random.choice([0, 0, 0, 1]), 
    "use_multiget": lambda: random.choice([0, 0, 0, 1]), 
    "compare_full_db_state_snapshot": lambda: random.choice([0, 0, 0, 1]), 
    "use_merge": lambda: random.choice([0, 0, 0, 1]), 
    "nooverwritepercent": random.choice([0, 5, 20, 30, 40, 50, 95]), 
    "seed": int(time.time() * 1000000) & 0xffffffff,

    # below are params that are incompatible with current settings.
    "clear_column_family_one_in": 0,
    "get_sorted_wal_files_one_in": 0,
    "get_current_wal_file_one_in": 0,
    "continuous_verification_interval": 0,
    "destroy_db_initially": 0,
    "progress_reports": 0,
}


def store_ops_supplied(params):
    for k in supplied_ops:
        supplied_ops[k] = params.get(k, -1)


# make sure sum of ops == 100.
# value of -1 means that the op should be initialized. 
def randomize_operation_type_percentages(src_params):
    num_to_initialize = sum(1 for v in supplied_ops.values() if v == -1)
    
    params = {k: (v if v != -1 else 0) for k, v in supplied_ops.items()}

    ops_percent_sum = sum(params.get(k, 0) for k in supplied_ops)
    current_max = 100 - ops_percent_sum
    if ops_percent_sum > 100 or (num_to_initialize == 0 and ops_percent_sum != 100):
        raise ValueError("Error - Sum of ops percents should be 100")
    
    if num_to_initialize != 0:        
        for k , v in supplied_ops.items():
            if v != -1:
                continue
            
            if num_to_initialize == 1:
                params[k] = current_max
                break

            if k == "writepercent" and current_max > 60:
                params["writepercent"] = random.randint(20, 60)
            elif k == "delpercent" and current_max > 35:
                params["delpercent"] = random.randint(0, current_max - 35)
            elif k == "prefixpercent" and current_max >= 10:
                params["prefixpercent"] = random.randint(0, 10)
            elif k == "delrangepercent" and current_max >= 5:
                params["delrangepercent"] = random.randint(0, 5)
            else:
                params[k] = random.randint(0, current_max)
            
            current_max = current_max - params[k]
            num_to_initialize -= 1

    src_params.update(params)


def finalize_and_sanitize(src_params, counter):
    dest_params = dict([(k,  v() if callable(v) else v)
                        for (k, v) in src_params.items()])
    if dest_params.get("compression_max_dict_bytes") == 0:
        dest_params["compression_zstd_max_train_bytes"] = 0
        dest_params["compression_max_dict_buffer_bytes"] = 0
    if dest_params.get("compression_type") != "zstd":
        dest_params["compression_zstd_max_train_bytes"] = 0
    if dest_params.get("allow_concurrent_memtable_write", 1) == 1:
        dest_params["memtablerep"] = random.choice(
            ["skip_list", "speedb.HashSpdRepFactory"]
        )
    if dest_params["mmap_read"] == 1:
        dest_params["use_direct_io_for_flush_and_compaction"] = 0
        dest_params["use_direct_reads"] = 0
    if (dest_params["use_direct_io_for_flush_and_compaction"] == 1
            or dest_params["use_direct_reads"] == 1) and \
            not is_direct_io_supported(dest_params["db"]):
        if is_release_mode():
            print("{} does not support direct IO. Disabling use_direct_reads and "
                    "use_direct_io_for_flush_and_compaction.\n".format(
                        dest_params["db"]))
            dest_params["use_direct_reads"] = 0
            dest_params["use_direct_io_for_flush_and_compaction"] = 0
        else:
            dest_params["mock_direct_io"] = True

    # DeleteRange is not currnetly compatible with Txns and timestamp
    if (dest_params.get("test_batches_snapshots") == 1 or
        dest_params.get("use_txn") == 1 or
        dest_params.get("user_timestamp_size") > 0):
        dest_params["delpercent"] += dest_params["delrangepercent"]
        dest_params["delrangepercent"] = 0
    # Only under WritePrepared txns, unordered_write would provide the same guarnatees as vanilla rocksdb
    if dest_params.get("unordered_write", 0) == 1:
        dest_params["txn_write_policy"] = 1
        dest_params["allow_concurrent_memtable_write"] = 1
    if dest_params.get("disable_wal", 0) == 1:
        dest_params["atomic_flush"] = 1
        # The `DbStressCompactionFilter` can apply memtable updates to SST
        # files, which would be problematic without WAL since such updates are
        # expected to be lost in crash recoveries.
        dest_params["enable_compaction_filter"] = 0
        dest_params["sync"] = 0
        dest_params["write_fault_one_in"] = 0
    if dest_params.get("open_files", 1) != -1:
        # Compaction TTL and periodic compactions are only compatible
        # with open_files = -1
        dest_params["compaction_ttl"] = 0
        dest_params["periodic_compaction_seconds"] = 0
    if dest_params.get("compaction_style", 0) == 2:
        # Disable compaction TTL in FIFO compaction, because right
        # now assertion failures are triggered.
        dest_params["compaction_ttl"] = 0
        dest_params["periodic_compaction_seconds"] = 0
    if dest_params["partition_filters"] == 1:
        if dest_params["index_type"] != 2:
            dest_params["partition_filters"] = 0
        else:
            dest_params["use_block_based_filter"] = 0
    if dest_params["ribbon_starting_level"] < 999:
        dest_params["use_block_based_filter"] = 0
    if dest_params.get("atomic_flush", 0) == 1:
        # disable pipelined write when atomic flush is used.
        dest_params["enable_pipelined_write"] = 0
    if dest_params.get("sst_file_manager_bytes_per_sec", 0) == 0:
        dest_params["sst_file_manager_bytes_per_truncate"] = 0
    # test_batches_snapshots needs to stay const (either 1 or 0) throught
    # successive runs. this stops the next check (enable_compaction_filter) 
    # from switching its value.
    if (dest_params.get("test_batches_snapshots", 0) == 1 and 
        dest_params.get("enable_compaction_filter", 0) == 1):
        dest_params["enable_compaction_filter"] = 0
    if dest_params.get("read_only", 0) == 1:
        if counter == 0:
            dest_params["read_only"] = 0
        else:
            dest_params["readpercent"] += dest_params["writepercent"]
            dest_params["writepercent"] = 0
            dest_params["iterpercent"] += dest_params["delpercent"]
            dest_params["delpercent"] = 0
            dest_params["iterpercent"] += dest_params["delrangepercent"]
            dest_params["delrangepercent"] = 0    
    if dest_params.get("enable_compaction_filter", 0) == 1:
        # Compaction filter is incompatible with snapshots. Need to avoid taking
        # snapshots, as well as avoid operations that use snapshots for
        # verification.
        dest_params["acquire_snapshot_one_in"] = 0
        dest_params["compact_range_one_in"] = 0
        # Give the iterator ops away to reads.
        dest_params["readpercent"] += dest_params.get("iterpercent", 0)
        dest_params["iterpercent"] = 0
        dest_params["test_batches_snapshots"] = 0
    # this stops the ("prefix_size") == -1 check from changing the value
    # of test_batches_snapshots between runs.
    if (dest_params.get("prefix_size", 0) == -1 and 
        dest_params.get("test_batches_snapshots", 0) == 1):
        dest_params["prefix_size"] = 7
    if dest_params.get("prefix_size") == -1:
        dest_params["readpercent"] += dest_params.get("prefixpercent", 0)
        dest_params["prefixpercent"] = 0
        dest_params["test_batches_snapshots"] = 0
    if dest_params.get("test_batches_snapshots") == 0:
        dest_params["batch_protection_bytes_per_key"] = 0
    if (dest_params.get("prefix_size") == -1 and
        dest_params.get("memtable_whole_key_filtering") == 0):
        dest_params["memtable_prefix_bloom_size_ratio"] = 0
    if dest_params.get("two_write_queues") == 1:
        dest_params["enable_pipelined_write"] = 0
    return dest_params


def gen_cmd_params(args):
    params = {}

    params.update(default_params)
    if args.test_type == 'blackbox':
        params.update(blackbox_default_params)
    if args.test_type == 'whitebox':
        params.update(whitebox_default_params)
    if args.simple:
        params.update(simple_default_params)
        if args.test_type == 'blackbox':
            params.update(blackbox_simple_default_params)
        if args.test_type == 'whitebox':
            params.update(whitebox_simple_default_params)
    if args.cf_consistency:
        params.update(cf_consistency_params)
    if args.txn:
        params.update(txn_params)
    if args.test_best_efforts_recovery:
        params.update(best_efforts_recovery_params)
    if args.enable_ts:
        params.update(ts_params)
    if args.test_multiops_txn:
        params.update(multiops_txn_default_params)
        if args.write_policy == 'write_committed':
            params.update(multiops_wc_txn_params)
        elif args.write_policy == 'write_prepared':
            params.update(multiops_wp_txn_params)

    # Best-effort recovery and BlobDB are currently incompatible. Test BE recovery
    # if specified on the command line; otherwise, apply BlobDB related overrides
    # with a 10% chance.
    if (not args.test_best_efforts_recovery and
        not args.enable_ts and
        random.choice([0] * 9 + [1]) == 1):
        params.update(blob_params)

    for k, v in vars(args).items():
        if v is not None:
            params[k] = v
    
    if params["max_key_len"] == 0 or params["key_len_percent_dist"] == "0":
        generate_key_dist_and_len(params)

    return params


def gen_cmd(params, unknown_params, counter):
    finalzied_params = finalize_and_sanitize(params, counter)
    cmd = [stress_cmd] + [
        '--{0}={1}'.format(k, v)
        for k, v in [(k, finalzied_params[k]) for k in sorted(finalzied_params)]
        if k not in set(['test_type', 'simple', 'duration', 'interval',
                         'random_kill_odd', 'cf_consistency', 'txn',
                         'test_best_efforts_recovery', 'enable_ts',
                         'test_multiops_txn', 'write_policy', 'stress_cmd',
                         'disable_kill_points'])
        and v is not None] + unknown_params
    return cmd


# Inject inconsistency to db directory.
def inject_inconsistencies_to_db_dir(dir_path):
    files = os.listdir(dir_path)
    file_num_rgx = re.compile(r'(?P<number>[0-9]{6})')
    largest_fnum = 0
    for f in files:
        m = file_num_rgx.search(f)
        if m and not f.startswith('LOG'):
            largest_fnum = max(largest_fnum, int(m.group('number')))

    candidates = [
        f for f in files if re.search(r'[0-9]+\.sst', f)
    ]
    deleted = 0
    corrupted = 0
    for f in candidates:
        rnd = random.randint(0, 99)
        f_path = os.path.join(dir_path, f)
        if rnd < 10:
            os.unlink(f_path)
            deleted = deleted + 1
        elif 10 <= rnd and rnd < 30:
            with open(f_path, "a") as fd:
                fd.write('12345678')
            corrupted = corrupted + 1
    print('Removed %d table files' % deleted)
    print('Corrupted %d table files' % corrupted)

    # Add corrupted MANIFEST and SST
    for num in range(largest_fnum + 1, largest_fnum + 10):
        rnd = random.randint(0, 1)
        fname = ("MANIFEST-%06d" % num) if rnd == 0 else ("%06d.sst" % num)
        print('Write %s' % fname)
        with open(os.path.join(dir_path, fname), "w") as fd:
            fd.write("garbage")


DEADLY_SIGNALS = {
    signal.SIGABRT, signal.SIGBUS, signal.SIGFPE, signal.SIGILL, signal.SIGSEGV
}


def execute_cmd(cmd, timeout):
    child = subprocess.Popen(cmd, stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE)
    print("[%s] Running db_stress with pid=%d: %s\n\n"
          % (str(datetime.datetime.now()), child.pid, ' '.join(cmd)))

    try:
        outs, errs = child.communicate(timeout=timeout)
        hit_timeout = False
        if child.returncode < 0 and (-child.returncode in DEADLY_SIGNALS):
            msg = ("[%s] ERROR: db_stress (pid=%d) failed before kill: "
                   "exitcode=%d, signal=%s\n") % (
                    str(datetime.datetime.now()), child.pid, child.returncode,
                    signal.Signals(-child.returncode).name)
            print(outs)
            print(errs, file=sys.stderr)
            print(msg)
            raise SystemExit(msg)
        print("[%s] WARNING: db_stress (pid=%d) ended before kill: exitcode=%d\n"
              % (str(datetime.datetime.now()), child.pid, child.returncode))
    except subprocess.TimeoutExpired:
        hit_timeout = True
        child.kill()
        print("[%s] KILLED %d\n" % (str(datetime.datetime.now()), child.pid))
        outs, errs = child.communicate()

    return hit_timeout, child.returncode, outs.decode('utf-8'), errs.decode('utf-8')


# old copy of the db is kept at same src dir as new db. 
def copy_tree_and_remove_old(counter, dbname):
    dest = dbname + "_" + str(counter)
    shutil.copytree(dbname, dest)
    shutil.copytree(expected_values_dir, dest + "/" + "expected_values_dir")
    old_db = dbname + "_" + str(counter - 2)
    if counter > 1:
        shutil.rmtree(old_db, True)


def gen_narrow_cmd_params(args):
    params = {}
    params.update(narrow_params)
    # add these to avoid a key error in finalize_and_sanitize
    params["mmap_read"] = 0
    params["use_direct_io_for_flush_and_compaction"] = 0
    params["partition_filters"] = 0
    params["use_direct_reads"] = 0
    params["user_timestamp_size"] = 0
    params["ribbon_starting_level"] = 0

    for k, v in vars(args).items():
        if v is not None:
            params[k] = v
            
    return params


def narrow_crash_main(args, unknown_args):
    cmd_params = gen_narrow_cmd_params(args)
    dbname = get_dbname('narrow')
    exit_time = time.time() + cmd_params['duration']
    
    store_ops_supplied(cmd_params)

    print("Running narrow-crash-test\n")
    
    counter = 0
    
    while time.time() < exit_time:
        randomize_operation_type_percentages(cmd_params)
        cmd = gen_cmd(dict(cmd_params, **{'db': dbname}), unknown_args, counter)

        hit_timeout, retcode, outs, errs = execute_cmd(cmd, cmd_params['duration'])
        copy_tree_and_remove_old(counter, dbname)
        counter += 1

        for line in errs.splitlines():
            if line and not line.startswith('WARNING'):
                run_had_errors = True
                print('stderr has error message:')
                print('***' + line + '***')
        
        if retcode != 0:
            raise SystemExit('TEST FAILED. See kill option and exit code above!!!\n')

        time.sleep(2)  # time to stabilize before the next run

    shutil.rmtree(dbname, True)


# This script runs and kills db_stress multiple times. It checks consistency
# in case of unsafe crashes in RocksDB.
def blackbox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname('blackbox')
    exit_time = time.time() + cmd_params['duration']

    store_ops_supplied(cmd_params)

    print("Running blackbox-crash-test with \n"
          + "interval_between_crash=" + str(cmd_params['interval']) + "\n"
          + "total-duration=" + str(cmd_params['duration']) + "\n")
   
    counter = 0

    while time.time() < exit_time:
        randomize_operation_type_percentages(cmd_params)
        cmd = gen_cmd(dict(cmd_params, **{'db': dbname}), unknown_args, counter)

        hit_timeout, retcode, outs, errs = execute_cmd(cmd, cmd_params['interval'])
        copy_tree_and_remove_old(counter, dbname)
        counter+=1

        if not hit_timeout:
            print('Exit Before Killing')
            print('stdout:')
            print(outs)
            print('stderr:')
            print(errs)
            sys.exit(2)

        for line in errs.split('\n'):
            if line != '' and  not line.startswith('WARNING'):
                print('stderr has error message:')
                print('***' + line + '***')

        time.sleep(1)  # time to stabilize before the next run

        if args.test_best_efforts_recovery:
            inject_inconsistencies_to_db_dir(dbname)

        time.sleep(1)  # time to stabilize before the next run

    # we need to clean up after ourselves -- only do this on test success
    shutil.rmtree(dbname, True)
    for ctr in range(max(0, counter - 2), counter):
        shutil.rmtree('{}_{}'.format(dbname, ctr))


# This python script runs db_stress multiple times. Some runs with
# kill_random_test that causes rocksdb to crash at various points in code.
def whitebox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname('whitebox')

    cur_time = time.time()
    exit_time = cur_time + cmd_params['duration']
    half_time = cur_time + cmd_params['duration'] // 2

    store_ops_supplied(cmd_params)

    print("Running whitebox-crash-test with \n"
          + "total-duration=" + str(cmd_params['duration']) + "\n")

    total_check_mode = 4
    check_mode = 0
    kill_random_test = cmd_params['random_kill_odd']
    kill_mode = 0

    counter = 0

    while time.time() < exit_time:
        if cmd_params["disable_kill_points"]:
            check_mode = 3
        if check_mode == 0:
            additional_opts = {
                # use large ops per thread since we will kill it anyway
                "ops_per_thread": 100 * cmd_params['ops_per_thread'],
            }
            # run with kill_random_test, with three modes.
            # Mode 0 covers all kill points. Mode 1 covers less kill points but
            # increases change of triggering them. Mode 2 covers even less
            # frequent kill points and further increases triggering change.
            if kill_mode == 0:
                additional_opts.update({
                    "kill_random_test": kill_random_test,
                })
            elif kill_mode == 1:
                if cmd_params.get('disable_wal', 0) == 1:
                    my_kill_odd = kill_random_test // 50 + 1
                else:
                    my_kill_odd = kill_random_test // 10 + 1
                additional_opts.update({
                    "kill_random_test": my_kill_odd,
                    "kill_exclude_prefixes": "WritableFileWriter::Append,"
                    + "WritableFileWriter::WriteBuffered",
                })
            elif kill_mode == 2:
                # TODO: May need to adjust random odds if kill_random_test
                # is too small.
                additional_opts.update({
                    "kill_random_test": (kill_random_test // 5000 + 1),
                    "kill_exclude_prefixes": "WritableFileWriter::Append,"
                    "WritableFileWriter::WriteBuffered,"
                    "PosixMmapFile::Allocate,WritableFileWriter::Flush",
                })
            # Run kill mode 0, 1 and 2 by turn.
            kill_mode = (kill_mode + 1) % 3
        elif check_mode == 1:
            # normal run with universal compaction mode
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params['ops_per_thread'],
                "compaction_style": 1,
            }
            # Single level universal has a lot of special logic. Ensure we cover
            # it sometimes.
            if random.randint(0, 1) == 1:
                additional_opts.update({
                    "num_levels": 1,
                })
        elif check_mode == 2:
            # normal run with FIFO compaction mode
            # ops_per_thread is divided by 5 because FIFO compaction
            # style is quite a bit slower on reads with lot of files
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params['ops_per_thread'] // 5,
                "compaction_style": 2,
            }
        else:
            # normal run
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params['ops_per_thread'],
            }
        randomize_operation_type_percentages(cmd_params)
        cmd = gen_cmd(dict(cmd_params, **{'db': dbname}, **additional_opts), unknown_args, counter)

        # If the running time is 15 minutes over the run time, explicit kill and
        # exit even if white box kill didn't hit. This is to guarantee run time
        # limit, as if it runs as a job, running too long will create problems
        # for job scheduling or execution.
        # TODO detect a hanging condition. The job might run too long as RocksDB
        # hits a hanging bug.
        hit_timeout, retncode, stdoutdata, stderrdata = execute_cmd(
            cmd, exit_time - time.time() + 900)
        msg = ("check_mode={0}, kill option={1}, exitcode={2}\n".format(
               check_mode, additional_opts['kill_random_test'], retncode))

        print(msg)
        print(stdoutdata)
        print(stderrdata)
        
        copy_tree_and_remove_old(counter, dbname)
        counter+=1

        if hit_timeout:
            print("Killing the run for running too long")
            break

        expected = False
        if additional_opts['kill_random_test'] is None and (retncode == 0):
            # we expect zero retncode if no kill option
            expected = True
        elif additional_opts['kill_random_test'] is not None and retncode <= 0:
            # When kill option is given, the test MIGHT kill itself.
            # If it does, negative retncode is expected. Otherwise 0.
            expected = True

        if not expected:
            print("TEST FAILED. See kill option and exit code above!!!\n")
            sys.exit(1)

        stderrdata = stderrdata.lower()
        errorcount = (stderrdata.count('error') -
                      stderrdata.count('got errors 0 times'))
        print("#times error occurred in output is " + str(errorcount) +
                "\n")

        if (errorcount > 0):
            print("TEST FAILED. Output has 'error'!!!\n")
            sys.exit(2)
        if (stderrdata.find('fail') >= 0):
            print("TEST FAILED. Output has 'fail'!!!\n")
            sys.exit(2)

        # First half of the duration, keep doing kill test. For the next half,
        # try different modes.
        if time.time() > half_time:
            # we need to clean up after ourselves -- only do this on test
            # success
            shutil.rmtree(dbname, True)
            os.mkdir(dbname)
            global expected_values_dir
            if os.path.exists(expected_values_dir):
                shutil.rmtree(expected_values_dir)
            expected_values_dir = None
            check_mode = (check_mode + 1) % total_check_mode
            for ctr in range(max(0, counter - 2), counter):
                shutil.rmtree('{}_{}'.format(dbname, ctr))
            counter = 0

        time.sleep(1)  # time to stabilize after a kill


def bool_converter(v):
    s = v.lower().strip()
    if s in ('false', '0', 'no'):
        return False
    elif s in ('true', '1', 'yes'):
        return True
    raise ValueError('Failed to parse `%s` as a boolean value' % v)


def main():
    global stress_cmd

    parser = argparse.ArgumentParser(description="This script runs and kills \
        db_stress multiple times")
    parser.add_argument("test_type", choices=["blackbox", "whitebox", "narrow"])
    parser.add_argument("--simple", action="store_true")
    parser.add_argument("--cf_consistency", action='store_true')
    parser.add_argument("--txn", action='store_true')
    parser.add_argument("--test_best_efforts_recovery", action='store_true')
    parser.add_argument("--enable_ts", action='store_true')
    parser.add_argument("--test_multiops_txn", action='store_true')
    parser.add_argument("--write_policy", choices=["write_committed", "write_prepared"])
    parser.add_argument("--stress_cmd")

    all_params = dict(list(default_params.items())
                      + list(blackbox_default_params.items())
                      + list(whitebox_default_params.items())
                      + list(simple_default_params.items())
                      + list(blackbox_simple_default_params.items())
                      + list(whitebox_simple_default_params.items())
                      + list(blob_params.items())
                      + list(ts_params.items())
                      + list(supplied_ops.items())
                      + list(narrow_params.items())
                      + list(multiops_txn_default_params.items())
                      + list(multiops_wc_txn_params.items())
                      + list(multiops_wp_txn_params.items()))

    for k, v in all_params.items():
        t = type(v() if callable(v) else v)
        if t is bool:
            t = bool_converter
        parser.add_argument("--" + k, type=t)
    # unknown_args are passed directly to db_stress
    args, unknown_args = parser.parse_known_args()

    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is not None and not os.path.isdir(test_tmpdir):
        print('%s env var is set to a non-existent directory: %s' %
                (_TEST_DIR_ENV_VAR, test_tmpdir))
        sys.exit(1)

    if args.stress_cmd:
        stress_cmd = args.stress_cmd
    if args.test_type == 'blackbox':
        blackbox_crash_main(args, unknown_args)
    if args.test_type == 'whitebox':
        whitebox_crash_main(args, unknown_args)
    if args.test_type == 'narrow':
        narrow_crash_main(args, unknown_args)
    # Only delete the `expected_values_dir` if test passes
    if expected_values_dir and os.path.exists(expected_values_dir):
        shutil.rmtree(expected_values_dir)
    if multiops_txn_key_spaces_file is not None:
        os.remove(multiops_txn_key_spaces_file)


if __name__ == '__main__':
    main()
