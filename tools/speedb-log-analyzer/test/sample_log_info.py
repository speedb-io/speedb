from datetime import timedelta


class SampleLogInfo:
    FILE_PATH = "input_files/LOG_sample.txt"
    START_TIME = "2022/04/17-14:13:10.723796"
    END_TIME = "2022/04/17-14:14:32.645120"
    PRODUCT_NAME = "SpeeDB"
    GIT_HASH = "UNKNOWN:0a396684d6c08f6fe4a37572c0429d91176c51d1"
    VERSION = "6.22.1"
    NUM_ENTRIES = 73
    CF_NAMES = ['default', '_sample/CF_1', '_sample/CF-2', '']
    DB_WIDE_OPTIONS_START_ENTRY_IDX = 7
    SUPPORT_INFO_START_ENTRY_IDX = 15

    NUM_WARNS = 1

    OPTIONS_ENTRIES_INDICES = [21, 33, 42, 50]
    TABLE_OPTIONS_ENTRIES_INDICES = [24, 38, 45, 55]

    DB_WIDE_OPTIONS_DICT = {
        'error_if_exists': '0',
        'create_if_missing': '1',
        'db_log_dir': '',
        'wal_dir': '',
        'track_and_verify_wals_in_manifest': '0',
        'env': '0x7f4a9117d5c0',
        'fs': 'Posix File System',
        'info_log': '0x7f4af4020bf0'
    }

    DEFAULT_OPTIONS_DICT = {
        'comparator': 'leveldb.BytewiseComparator',
        'merge_operator': 'StringAppendTESTOperator',
        'write_buffer_size': '67108864',
        'max_write_buffer_number': '2',
        'ttl': '2592000'
    }

    SAMPLE_CF1_OPTIONS_DICT = {
        'comparator': 'leveldb.BytewiseComparator-XXX',
        'merge_operator': 'StringAppendTESTOperator-XXX',
        'compaction_filter': 'None',
        'table_factory': 'BlockBasedTable',
        'write_buffer_size': '67108864',
        'max_write_buffer_number': '2',
    }

    SAMPLE_CF2_OPTIONS_DICT = {
        'comparator': 'leveldb.BytewiseComparator-YYY',
        'table_factory': 'BlockBasedTable-YYY',
        'write_buffer_size': '123467108864',
        'max_write_buffer_number': '10',
        'compression': 'Snappy'
    }

    EMPTY_CF_OPTIONS_DICT = {
        'comparator': 'leveldb.BytewiseComparator-ZZZ',
        'merge_operator': 'StringAppendTESTOperator-ZZZ',
        'compaction_filter': 'None',
        'table_factory': 'BlockBasedTable'
    }

    DEFAULT_TABLE_OPTIONS_DICT = {
        'flush_block_policy_factory':
            'FlushBlockBySizePolicyFactory (0x7f4af401f570)',
        'cache_index_and_filter_blocks': '1',
        'cache_index_and_filter_blocks_with_high_priority': '1',
        'pin_l0_filter_and_index_blocks_in_cache': '1',
        'pin_top_level_index_and_filter': '1',
        'metadata_cache_options': '',
        'top_level_index_pinning': '0',
        'block_cache_options': '',
        'capacity': '209715200',
        'block_cache_compressed': '(nil)',
        'prepopulate_block_cache': '0'}

    SAMPLE_CF1_TABLE_OPTIONS_DICT = {
        'flush_block_policy_factory':
            'FlushBlockBySizePolicyFactory (0x7f4af4031090)',
        'pin_top_level_index_and_filter': '1',
        'metadata_cache_options': '',
        'unpartitioned_pinning': '3',
        'no_block_cache': '0',
        'block_cache': '0x7f4bc07214d0',
        'block_cache_options': '',
        'memory_allocator': 'None',
        'high_pri_pool_ratio': '0.100',
        'block_cache_compressed': '(nil)'}

    SAMPLE_CF2_TABLE_OPTIONS_DICT = {
        'flush_block_policy_factory':
            'FlushBlockBySizePolicyFactory (0x7f4af4091b90)',
        'cache_index_and_filter_blocks': '1',
        'cache_index_and_filter_blocks_with_high_priority': '1'
    }

    EMPTY_CF_TABLE_OPTIONS_DICT = {
        'flush_block_policy_factory':
            'FlushBlockBySizePolicyFactory (0x7f4af4030f30)',
        'pin_top_level_index_and_filter': '1'}

    OPTIONS_DICTS = [
        DEFAULT_OPTIONS_DICT,
        SAMPLE_CF1_OPTIONS_DICT,
        SAMPLE_CF2_OPTIONS_DICT,
        EMPTY_CF_OPTIONS_DICT
    ]

    TABLE_OPTIONS_DICTS = [
        DEFAULT_TABLE_OPTIONS_DICT,
        SAMPLE_CF1_TABLE_OPTIONS_DICT,
        SAMPLE_CF2_TABLE_OPTIONS_DICT,
        EMPTY_CF_TABLE_OPTIONS_DICT
    ]

    DB_STATS_ENTRY_TIME = "2022/04/17-14:14:28.645150"
    CUMULATIVE_DURATION = \
        timedelta(hours=12, minutes=10, seconds=56, milliseconds=123)
    INTERVAL_DURATION = \
        timedelta(hours=45, minutes=34, seconds=12, milliseconds=789)
    DB_WIDE_STALLS_ENTRIES = \
        {DB_STATS_ENTRY_TIME: {"cumulative_duration": CUMULATIVE_DURATION,
                               "cumulative_percent": 98.7,
                               "interval_duration": INTERVAL_DURATION,
                               "interval_percent": 12.3}}


class SampleRolledLogInfo:
    FILE_PATH = "input_files/Rolled_LOG_sample.txt"
    START_TIME = "2022/04/17-14:13:10.723796"
    END_TIME = "2022/04/17-14:14:32.645120"
    PRODUCT_NAME = "SpeeDB"
    GIT_HASH = "UNKNOWN:0a396684d6c08f6fe4a37572c0429d91176c51d1"
    VERSION = "6.22.1"
    NUM_ENTRIES = 60
    CF_NAMES = ['default', 'Unknown-1', 'Unknown-2', '']
    DB_WIDE_OPTIONS_START_ENTRY_IDX = 7
    SUPPORT_INFO_START_ENTRY_IDX = 15

    NUM_WARNS = 1

    OPTIONS_ENTRIES_INDICES = [19, 25, 32, 38]
    TABLE_OPTIONS_ENTRIES_INDICES = [21, 29, 34, 42]

    DB_WIDE_OPTIONS_DICT = SampleLogInfo.DB_WIDE_OPTIONS_DICT
    DEFAULT_OPTIONS_DICT = SampleLogInfo.DEFAULT_OPTIONS_DICT
    SAMPLE_CF1_OPTIONS_DICT = SampleLogInfo.SAMPLE_CF1_OPTIONS_DICT
    SAMPLE_CF2_OPTIONS_DICT = SampleLogInfo.SAMPLE_CF2_OPTIONS_DICT
    EMPTY_CF_OPTIONS_DICT = SampleLogInfo.EMPTY_CF_OPTIONS_DICT
    DEFAULT_TABLE_OPTIONS_DICT = SampleLogInfo.DEFAULT_TABLE_OPTIONS_DICT
    SAMPLE_CF1_TABLE_OPTIONS_DICT = SampleLogInfo.SAMPLE_CF1_TABLE_OPTIONS_DICT
    SAMPLE_CF2_TABLE_OPTIONS_DICT = SampleLogInfo.SAMPLE_CF2_TABLE_OPTIONS_DICT
    EMPTY_CF_TABLE_OPTIONS_DICT = SampleLogInfo.EMPTY_CF_TABLE_OPTIONS_DICT

    OPTIONS_DICTS = [
        DEFAULT_OPTIONS_DICT,
        SAMPLE_CF1_OPTIONS_DICT,
        SAMPLE_CF2_OPTIONS_DICT,
        EMPTY_CF_OPTIONS_DICT
    ]

    TABLE_OPTIONS_DICTS = [
        DEFAULT_TABLE_OPTIONS_DICT,
        SAMPLE_CF1_TABLE_OPTIONS_DICT,
        SAMPLE_CF2_TABLE_OPTIONS_DICT,
        EMPTY_CF_TABLE_OPTIONS_DICT
    ]

    DB_STATS_ENTRY_TIME = "2022/04/17-14:14:28.645150"
    CUMULATIVE_DURATION = \
        timedelta(hours=12, minutes=10, seconds=56, milliseconds=123)
    INTERVAL_DURATION = \
        timedelta(hours=45, minutes=34, seconds=12, milliseconds=789)
    DB_WIDE_STALLS_ENTRIES = \
        {DB_STATS_ENTRY_TIME: {"cumulative_duration": CUMULATIVE_DURATION,
                               "cumulative_percent": 98.7,
                               "interval_duration": INTERVAL_DURATION,
                               "interval_percent": 12.3}}
