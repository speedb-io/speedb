import defs_and_utils
from log_entry import LogEntry
from stats_mngr import DbWideStatsMngr, CompactionStatsMngr, BlobStatsMngr, \
    CfFileHistogramStatsMngr, BlockCacheStatsMngr, \
    StatsMngr, parse_uptime_line, StatsCountersAndHistogramsMngr
from test.test_utils import lines_to_entries


NUM_LINES = 381
STATS_DUMP_IDX = 0
DB_STATS_IDX = 2
COMPACTION_STATS_DEFAULT_PER_LEVEL_IDX = 11
COMPACTION_STATS_DEFAULT_BY_PRIORITY_IDX = 22
BLOB_STATS_IDX = 27
CF_NO_FILE_HISTOGRAM_DEFAULT_IDX = 29
BLOCK_CACHE_DEFAULT_IDX = 38
CF_FILE_READ_LATENCY_IDX = 41
STATISTICS_COUNTERS_IDX = 138
ONE_PAST_STATS_IDX = 377

EMPTY_LINE1 = ''
EMPTY_LINE2 = '                      '


def read_sample_stats_file():
    f = open("input_files/LOG_sample_stats.txt")
    lines = f.readlines()
    assert len(lines) == NUM_LINES
    return lines


def test_parse_uptime_line():
    line1 = "Uptime(secs): 4.8 total, 8.4 interval"
    assert (4.8, 8.4) == parse_uptime_line(line1)

    line2 = "Uptime(secs): 4.8 total, XXX 8.4 interval"
    assert parse_uptime_line(line2, allow_mismatch=True) is None


def test_is_db_wide_stats_start_line():
    assert DbWideStatsMngr.is_start_line("** DB Stats **")
    assert DbWideStatsMngr.is_start_line("** DB Stats **      ")
    assert not DbWideStatsMngr.is_start_line("** DB Stats **   DUMMY TEXT")
    assert not DbWideStatsMngr.is_start_line("** DB XXX Stats **")


def test_is_compaction_stats_start_line():
    line1 = "** Compaction Stats [default] **"
    assert CompactionStatsMngr.is_start_line(line1)
    assert CompactionStatsMngr.parse_start_line(line1) == "default"

    line2 = "** Compaction Stats [col-family] **       "
    assert CompactionStatsMngr.is_start_line(line2)
    assert CompactionStatsMngr.parse_start_line(line2) == "col-family"

    line3 = "       ** Compaction Stats [col-family] **"
    assert CompactionStatsMngr.is_start_line(line3)
    assert CompactionStatsMngr.parse_start_line(line3) == "col-family"

    line4 = "** Compaction Stats    [col-family]     **     "
    assert CompactionStatsMngr.is_start_line(line4)
    assert CompactionStatsMngr.parse_start_line(line4) == "col-family"

    line5 = "** Compaction XXX Stats  [col-family] **"
    assert not CompactionStatsMngr.is_start_line(line5)


def test_is_blob_stats_start_line():
    line1 =\
        'Blob file count: 0, total size: 0.0 GB, garbage size: 0.0 GB,' \
        ' space amp: 0.0'

    assert BlobStatsMngr.is_start_line(line1)
    assert (0, 0.0, 0.0, 0.0) == BlobStatsMngr.parse_blob_stats_line(line1)

    line2 =\
        'Blob file count: 100, total size: 1.5 GB, garbage size: 3.5 GB,' \
        ' space amp: 0.2'
    assert BlobStatsMngr.is_start_line(line2)
    assert (100, 1.5, 3.5, 0.2) == BlobStatsMngr.parse_blob_stats_line(line2)


def test_is_cf_file_histogram_stats_start_line():
    line1 = "** File Read Latency Histogram By Level [default] **"
    assert CfFileHistogramStatsMngr.is_start_line(line1)
    assert CfFileHistogramStatsMngr.parse_start_line(line1) == "default"

    line2 = "** File Read Latency Histogram By Level [col-family] **       "
    assert CfFileHistogramStatsMngr.is_start_line(line2)
    assert CfFileHistogramStatsMngr.parse_start_line(line2) == "col-family"

    line3 = "       ** File Read Latency Histogram By Level [col-family] **"
    assert CfFileHistogramStatsMngr.is_start_line(line3)
    assert CfFileHistogramStatsMngr.parse_start_line(line3) == "col-family"

    line4 = \
        "** File Read Latency Histogram By Level    [col-family]     **     "
    assert CfFileHistogramStatsMngr.is_start_line(line4)
    assert CfFileHistogramStatsMngr.parse_start_line(line4) == "col-family"

    line5 =\
        "** File Read Latency Histogram XXX By Level Stats  [col-family] **"
    assert not CfFileHistogramStatsMngr.is_start_line(line5)


def test_is_block_cache_stats_start_line():
    line1 = 'Block cache LRUCache@0x5600bb634770#32819 capacity: 8.00 MB ' \
            'collections: 1 last_copies: 0 last_secs: 4.9e-05 secs_since: 0'

    line2 = \
        'Block cache entry stats(count,size,portion): ' \
        'Misc(3,8.12 KB, 0.0991821%)'

    assert BlockCacheStatsMngr.is_start_line(line1)
    assert not BlockCacheStatsMngr.is_start_line(line2)


def test_stats_counter_and_histograms_is_your_entry():
    lines = \
    '''2022/11/24-15:58:09.512106 32851 [/db_impl/db_impl.cc:761] STATISTICS:
    rocksdb.block.cache.miss COUNT : 61
    '''.splitlines() # noqa

    entry = LogEntry(0, lines[0])
    entry.add_line(lines[1], True)

    assert StatsCountersAndHistogramsMngr.is_your_entry(entry)


def test_find_next_start_line_in_db_stats():
    lines = read_sample_stats_file()
    assert DbWideStatsMngr.is_start_line(lines[DB_STATS_IDX])

    expected_next_line_idxs = [COMPACTION_STATS_DEFAULT_PER_LEVEL_IDX,
                               COMPACTION_STATS_DEFAULT_BY_PRIORITY_IDX,
                               BLOB_STATS_IDX,
                               CF_NO_FILE_HISTOGRAM_DEFAULT_IDX,
                               BLOCK_CACHE_DEFAULT_IDX,
                               CF_FILE_READ_LATENCY_IDX]
    expected_next_types = [StatsMngr.StatsType.COMPACTION,
                           StatsMngr.StatsType.COMPACTION,
                           StatsMngr.StatsType.BLOB,
                           StatsMngr.StatsType.CF_NO_FILE,
                           StatsMngr.StatsType.BLOCK_CACHE,
                           StatsMngr.StatsType.CF_FILE_HISTOGRAM]

    expected_next_cf_names = ["default", "default", None, None,
                              None, "CF1"]

    line_idx = DB_STATS_IDX
    stats_type = StatsMngr.StatsType.DB_WIDE
    for i, expected_next_line_idx in enumerate(expected_next_line_idxs):
        next_line_idx, next_stats_type, next_cf_name =\
            StatsMngr.find_next_start_line_in_db_stats(lines,
                                                       line_idx,
                                                       stats_type)
        assert (next_line_idx > line_idx) or (next_stats_type is None)
        assert next_line_idx == expected_next_line_idx
        assert next_stats_type == expected_next_types[i]
        assert next_cf_name == expected_next_cf_names[i]

        line_idx = next_line_idx
        stats_type = next_stats_type


def test_blob_stats_mngr():
    blob_line = \
        'Blob file count: 10, total size: 1.5 GB, garbage size: 2.0 GB, ' \
        'space amp: 4.0'
    blob_lines = [blob_line, EMPTY_LINE2]
    time = '2022/11/24-15:58:09.511260 32851'
    cf = "cf1"
    mngr = BlobStatsMngr()
    mngr.add_lines(time, cf, blob_lines)

    expected_blob_entries =\
        {time: {"File Count": 10,
                "Total Size": float(1.5 * 2**30),
                "Garbage Size": float(2 * 2**30),
                "Space Amp": 4.0}}

    assert mngr.get_cf_stats(cf) == expected_blob_entries


def test_block_cache_stats_mngr():
    lines = \
    '''Block cache LRUCache@0x5600bb634770#32819 capacity: 8.00 MB collections: 1 last_copies: 0 last_secs: 4.9e-05 secs_since: 0
    Block cache entry stats(count,size,portion): Misc(3,8.12 KB,0.0991821%)
    '''.splitlines() # noqa

    time = '2022/11/24-15:58:09.511260 32851'
    cf = "cf1"
    mngr = BlockCacheStatsMngr()
    mngr.add_lines(time, cf, lines)


def test_counters_and_histograms_mngr():
    entry_lines = \
    '''2022/11/24-15:58:09.512106 32851 [/db_impl/db_impl.cc:761] STATISTICS:
     rocksdb.block.cache.miss COUNT : 61
    rocksdb.block.cache.hit COUNT : 0
    rocksdb.block.cache.add COUNT : 0
    rocksdb.block.cache.data.miss COUNT : 61
    rocksdb.db.get.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
    rocksdb.read.block.compaction.micros P50 : 1.321429 P95 : 3.650000 P99 : 17.000000 P100 : 17.000000 COUNT : 67 SUM : 140
    rocksdb.blobdb.next.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0'''.splitlines() # noqa
    entry = LogEntry(0, entry_lines[0])
    for line in entry_lines[1:]:
        entry.add_line(line)

    mngr = StatsCountersAndHistogramsMngr()
    mngr.add_entry(entry.all_lines_added())

    assert mngr.get_counter_entries("rocksdb.block.cache.hit") == \
           [{'time': '2022/11/24-15:58:09.512106',
             'value': 0}]

    assert mngr.get_counter_entries("rocksdb.block.cache.data.miss") == \
           [{'time': '2022/11/24-15:58:09.512106',
            'value': 61}]

    assert mngr.get_counter_entries('XXXXX') == {}

    assert mngr.get_counters_names() == ['rocksdb.block.cache.miss',
                                         'rocksdb.block.cache.hit',
                                         'rocksdb.block.cache.add',
                                         'rocksdb.block.cache.data.miss']

    assert mngr.get_counters_times() == ['2022/11/24-15:58:09.512106']

    expected_histogram = \
        [{'time': '2022/11/24-15:58:09.512106',
         'values': {'Average': 0.48,
                    'Count': 67,
                    'P100': 17.0,
                    'P50': 1.321429,
                    'P95': 3.65,
                    'P99': 17.0,
                    'Sum': 140}}]

    assert mngr.get_histogram_entries(
        'rocksdb.read.block.compaction.micros') == expected_histogram
    assert mngr.get_histogram_entries('rocksdb.db.get.micros') == {}
    assert mngr.get_histogram_entries('YYYY') == {}


def test_stats_mngr():
    lines = read_sample_stats_file()
    entries = lines_to_entries(lines)

    mngr = StatsMngr()

    mngr.try_adding_entries(entries, start_entry_idx=0) == (True, 3)


def test_compaction_stats_mngr():
    lines_level = \
    '''** Compaction Stats [default] **
    Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop Rblob(GB) Wblob(GB)
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
      L0      1/0   149.99 MB   0.6      0.0     0.0      0.0       0.1      0.1       0.0   1.0      0.0    218.9      0.69              0.00         1    0.685       0      0       0.0       0.0
      L1      5/0   271.79 MB   1.1      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
      L2     50/1    2.73 GB   1.1      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
      L3    421/6   24.96 GB   1.0      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
      L4   1022/0   54.33 GB   0.2      0.0     0.0      0.0       0.0      0.0       0.0   0.0      0.0      0.0      0.00              0.00         0    0.000       0      0       0.0       0.0
     Sum   1499/7   82.43 GB   0.0      0.0     0.0      0.0       0.1      0.1       0.0   1.0      0.0    218.9      0.69              0.00         1    0.685       0      0       0.0       0.0
     Int      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.1      0.1       0.0   1.0      0.0    218.9      0.69              0.00         1    0.685       0      0       0.0       0.0
    '''.splitlines() # noqa

    lines_priority = \
    '''** Compaction Stats [CF1] **
    Priority    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop Rblob(GB) Wblob(GB)
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    User      0/0    0.00 KB   0.0      0.0     0.0      0.0       0.1      0.1       0.0   0.0      0.0    218.9      0.69              0.00         1    0.685       0      0       0.0       0.0
    '''.splitlines() # noqa

    lines_level = [line.strip() for line in lines_level]
    lines_priority = [line.strip() for line in lines_priority]

    time = "2022/11/24-15:58:09.511260"
    mngr = CompactionStatsMngr()

    assert mngr.get_cf_size_bytes("default") == 0

    mngr.add_lines(time, "default", lines_level)
    mngr.add_lines(time, "CF1", lines_priority)

    assert mngr.get_cf_size_bytes("default") == \
           defs_and_utils.get_value_by_unit("82.43", "GB")
    assert mngr.get_cf_size_bytes("CF1") == 0
