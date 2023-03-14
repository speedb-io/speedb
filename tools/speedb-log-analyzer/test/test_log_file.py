import pytest
import itertools
import defs_and_utils
from database_options import DatabaseOptions
from log_file import LogFileMetadata, ParsedLog
from test.sample_log_info import SampleLogInfo, SampleRolledLogInfo
import test.test_utils as test_utils


def test_empty_md():
    md = LogFileMetadata([], 0)
    assert md.get_product_name() is None
    assert md.get_version() is None
    assert md.get_git_hash() is None
    assert md.get_db_session_id() is None
    assert md.get_start_time() is None
    assert md.get_end_time() is None

    with pytest.raises(defs_and_utils.ParsingAssertion):
        md.get_log_time_span_seconds()


def test_md_parse_product_and_version():
    lines =\
        ["2022/02/17-12:38:38.054710 7f574ef65f80 SpeeDB version: 6.11.4",
         "2022/02/17-12:38:38.054710 7f574ef65f80 RocksDB version: 6.11.5",
         "2022/02/17-12:38:38.054710 7f574ef65f80 SpeeDB version:",
         "2022/02/17-12:38:38.054710 7f574ef65f80 XXXXXX version: 6.11.6",
         "2022/02/17-12:38:38.054710 7f574ef65f80 version:6.11.4",
         ]

    expected_values = [
        ("SpeeDB", "6.11.4"),
        ("RocksDB", "6.11.5"),
        (None, None),
        ("XXXXXX", "6.11.6"),
        (None, None),
    ]

    for i, line in enumerate(lines):
        entry = test_utils.line_to_entry(line)
        md = LogFileMetadata([entry], 0)
        assert md.get_product_name() == expected_values[i][0]
        assert md.get_version() == expected_values[i][1]

    with pytest.raises(defs_and_utils.ParsingError):
        lines_with_duplicates = lines[0:2]
        entries_with_duplicates =\
            test_utils.lines_to_entries(lines_with_duplicates)
        md = LogFileMetadata(entries_with_duplicates, 0)


def test_md_parse_git_hash():
    lines =\
        ["2022/02/17-12:38:38.054710 7f574ef65f80 Git sha 123456",
         "2022/02/17-12:38:38.054710 7f574ef65f80 Git sha    123456",
         "2022/02/17-12:38:38.054710 7f574ef65f80 Git SHA 123456",
         "2022/02/17-12:38:38.054710 7f574ef65f80 Git sha XXXXX:123456",
         "2022/02/17-12:38:38.054710 7f574ef65f80 Git sha ",
         "2022/02/17-12:38:38.054710 7f574ef65f80 Git 123456",
         "2022/02/17-12:38:38.054710 7f574ef65f80 sha 123456",
         ]

    expected_values = [
        "123456",
        "123456",
        None,
        "XXXXX:123456",
        None,
        None,
        None,
    ]

    for i, line in enumerate(lines):
        entry = test_utils.line_to_entry(line)
        md = LogFileMetadata([entry], 0)
        assert md.get_git_hash() == expected_values[i]

    # Test duplicates
    with pytest.raises(defs_and_utils.ParsingError):
        lines_with_duplicates = lines[0:2]
        entries_with_duplicates =\
            test_utils.lines_to_entries(lines_with_duplicates)
        md = LogFileMetadata(entries_with_duplicates, 0)


def test_md_parse_db_session_id():
    lines =\
        ["2022/02/17-12:38:38.054710 7f574ef65f80 DB Session ID:  21GZXO5TD",
         "2022/02/17-12:38:38.054710 7f574ef65f80 DB Session ID: 21GZXO5TD",
         "2022/02/17-12:38:38.054710 7f574ef65f80 DB Session ID:21GZXO5TD",
         "2022/02/17-12:38:38.054710 7f574ef65f80 DB Session ID:",
         "2022/02/17-12:38:38.054710 7f574ef65f80 DB ID: 21GZXO5TD",
         "2022/02/17-12:38:38.054710 7f574ef65f80 Session ID: 21GZXO5TD",
         "2022/02/17-12:38:38.054710 7f574ef65f80 DB Session ID:",
         ]

    expected_values = [
        "21GZXO5TD",
        "21GZXO5TD",
        "21GZXO5TD",
        None,
        None,
        None,
        None,
    ]

    for i, line in enumerate(lines):
        entry = test_utils.line_to_entry(line)
        md = LogFileMetadata([entry], 0)
        assert md.get_db_session_id() == expected_values[i]

    # Test duplicates
    with pytest.raises(defs_and_utils.ParsingError):
        lines_with_duplicates = lines[0:2]
        entries_with_duplicates =\
            test_utils.lines_to_entries(lines_with_duplicates)
        md = LogFileMetadata(entries_with_duplicates, 0)


def test_parse_md_valid_combinations():
    lines = [
        "2022/02/17-12:38:38.054710 f65f80 SpeeDB version: 6.11.4",
        "2022/02/17-12:38:38.054710 f65f80 DB Session ID: 21GZXO5TD9VAIRA",
        "2022/02/17-12:38:38.054710 7f574ef65f80 Git sha 123456",
    ]

    # Test all combinations of line orderings. Parsing should not be affected
    for i, lines_permutation in enumerate(list(itertools.permutations(lines))):
        entries = test_utils.lines_to_entries(lines_permutation)
        md = LogFileMetadata(entries, 0)
        assert md.get_product_name() == "SpeeDB"
        assert md.get_version() == "6.11.4"
        assert md.get_git_hash() == "123456"
        assert md.get_db_session_id() == "21GZXO5TD9VAIRA"


def test_parse_log_to_entries():
    lines = '''2022/04/17-14:13:10.724683 7f4a9fdff700 Entry 1
    2022/04/17-14:14:10.724683 7f4a9fdff700 Entry 2
    Entry 2 Continuation 1

    
    2022/04/17-14:14:20.724683 7f4a9fdff700 Entry 3
    
    Entry 3 Continuation 1
    '''.splitlines() # noqa

    entries = ParsedLog.parse_log_to_entries("DummyPath", lines[:1])
    assert len(entries) == 1
    assert entries[0].get_msg() == "Entry 1"

    entries = ParsedLog.parse_log_to_entries("DummyPath", lines[:2])
    assert len(entries) == 2
    assert entries[0].get_msg() == "Entry 1"
    assert entries[1].get_msg() == "Entry 2"

    entries = ParsedLog.parse_log_to_entries("DummyPath", lines)
    assert len(entries) == 3

    assert entries[1].get_msg_lines() == ["Entry 2",
                                          "Entry 2 Continuation 1",
                                          "",
                                          ""]
    assert entries[1].get_msg() == "Entry 2\nEntry 2 Continuation 1"

    assert entries[2].get_msg_lines() == ["Entry 3",
                                          "",
                                          "Entry 3 Continuation 1",
                                          ""]
    assert entries[2].get_msg() == "Entry 3\n\nEntry 3 Continuation 1"


def test_parse_metadata():
    lines = '''2022/11/24-15:58:04.758352 32819 RocksDB version: 7.2.2
    2022/11/24-15:58:04.758397 32819 Git sha 35f6432643807267f210eda9e9388a80ea2ecf0e
    2022/11/24-15:58:04.758398 32819 Compile date
    2022/11/24-15:58:04.758402 32819 DB SUMMARY
    2022/11/24-15:58:04.758403 32819 DB Session ID:  V90YQ8JY6T5E5H2ES6LK
    2022/11/24-15:58:04.759056 32819 CURRENT file:  CURRENT
    2022/11/24-15:58:04.759058 32819 IDENTITY file:  IDENTITY
    2022/11/24-15:58:04.759060 32819 MANIFEST file:  MANIFEST-025591 size: 439474 Bytes
    2022/11/24-15:58:04.759061 32819 SST files in /data/ dir, Total Num: 1498,"
    2022/11/24-15:58:04.759062 32819 Write Ahead Log file in /data/: 028673.log
    '''.splitlines() # noqa

    entries = test_utils.lines_to_entries(lines)

    metadata = LogFileMetadata(entries, start_entry_idx=0)

    assert metadata.get_product_name() == "RocksDB"
    assert metadata.get_version() == "7.2.2"
    assert metadata.get_git_hash() == \
           "35f6432643807267f210eda9e9388a80ea2ecf0e"

    assert metadata.get_start_time() == "2022/11/24-15:58:04.758352"

    with pytest.raises(defs_and_utils.ParsingAssertion):
        metadata.get_log_time_span_seconds()

    assert str(metadata) == \
           "LogFileMetadata: Start:2022/11/24-15:58:04.758352, End:UNKNOWN"

    metadata.set_end_time("2022/11/24-16:08:04.758352")
    assert metadata.get_end_time() == "2022/11/24-16:08:04.758352"
    assert metadata.get_log_time_span_seconds() == 600

    assert str(metadata) == \
           "LogFileMetadata: Start:2022/11/24-15:58:04.758352, " \
           "End:2022/11/24-16:08:04.758352"


def test_parse_metadata1():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)
    metadata = parsed_log.get_metadata()

    assert metadata.get_product_name() == SampleLogInfo.PRODUCT_NAME
    assert metadata.get_version() == SampleLogInfo.VERSION
    assert metadata.get_git_hash() == SampleLogInfo.GIT_HASH

    assert metadata.get_start_time() == SampleLogInfo.START_TIME
    assert metadata.get_end_time() == SampleLogInfo.END_TIME

    expected_time_span = \
        (defs_and_utils.parse_date_time(SampleLogInfo.END_TIME) -
         defs_and_utils.parse_date_time(SampleLogInfo.START_TIME)).seconds
    assert metadata.get_log_time_span_seconds() == expected_time_span


def test_parse_options():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)
    assert parsed_log.get_cf_names() == SampleLogInfo.CF_NAMES

    actual_db_options = parsed_log.get_database_options()
    assert actual_db_options.are_db_wide_options_set()
    assert actual_db_options.get_cfs_names() == parsed_log.get_cf_names()

    # Assuming LogFileOptionsParser is fully tested and may be used
    expected_db_options = DatabaseOptions()
    expected_db_options.set_db_wide_options(SampleLogInfo.DB_WIDE_OPTIONS_DICT)
    for i in range(len(SampleLogInfo.CF_NAMES)):
        expected_db_options.set_cf_options(
            SampleLogInfo.CF_NAMES[i],
            SampleLogInfo.OPTIONS_DICTS[i],
            SampleLogInfo.TABLE_OPTIONS_DICTS[i])

    actual_db_wide_options = actual_db_options.get_db_wide_options()
    expected_db_wide_options = expected_db_options.get_db_wide_options()
    assert expected_db_wide_options == actual_db_wide_options

    for i in range(len(SampleLogInfo.CF_NAMES)):
        cf_name = SampleLogInfo.CF_NAMES[i]
        actual_options = actual_db_options.get_cf_options(cf_name)
        expected_options = expected_db_options.get_cf_options(cf_name)
        assert expected_options == actual_options


def test_parse_options_in_rolled_log():
    parsed_log = test_utils.create_parsed_log(SampleRolledLogInfo.FILE_PATH)
    assert parsed_log.get_cf_names() == SampleRolledLogInfo.CF_NAMES
    assert parsed_log.get_auto_generated_cf_names() ==\
           SampleRolledLogInfo.AUTO_GENERATED_CF_NAMES

    actual_db_options = parsed_log.get_database_options()
    assert actual_db_options.are_db_wide_options_set()

    expected_cfs_names_with_options = ["default"] + \
        SampleRolledLogInfo.AUTO_GENERATED_CF_NAMES
    assert actual_db_options.get_cfs_names() == \
           expected_cfs_names_with_options

    # Assuming LogFileOptionsParser is fully tested and may be used
    expected_db_options = DatabaseOptions()
    expected_db_options.set_db_wide_options(SampleLogInfo.DB_WIDE_OPTIONS_DICT)
    for i in range(len(expected_cfs_names_with_options)):
        expected_db_options.set_cf_options(
            expected_cfs_names_with_options[i],
            SampleLogInfo.OPTIONS_DICTS[i],
            SampleLogInfo.TABLE_OPTIONS_DICTS[i])

    actual_db_wide_options = actual_db_options.get_db_wide_options()
    expected_db_wide_options = expected_db_options.get_db_wide_options()
    assert expected_db_wide_options == actual_db_wide_options

    for i in range(len(expected_cfs_names_with_options)):
        cf_name = expected_cfs_names_with_options[i]
        actual_options = actual_db_options.get_cf_options(cf_name)
        expected_options = expected_db_options.get_cf_options(cf_name)
        assert expected_options == actual_options


def test_parse_warns():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)

    warns_mngr = parsed_log.get_warnings_mngr()
    assert warns_mngr.get_total_num_warns() == 1


def test_parse_db_wide_stats():
    parsed_log = test_utils.create_parsed_log(SampleLogInfo.FILE_PATH)

    mngr = parsed_log.get_stats_mngr()
    db_wide_stats_mngr = mngr.get_db_wide_stats_mngr()
    assert db_wide_stats_mngr.get_stalls_entries() == \
           SampleLogInfo.DB_WIDE_STALLS_ENTRIES


def test_empty_log_file():
    with pytest.raises(defs_and_utils.ParsingError) as e:
        ParsedLog("DummyPath", [])
    assert str(e.value) == "[DummyPath (line:2)] - Empty File"


def test_unexpected_1st_log_line():
    with pytest.raises(defs_and_utils.ParsingError) as e:
        ParsedLog("DummyPath", ["Dummy Line", "Another Dummy Line"])

    expected_msg =\
        "[DummyPath (line:2)] - Unexpected first log line:\nDummy Line"
    assert str(e.value) == expected_msg
