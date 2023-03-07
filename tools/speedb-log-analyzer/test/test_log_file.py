import defs_and_utils
from database_options import DatabaseOptions
from log_file import ParsedLog
from test.sample_log_info import SampleInfo
import test.test_utils as test_utils


def test_parse_log_to_entries():
    log_lines = test_utils.read_file(SampleInfo.FILE_PATH)
    entries = ParsedLog.parse_log_to_entries(log_lines)
    assert len(entries) == SampleInfo.NUM_ENTRIES


def test_parse_metadata():
    parsed_log = test_utils.create_parsed_log()
    metadata = parsed_log.get_metadata()

    assert metadata.get_num_lines() == 5
    assert metadata.get_product_name() == SampleInfo.PRODUCT_NAME
    assert metadata.get_version() == SampleInfo.VERSION
    assert metadata.get_git_hash() == SampleInfo.GIT_HASH

    assert metadata.get_start_time() == SampleInfo.START_TIME
    assert metadata.get_end_time() == SampleInfo.END_TIME

    expected_time_span = \
        (defs_and_utils.parse_date_time(SampleInfo.END_TIME) -
         defs_and_utils.parse_date_time(SampleInfo.START_TIME)).seconds
    assert metadata.get_log_time_span_seconds() == expected_time_span


def test_parse_options():
    parsed_log = test_utils.create_parsed_log()
    assert parsed_log.get_cf_names() == SampleInfo.CF_NAMES

    actual_db_options = parsed_log.get_database_options()
    assert actual_db_options.are_db_wide_options_set()
    assert actual_db_options.get_column_families() == parsed_log.get_cf_names()

    # Assuming LogFileOptionsParser is fully tested and may be used
    expected_db_options = DatabaseOptions()
    expected_db_options.set_db_wide_options(SampleInfo.DB_WIDE_OPTIONS_DICT)
    for i in range(len(SampleInfo.CF_NAMES)):
        expected_db_options.set_cf_options(SampleInfo.CF_NAMES[i],
                                           SampleInfo.OPTIONS_DICTS[i],
                                           SampleInfo.TABLE_OPTIONS_DICTS[i])

    actual_db_wide_options = actual_db_options.get_db_wide_options()
    expected_db_wide_options = expected_db_options.get_db_wide_options()
    assert expected_db_wide_options == actual_db_wide_options

    for i in range(len(SampleInfo.CF_NAMES)):
        cf_name = SampleInfo.CF_NAMES[i]
        actual_options = actual_db_options.get_cf_options(cf_name)
        expected_options = expected_db_options.get_cf_options(cf_name)
        assert expected_options == actual_options


def test_parse_warns():
    parsed_log = test_utils.create_parsed_log()

    warns_mngr = parsed_log.get_warnings_mngr()
    assert warns_mngr.get_total_num_warns() == 1


def test_parse_db_wide_stats():
    parsed_log = test_utils.create_parsed_log()

    mngr = parsed_log.get_stats_mngr()
    db_wide_stats_mngr = mngr.get_db_wide_stats_mngr()
    assert db_wide_stats_mngr.get_stalls_entries() == \
           SampleInfo.DB_WIDE_STALLS_ENTRIES
