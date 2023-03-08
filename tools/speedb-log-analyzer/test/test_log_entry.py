import pytest
from log_entry import LogEntry
import defs_and_utils


def test_is_entry_start():
    # Dummy text
    assert not LogEntry.is_entry_start(("XXXX"))

    # Invalid line - timestamp missing microseconds
    assert not LogEntry.is_entry_start("2022/11/24-15:58:04")

    # Invalid line - timestamp microseconds is cropped
    assert not LogEntry.is_entry_start("2022/11/24-15:58:04.758")

    # Valid line
    assert LogEntry.is_entry_start("2022/11/24-15:58:04.758352 32819 ")


def test_basic_single_line():
    log_line1 = "2022/11/24-15:58:04.758402 32819 DB SUMMARY"
    log_line2 = "2022/11/24-15:58:05.068464 32819 [/version_set.cc:4965] " \
                "Recovered from manifest"

    entry = LogEntry(100, log_line1, True)
    assert "2022/11/24-15:58:04.758402" == entry.get_time()
    assert entry.get_start_line_idx() == 100
    assert entry.get_lines_idxs_range() == (100, 101)
    assert not entry.get_code_pos()
    assert not entry.is_warn_msg()
    assert entry.get_warning_type() is None
    assert entry.have_all_lines_been_added()

    with pytest.raises(defs_and_utils.ParsingAssertion):
        entry.add_line(log_line2, last_line=True)
    with pytest.raises(defs_and_utils.ParsingAssertion):
        entry.add_line(log_line2, last_line=False)
    with pytest.raises(defs_and_utils.ParsingAssertion):
        entry.all_lines_added()


def test_warn_single_line():
    warn_msg = "2022/04/17-15:24:51.089890 7f4a9fdff700 [WARN] " \
               "[/column_family.cc:932] Stalling writes, " \
               "L0 files 2, memtables 2"

    entry = LogEntry(100, warn_msg, True)
    assert "2022/04/17-15:24:51.089890" == entry.get_time()
    assert entry.get_code_pos() == "/column_family.cc:932"
    assert entry.is_warn_msg()
    assert entry.get_warning_type() == defs_and_utils.WarningType.WARN


def test_multi_line_entry():
    log_line1 = "2022/11/24-15:58:04.758402 32819 DB SUMMARY"
    log_line2 = "Continuation Line 1"
    log_line3 = "Continuation Line 2"
    log_line4 = "Continuation Line 2"

    log_line5 = "2022/11/24-15:58:05.068464 32819 [/version_set.cc:4965] " \
                "Recovered from manifest"

    entry = LogEntry(100, log_line1, False)
    assert "2022/11/24-15:58:04.758402" == entry.get_time()
    assert not entry.have_all_lines_been_added()

    entry.add_line(log_line2, last_line=False)
    assert not entry.have_all_lines_been_added()
    assert entry.get_lines_idxs_range() == (100, 102)

    # Attempting to add the start of a new entry
    with pytest.raises(defs_and_utils.ParsingAssertion):
        entry.add_line(log_line5, last_line=True)

    assert not entry.have_all_lines_been_added()
    assert entry.get_lines_idxs_range() == (100, 102)

    entry.add_line(log_line3, last_line=False)
    assert not entry.have_all_lines_been_added()
    assert entry.get_lines_idxs_range() == (100, 103)

    entry.all_lines_added()
    assert entry.have_all_lines_been_added()

    with pytest.raises(defs_and_utils.ParsingAssertion):
        entry.all_lines_added()

    with pytest.raises(defs_and_utils.ParsingAssertion):
        entry.add_line(log_line4, last_line=True)
    with pytest.raises(defs_and_utils.ParsingAssertion):
        entry.add_line(log_line4, last_line=False)


def test_invalid_entry_start_no_time():
    with pytest.raises(defs_and_utils.ParsingAssertion) as e:
        LogEntry(10, "Not an entry start line")

    expected_msg = "[ (line:11)] - Line isn't entry's start\n" \
                   "Not an entry start line"
    assert str(e.value) == expected_msg


def test_invalid_entry_start_with_time_rest_missing():
    log_line = "2022/11/24-15:58:04.758402"
    with pytest.raises(defs_and_utils.ParsingError) as e:
        LogEntry(10, log_line)

    expected_msg = f"[ (line:11)] - Failed parsing Log Entry start line:\n" \
                   f"{log_line}"
    assert str(e.value) == expected_msg
