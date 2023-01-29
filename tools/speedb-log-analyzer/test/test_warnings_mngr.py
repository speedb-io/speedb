import defs_and_utils
from log_entry import LogEntry
from warnings_mngr import WarningElementInfo, WarningsMngr
import regexes


def create_warning_entry(warning_type, code_pos,
                         warning_element_info: WarningElementInfo):
    warning_line = warning_element_info.warning_time + " "
    warning_line += '7f4a8b5bb700 '

    warning_line += f'[{warning_type.name}] '
    warning_line += f'[{code_pos}] '

    if warning_element_info.cf_name != defs_and_utils.NO_COL_FAMILY:
        warning_line += f'[{warning_element_info.cf_name}] '
    warning_line += warning_element_info.warning_msg

    warning_entry = LogEntry(0, warning_line, True)
    assert warning_entry.is_warn_msg()
    assert warning_entry.get_warning_type() == warning_type
    assert warning_entry.get_code_pos() == code_pos

    return warning_entry


def add_warning(warnings_mngr: WarningsMngr, time, code_pos, cf_name,
                warn_msg, expected_warnings_info, expected_errors_info):
    num_stalls = warnings_mngr.get_num_stalls()
    num_stops = warnings_mngr.get_num_stops()

    warning_element_info = WarningElementInfo(time, cf_name, warn_msg)
    warn_entry1 = create_warning_entry(defs_and_utils.WarningType.WARN,
                                       code_pos,
                                       warning_element_info)
    assert warnings_mngr.try_adding_entry(warn_entry1)

    if code_pos not in expected_warnings_info:
        expected_warnings_info[code_pos] = []
    expected_warnings_info[code_pos].append(warning_element_info)
    validate_expected_warnings(warnings_mngr, expected_warnings_info,
                               expected_errors_info)

    if warn_msg.startswith(regexes.STALLS_WARN_MSG_PREFIX):

        num_stalls += 1
    if warn_msg.startswith(regexes.STOPS_WARN_MSG_PREFIX):
        num_stops += 1
    assert num_stalls == warnings_mngr.get_num_stalls()
    assert num_stops == warnings_mngr.get_num_stops()

    return expected_warnings_info


def validate_expected_warnings(warnings_mngr: WarningsMngr,
                               expected_warnings_info,
                               expected_errors_info):
    assert warnings_mngr.get_total_num_warns() == len(expected_warnings_info)
    assert warnings_mngr.get_total_num_errors() == len(expected_errors_info)

    actual_warn_warnings = warnings_mngr.get_warn_warnings()
    for warning_type_id, elements_info_list in expected_warnings_info.items():
        assert warning_type_id in actual_warn_warnings
        assert elements_info_list == actual_warn_warnings[warning_type_id]

    actual_error_warnings = warnings_mngr.get_error_warnings()
    for warning_type_id, elements_info_list in expected_errors_info.items():
        assert warning_type_id in actual_error_warnings
        assert elements_info_list == actual_error_warnings[warning_type_id]


def test_non_warnings_entries():
    line1 = '''2022/04/17-14:21:50.026058 7f4a8b5bb700 [/flush_job.cc:333]
    [cf1] [JOB 9] Flushing memtable with next log file: 5'''

    line2 = '''2022/04/17-14:21:50.026087 7f4a8b5bb700 EVENT_LOG_v1
    {"time_micros": 1650205310026076, "job": 9, "event": "flush_started"'''

    cf_names = ["cf1", "cf2"]
    warnings_mngr = WarningsMngr(cf_names)

    assert LogEntry.is_entry_start(line1)
    entry1 = LogEntry(0, line1, True)
    assert LogEntry.is_entry_start(line2)
    entry2 = LogEntry(0, line2, True)

    assert not warnings_mngr.try_adding_entry(entry1)
    assert not warnings_mngr.try_adding_entry(entry2)


def test_warn_entries():
    cf1 = "cf1"
    cf2 = "cf2"
    cf_names = [cf1, cf2]
    warnings_mngr = WarningsMngr(cf_names)

    expected_warnings_info = dict()
    expected_errors_info = dict()
    validate_expected_warnings(warnings_mngr, expected_warnings_info,
                               expected_errors_info)

    code_pos1 = "/flush_job.cc:333"
    code_pos2 = "/column_family.cc:932"

    expected_warnings_info = add_warning(warnings_mngr,
                                         "2022/04/17-14:21:50.026058",
                                         code_pos1, cf1, "Warning Message 1",
                                         expected_warnings_info,
                                         expected_errors_info)

    expected_warnings_info = add_warning(warnings_mngr,
                                         "2022/04/17-14:21:50.026058",
                                         code_pos1, cf2, "Warning Message 2",
                                         expected_warnings_info,
                                         expected_errors_info)

    expected_warnings_info = add_warning(warnings_mngr,
                                         "2022/04/17-14:21:50.026058",
                                         code_pos2, cf2, "Warning Message 3",
                                         expected_warnings_info,
                                         expected_errors_info)


def test_num_stalls_and_stops():
    cf1 = "cf1"
    cf2 = "cf2"
    cf_names = [cf1, cf2]

    warnings_mngr = WarningsMngr(cf_names)
    assert warnings_mngr.get_num_stalls() == 0
    assert warnings_mngr.get_num_stops() == 0

    stall_msg1 = regexes.STALLS_WARN_MSG_PREFIX + ", L0 files 2, memtables 2"
    stall_msg2 = regexes.STALLS_WARN_MSG_PREFIX + ", L0 files 3, memtables 2"
    stall_msg3 = regexes.STALLS_WARN_MSG_PREFIX + ", L0 files 4, memtables 2"
    stop_msg1 = regexes.STOPS_WARN_MSG_PREFIX + " Dummy Text 1"
    stop_msg2 = regexes.STOPS_WARN_MSG_PREFIX + " Dummy Text 2"

    expected_warnings_info = dict()
    expected_errors_info = dict()
    code_pos1 = "/flush_job.cc:333"
    code_pos2 = "/column_family.cc:932"

    # Add a stall message #1
    expected_warnings_info = add_warning(warnings_mngr,
                                         "2022/04/17-14:21:50.026058",
                                         code_pos1, cf1, stall_msg1,
                                         expected_warnings_info,
                                         expected_errors_info)

    # Add a stop message #1
    expected_warnings_info = add_warning(warnings_mngr,
                                         "2022/04/17-14:21:50.026058",
                                         code_pos1, cf1, stall_msg2,
                                         expected_warnings_info,
                                         expected_errors_info)

    # Add a stall message #1
    expected_warnings_info = add_warning(warnings_mngr,
                                         "2022/05/17-14:21:50.026058",
                                         code_pos2, cf2, stop_msg1,
                                         expected_warnings_info,
                                         expected_errors_info)

    # Add a stall message #3
    expected_warnings_info = add_warning(warnings_mngr,
                                         "2022/06/17-14:21:50.026058",
                                         code_pos2, cf2, stall_msg3,
                                         expected_warnings_info,
                                         expected_errors_info)

    # Add a stall message #2
    expected_warnings_info = add_warning(warnings_mngr,
                                         "2022/05/17-14:21:50.026058",
                                         code_pos2, cf1, stop_msg2,
                                         expected_warnings_info,
                                         expected_errors_info)

    assert warnings_mngr.get_num_stalls() == 3
    assert warnings_mngr.get_num_stops() == 2
