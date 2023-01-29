from log_entry import LogEntry
from log_file import ParsedLog
from test.sample_log_info import SampleInfo


def read_file(file_path):
    with open(file_path, "r") as f:
        return f.readlines()


def create_parsed_log():
    log_lines = read_file(SampleInfo.FILE_PATH)
    return ParsedLog(log_lines)


def lines_to_entries(lines):
    entries = []
    entry = None
    for i, line in enumerate(lines):
        if LogEntry.is_entry_start(line):
            if entry:
                entries.append(entry.all_lines_added())
            entry = LogEntry(i, line)
        else:
            assert entry
            entry.add_line(line)

    if entry:
        entries.append(entry.all_lines_added())

    return entries
