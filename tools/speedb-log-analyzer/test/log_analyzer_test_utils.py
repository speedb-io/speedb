from log_entry import LogEntry


def read_sample_file(file_name, expected_num_entries):
    file_path = "input_files/" + file_name
    f = open(file_path)
    lines = f.readlines()
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

    assert len(entries) == expected_num_entries

    return entries
