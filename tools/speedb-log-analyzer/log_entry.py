import re
from enum import Enum, auto
import regexes
import defs_and_utils


class EntryType(Enum):
    NON_TABLE_OPTION = auto()
    TABLE_OPTIONS = auto()
    EVENT_PREAMBLE = auto()
    EVENT = auto()
    STATS = auto()
    MISC = auto()


class LogEntry:
    @staticmethod
    def is_entry_start(log_line, regex=None):
        token_list = log_line.strip().split()
        if not token_list:
            return False

        # The assumption is that a new log will start with a date
        if not re.findall(regexes.TIMESTAMP_REGEX, token_list[0]):
            return False

        if regex:
            # token_list[1] should be the context
            if not re.findall(regex, " ".join(token_list[2:])):
                return False

        return True

    def __init__(self, line_idx, log_line, last_line=False):
        assert LogEntry.is_entry_start(log_line),\
            "Line is not the start of a log entry"

        self.is_finalized = False
        self.line_idx = line_idx

        # Initially column family and type are unknown
        # May be deduced when the entry is finalized
        self.column_family = defs_and_utils.NO_COL_FAMILY
        self.type = EntryType.MISC
        self.job_id = None

        # Try to parse as a warning line
        parts = re.findall(regexes.START_LINE_WITH_WARN_PARTS_REGEX, log_line)
        if parts:
            num_parts_expected = 6
        else:
            # Not a warning line => Parse as "normal" line
            parts = re.findall(regexes.START_LINE_PARTS_REGEX, log_line)
            if not parts:
                raise defs_and_utils.ParsingError(
                    f"Failed parsing Log Entry start line:\n{log_line}")
            num_parts_expected = 5

        assert len(parts) == 1 and len(parts[0]) == num_parts_expected, \
            f"Unexpected # of parts (expected {num_parts_expected}) ({parts})"

        parts = parts[0]

        self.time = parts[0]
        self.context = parts[1]
        self.orig_time = parts[2]

        # warn msg
        if num_parts_expected == 6:
            self.warning_type = defs_and_utils.WarningType(parts[3])
            part_increment = 1
        else:
            self.warning_type = None
            part_increment = 0

        # File + Line in a source file
        # example: '... [/column_family.cc: 932] ...'
        self.code_pos = parts[3 + part_increment]
        if self.code_pos:
            code_pos_value_match = re.findall(r"\[(.*)\]", self.code_pos)
            if code_pos_value_match:
                self.code_pos = code_pos_value_match[0]

        # Rest of 1st line's text starts the msg_lines part
        self.msg_lines = list()
        if parts[4 + part_increment]:
            self.msg_lines.append(parts[4 + part_increment].strip())

        if last_line:
            self.all_lines_added()

    def __str__(self):
        return f"{self.get_time()} line:{self.line_idx}\n{self.get_msg()}"

    def add_line(self, log_line, last_line=False):
        self.msg_lines.append(log_line.strip())
        if last_line:
            self.all_lines_added()

    def all_lines_added(self):
        assert not self.is_finalized

        self.is_finalized = True
        return self

    def get_line_idx(self):
        return self.line_idx

    def get_type(self):
        assert self.is_finalized
        return self.type

    def get_column_family(self):
        assert self.is_finalized
        return self.get_column_family()

    def get_time(self):
        return self.time

    def get_gmt_timestamp(self):
        return defs_and_utils.get_gmt_timestamp(self.time)

    def get_human_readable_time(self):
        # example from a log line: '2018/07/25-11:25:45.782710'
        return self.time

    def get_code_pos(self):
        return self.code_pos

    def get_msg_lines(self):
        return self.msg_lines

    def get_msg(self):
        return ("\n".join(self.msg_lines)).strip()

    def get_job_id(self):
        return self.job_id

    def is_warn_msg(self):
        return self.warning_type

    def get_warning_type(self):
        assert self.is_warn_msg()
        return self.warning_type
