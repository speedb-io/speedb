import re
import regexes
import defs_and_utils


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

    @staticmethod
    def validate_entry_start(line_idx, log_line):
        if not LogEntry.is_entry_start(log_line):
            raise defs_and_utils.ParsingAssertion(
                f"Line isn't entry's start\n{log_line}", line_idx=line_idx)

    def validate_finalized(self):
        if not self.is_finalized:
            raise defs_and_utils.ParsingAssertion(
                f"Entry already finalized. {self}")

    def __init__(self, line_idx, log_line, last_line=False):
        LogEntry.validate_entry_start(line_idx, log_line)

        self.is_finalized = False
        self.start_line_idx = line_idx

        # Try to parse as a warning line
        parts = re.findall(regexes.START_LINE_WITH_WARN_PARTS_REGEX, log_line)
        if parts:
            num_parts_expected = 6
        else:
            # Not a warning line => Parse as "normal" line
            parts = re.findall(regexes.START_LINE_PARTS_REGEX, log_line)
            if not parts:
                raise defs_and_utils.ParsingError(
                    f"Failed parsing Log Entry start line:\n{log_line}",
                    line_idx=line_idx)
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
        return f"LogEntry (lines:{self.get_lines_idxs_range()}), Start:\n" \
               f"{self.msg_lines[0]}"

    def validate_not_finalized(self, log_line=None):
        if self.is_finalized:
            msg = "Entry already finalized."
            if log_line:
                msg += f". Added line:\n{log_line}\n"
            msg += f"\n{self}"
            raise defs_and_utils.ParsingAssertion(msg, self.start_line_idx)

    def validate_not_adding_entry_start_line(self, log_line):
        if LogEntry.is_entry_start(log_line):
            raise defs_and_utils.ParsingAssertion(
                f"Illegal attempt to add an entry start as a line to "
                f"an existing entry. Line:\n{log_line}\n{self}")

    def add_line(self, log_line, last_line=False):
        self.validate_not_finalized(log_line)
        self.validate_not_adding_entry_start_line(log_line)

        self.msg_lines.append(log_line.strip())
        if last_line:
            self.all_lines_added()

    def all_lines_added(self):
        self.validate_not_finalized()
        assert not self.is_finalized

        self.is_finalized = True
        return self

    def have_all_lines_been_added(self):
        return self.is_finalized

    def get_start_line_idx(self):
        return self.start_line_idx

    def get_lines_idxs_range(self):
        return self.start_line_idx, self.start_line_idx+len(self.msg_lines)

    def get_time(self):
        return self.time

    def get_gmt_timestamp(self):
        return defs_and_utils.get_gmt_timestamp(self.time)

    def get_code_pos(self):
        return self.code_pos

    def get_msg_lines(self):
        return self.msg_lines

    def get_msg(self):
        return ("\n".join(self.msg_lines)).strip()

    def is_warn_msg(self):
        return self.warning_type

    def get_warning_type(self):
        return self.warning_type
