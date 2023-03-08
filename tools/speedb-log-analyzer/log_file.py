import re
import defs_and_utils
import regexes
from log_entry import LogEntry
from log_file_options_parser import LogFileOptionsParser
from database_options import DatabaseOptions
from events_mngr import EventsMngr
from warnings_mngr import WarningsMngr
from stats_mngr import StatsMngr


class LogFileMetadata:
    """
    Contains the metadata information about a log file:
    - Product Name (RocksDB / Speedb)
    - S/W Version
    - Git Hash
    - DB Session Id
    - Times of first and last log entries in the file
    """
    def __init__(self, log_entries, start_entry_idx):
        entry_idx = start_entry_idx
        entry = log_entries[entry_idx]

        # 1st entry assumed to be the Version Line
        self.product_name, self.version =\
            LogFileMetadata.extract_version_info(entry)
        entry_idx += 1

        # Start / end times of file log's entries
        self.start_time = entry.get_time()
        # Will be set later (when file is fully parsed)
        self.end_time = None

        # GIT Hash Line
        # example:
        # "... Git sha UNKNOWN:0a396684d6c08f6fe4a37572c0429d91176c51d1 ..."
        self.git_hash = \
            LogFileMetadata.extract_git_hash(log_entries[entry_idx])
        entry_idx += 1

        # Skip Compile Time Line & "DB SUMMARY" lines
        # TODO - verify that the lines indeed contain these strings
        entry_idx += 2

        self.db_session_id = LogFileMetadata.extract_db_session_id(
            log_entries[entry_idx])
        entry_idx += 1

        self.num_lines = entry_idx - start_entry_idx

    @staticmethod
    def extract_version_info(log_entry):
        product_name_and_version = str(log_entry.msg_lines[0]).strip()
        match_parts = re.findall(regexes.VERSION_PARTS_REGEX,
                                 product_name_and_version)
        assert len(match_parts) == 1,\
            f"Failed parsing product & version line " \
            f"({product_name_and_version})"
        return match_parts[0]

    @staticmethod
    def extract_git_hash(log_entry):
        git_hash_parts = re.findall(regexes.GIT_HASH_LINE_REGEX,
                                    log_entry.msg_lines[0])
        assert git_hash_parts and len(git_hash_parts) == 1
        return git_hash_parts[0]

    @staticmethod
    def extract_db_session_id(log_entry):
        session_id_str = str(log_entry.msg_lines).strip()
        session_id_parts = re.findall(r"DB Session ID:  ([0-9A-Z]*)",
                                      session_id_str)
        # TODO - Check why Redis do not have a session id
        if not session_id_parts:
            return ""

        assert len(session_id_parts) == 1
        return session_id_parts[0]

    def set_end_time(self, end_time):
        assert not self.end_time,\
            f"End time already set ({self.end_time})"
        assert defs_and_utils.parse_date_time(end_time) >=\
               defs_and_utils.parse_date_time(self.start_time)

        self.end_time = end_time

    def get_num_lines(self):
        return self.num_lines

    def get_product_name(self):
        return self.product_name

    def get_version(self):
        return self.version

    def get_git_hash(self):
        return self.git_hash

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def get_log_time_span_seconds(self):
        assert self.end_time, "Unknown end time"

        return ((defs_and_utils.parse_date_time(self.end_time) -
                 defs_and_utils.parse_date_time(self.start_time)).seconds)


class ParsedLog:
    def __init__(self, log_file_path, log_lines):
        self.log_file_path = log_file_path
        self.metadata = None
        self.db_options = DatabaseOptions()
        self.cf_names = []

        # These entities need to support dynamic addition of cf-s
        # The cf names will be added when they are detected during parsing
        self.events_mngr = EventsMngr(self.cf_names)
        self.warnings_mngr = WarningsMngr(self.cf_names)
        self.stats_mngr = StatsMngr()

        entry_idx = 0
        log_entries = self.parse_log_to_entries(log_file_path, log_lines)

        entry_idx = self.parse_metadata(log_entries, entry_idx)
        self.set_end_time(log_entries)
        self.parse_rest_of_log(log_entries, entry_idx)

    @staticmethod
    def parse_log_to_entries(log_file_path, log_lines):
        line_idx = 0
        if len(log_lines) < 1:
            raise defs_and_utils.ParsingError(log_file_path, 1,
                                              "Empty File")

        # first line must be the beginning of a log entry
        if not LogEntry.is_entry_start(log_lines[0]):
            raise defs_and_utils.ParsingError(log_file_path, 1,
                                              f"Unexpected first log line:"
                                              f"\n{log_lines[0]}")

        log_entries = []
        new_entry = None
        for line_idx, line in enumerate(log_lines):
            if LogEntry.is_entry_start(line):
                if new_entry:
                    log_entries.append(new_entry.all_lines_added())
                new_entry = LogEntry(line_idx, line)
            else:
                # To account for logs split into multiple lines
                new_entry.add_line(line)

        # Handle the last entry in the file.
        if new_entry:
            log_entries.append(new_entry.all_lines_added())

        return log_entries

    def parse_metadata(self, log_entries, start_entry_idx):
        self.metadata = LogFileMetadata(log_entries, start_entry_idx)
        return start_entry_idx + self.metadata.num_lines

    def parse_db_wide_options(self, log_entries, start_entry_idx):
        support_info_entry_idx = \
            ParsedLog.find_support_info_start_index(log_entries,
                                                    start_entry_idx)
        options_dict =\
            LogFileOptionsParser.parse_db_wide_options(log_entries,
                                                       start_entry_idx,
                                                       support_info_entry_idx)
        entry_idx = support_info_entry_idx
        self.db_options.set_db_wide_options(options_dict)
        return entry_idx

    def set_end_time(self, log_entries):
        last_entry = log_entries[-1]
        end_time = last_entry.get_time()
        self.metadata.set_end_time(end_time)

    def parse_cf_options(self, log_entries, start_entry_idx):
        cf_name, options_dict, table_options_dict, entry_idx = \
            LogFileOptionsParser.parse_cf_options(log_entries, start_entry_idx)

        self.db_options.set_cf_options(cf_name,
                                       options_dict,
                                       table_options_dict)
        return entry_idx, cf_name

    def add_cf_name(self, cf_name):
        assert cf_name not in self.cf_names
        self.cf_names.append(cf_name)

    @staticmethod
    def find_support_info_start_index(log_entries, start_entry_idx):
        entry_idx = start_entry_idx
        while entry_idx < len(log_entries):
            if re.findall(regexes.SUPPORT_INFO_START_LINE_REGEX,
                          log_entries[entry_idx].get_msg_lines()[0]):
                return entry_idx
            entry_idx += 1
        assert False

    def parse_rest_of_log(self, log_entries, start_entry_idx):
        # Parse all the entries and process those that are required
        entry_idx = start_entry_idx
        while entry_idx < len(log_entries):
            entry = log_entries[entry_idx]

            if LogFileOptionsParser.is_cf_options_start_entry(entry):
                entry_idx, cf_name =\
                    self.parse_cf_options(log_entries, entry_idx)
                self.add_cf_name(cf_name)

            elif LogFileOptionsParser.is_options_entry(entry):
                entry_idx = self.parse_db_wide_options(log_entries, entry_idx)

            elif entry.is_warn_msg():
                assert self.warnings_mngr.try_adding_entry(entry)
                entry_idx += 1

            elif self.events_mngr.try_adding_entry(entry):
                entry_idx += 1

            else:
                prev_entry_idx = entry_idx
                is_stats, entry_idx = \
                    self.stats_mngr.try_adding_entries(log_entries, entry_idx)
                if is_stats:
                    assert entry_idx > prev_entry_idx
                else:
                    entry_idx += 1

    def get_log_file_path(self):
        return self.log_file_path

    def get_metadata(self):
        return self.metadata

    def get_cf_names(self):
        return self.cf_names

    def get_num_cfs(self):
        return len(self.cf_names)

    def get_database_options(self):
        return self.db_options

    def get_events_mngr(self):
        return self.events_mngr

    def get_stats_mngr(self):
        return self.stats_mngr

    def get_warnings_mngr(self):
        return self.warnings_mngr
