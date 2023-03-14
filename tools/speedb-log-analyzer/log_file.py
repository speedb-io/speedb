import re
import defs_and_utils
import regexes
import logging
from log_entry import LogEntry
from log_file_options_parser import LogFileOptionsParser
from database_options import DatabaseOptions
from events_mngr import EventsMngr
from warnings_mngr import WarningsMngr
from stats_mngr import StatsMngr


class LogFileMetadata:
    """
    (Possibly) Contains the metadata information about a log file:
    - Product Name (RocksDB / Speedb)
    - S/W Version
    - Git Hash
    - DB Session Id
    - Times of first and last log entries in the file
    """
    def __init__(self, log_entries, start_entry_idx):
        self.product_name = None
        self.version = None
        self.db_session_id = None
        self.git_hash = None
        self.start_time = None
        # Will be set later (when file is fully parsed)
        self.end_time = None

        if len(log_entries) == 0:
            logging.warning("Empty Metadata pars (no entries)")
            return

        self.start_time = log_entries[0].get_time()

        # Parsing all entries and searching for predefined metadata
        # entities. Some may or may not exist (e.g., the DB Session Id is
        # not present in rolled logs). Also, no fixed order is assumed.
        # However, it is a parsing error if the same entitiy is found more
        # than once
        for i, entry in enumerate(log_entries):
            if self.try_parse_as_product_and_version_entry(entry):
                continue
            elif self.try_parse_as_git_hash_entry(entry):
                continue
            elif self.try_parse_as_db_session_id_entry(entry):
                continue

    def __str__(self):
        start_time = self.start_time if self.start_time else "UNKNOWN"
        end_time = self.end_time if self.end_time else "UNKNOWN"
        return f"LogFileMetadata: Start:{start_time}, End:{end_time}"

    def try_parse_as_product_and_version_entry(self, log_entry):
        lines = str(log_entry.msg_lines[0]).strip()
        match_parts = re.findall(regexes.PRODUCT_AND_VERSION_REGEX, lines)

        if not match_parts or len(match_parts) != 1:
            return False

        if self.product_name or self.version:
            raise defs_and_utils.ParsingError(
                    f"Product / Version already parsed. Product:"
                    f"{self.product_name}, Version:{self.version})."
                    f"\n{log_entry}")

        self.product_name, self.version = match_parts[0]
        return True

    def try_parse_as_git_hash_entry(self, log_entry):
        lines = str(log_entry.msg_lines[0]).strip()
        match_parts = re.findall(regexes.GIT_HASH_LINE_REGEX, lines)

        if not match_parts or len(match_parts) != 1:
            return False

        if self.git_hash:
            raise defs_and_utils.ParsingError(
                f"Git Hash Already Parsed ({self.git_hash})\n{log_entry}")

        self.git_hash = match_parts[0]
        return True

    def try_parse_as_db_session_id_entry(self, log_entry):
        lines = str(log_entry.msg_lines[0]).strip()
        match_parts = re.findall(regexes.DB_SESSION_ID_REGEX, lines)

        if not match_parts or len(match_parts) != 1:
            return False

        if self.db_session_id:
            raise defs_and_utils.ParsingError(
                f"DB Session Id Already Parsed ({self.db_session_id})"
                f"n{log_entry}")

        self.db_session_id = match_parts[0]
        return True

    def set_end_time(self, end_time):
        assert not self.end_time,\
            f"End time already set ({self.end_time})"
        assert defs_and_utils.parse_date_time(end_time) >=\
               defs_and_utils.parse_date_time(self.start_time)

        self.end_time = end_time

    def get_product_name(self):
        return self.product_name

    def get_version(self):
        return self.version

    def get_git_hash(self):
        return self.git_hash

    def get_db_session_id(self):
        return self.db_session_id

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def get_log_time_span_seconds(self):
        if not self.end_time:
            raise defs_and_utils.ParsingAssertion(f"Unknown end time.\n{self}")

        return ((defs_and_utils.parse_date_time(self.end_time) -
                 defs_and_utils.parse_date_time(self.start_time)).seconds)


class ParsedLog:
    def __init__(self, log_file_path, log_lines):
        self.log_file_path = log_file_path
        self.metadata = None
        self.db_options = DatabaseOptions()
        self.cf_names = {}
        self.next_unknown_cf_name_suffix = None

        # These entities need to support dynamic addition of cf-s
        # The cf names will be added when they are detected during parsing
        self.events_mngr = EventsMngr()
        self.warnings_mngr = WarningsMngr()
        self.stats_mngr = StatsMngr()
        self.not_parsed_entries = []

        self.entry_idx = 0
        log_entries = self.parse_log_to_entries(log_file_path, log_lines)
        self.parse_metadata(log_entries)
        self.set_end_time(log_entries)
        self.parse_rest_of_log(log_entries)

    def __str__(self):
        return f"ParsedLog ({self.log_file_path})"

    @staticmethod
    def parse_log_to_entries(log_file_path, log_lines):
        if len(log_lines) < 1:
            raise defs_and_utils.ParsingError("Empty File", log_file_path, 1)

        # first line must be the beginning of a log entry
        if not LogEntry.is_entry_start(log_lines[0]):
            raise defs_and_utils.ParsingError(f"Unexpected first log line:"
                                              f"\n{log_lines[0]}",
                                              log_file_path, 1,)

        # Failure to parse an entry should just skip that entry
        # (best effort)
        log_entries = []
        new_entry = None
        skip_until_next_entry_start = False
        for line_idx, line in enumerate(log_lines):
            try:
                if LogEntry.is_entry_start(line):
                    skip_until_next_entry_start = False
                    if new_entry:
                        log_entries.append(new_entry.all_lines_added())
                    new_entry = LogEntry(line_idx, line)
                else:
                    # To account for logs split into multiple lines
                    if new_entry:
                        new_entry.add_line(line)
                    else:
                        if not skip_until_next_entry_start:
                            raise defs_and_utils.ParsingAssertion(
                                "Bug while parsing log to entries.",
                                log_file_path, line_idx)
            except defs_and_utils.ParsingError as e:
                logging.error(str(e.value))
                # Discarding the "bad" entry and skipping all lines until
                # finding the start of the next one.
                new_entry = None
                skip_until_next_entry_start = True

        # Handle the last entry in the file.
        if new_entry:
            log_entries.append(new_entry.all_lines_added())

        return log_entries

    @staticmethod
    def find_next_options_entry(log_entries, start_entry_idx):
        entry_idx = start_entry_idx
        while entry_idx < len(log_entries) and \
                not LogFileOptionsParser.is_options_entry(
                    log_entries[entry_idx]):
            entry_idx += 1

        return (entry_idx < len(log_entries)), entry_idx

    def parse_metadata(self, log_entries):
        # Metadata must be at the top of the log and surely doesn't extend
        # beyond the first options line
        has_found, options_entry_idx = \
            ParsedLog.find_next_options_entry(log_entries, self.entry_idx)

        self.metadata = \
            LogFileMetadata(log_entries[self.entry_idx:options_entry_idx],
                            self.entry_idx)
        self.entry_idx = options_entry_idx

    def get_next_unknown_cf_name(self):
        # The first one is always "default"
        if self.next_unknown_cf_name_suffix is None:
            self.next_unknown_cf_name_suffix = 1
            return "default"
        else:
            next_cf_name = f"Unknown-CF-#{self.next_unknown_cf_name_suffix}"
            self.next_unknown_cf_name_suffix += 1
            return next_cf_name

    def parse_cf_options(self, log_entries, cf_options_header_available):
        if cf_options_header_available:
            auto_generated_cf_name = None
        else:
            auto_generated_cf_name = self.get_next_unknown_cf_name()

        cf_name, options_dict, table_options_dict, self.entry_idx, \
            duplicate_option = \
            LogFileOptionsParser.parse_cf_options(log_entries,
                                                  self.entry_idx,
                                                  auto_generated_cf_name)
        self.db_options.set_cf_options(cf_name,
                                       options_dict,
                                       table_options_dict)

        self.add_cf_name(cf_name, auto_generated_cf_name, has_options=True)

    def add_cf_name(self, cf_name, auto_generated_cf_name, has_options):

        # "default" may have been auto-generated, but it is always the actual
        # name of the first CF
        if auto_generated_cf_name is None or cf_name == "default":
            is_auto_generated_cf_name = False
        else:
            is_auto_generated_cf_name = True

        assert cf_name not in self.cf_names
        self.cf_names[cf_name] = {"auto_generated": is_auto_generated_cf_name,
                                  "has_options": has_options}

        # We don't expect to see our auto-generated cf names in events /
        # warnings. In these we expect to find only actual cf names
        if not auto_generated_cf_name:
            self.events_mngr.add_cf_name(cf_name)
            self.warnings_mngr.add_cf_name(cf_name)

    def handle_cf_name_found_during_parsing(self, cf_name):
        # a name found during parsing can't be one of the auto-generated ones
        if cf_name in self.cf_names:
            assert not self.cf_names[cf_name]["auto_generated"]
            return

        self.add_cf_name(cf_name, auto_generated_cf_name=None,
                         has_options=False)

    @staticmethod
    def find_support_info_start_index(log_entries, start_entry_idx):
        entry_idx = start_entry_idx
        while entry_idx < len(log_entries):
            if re.findall(regexes.SUPPORT_INFO_START_LINE_REGEX,
                          log_entries[entry_idx].get_msg_lines()[0]):
                return entry_idx
            entry_idx += 1

        raise defs_and_utils.ParsingError(
            f"Failed finding Support Info. start-idx:{start_entry_idx}")

    def are_dw_wide_options_set(self):
        return self.db_options.are_db_wide_options_set()

    def try_parse_as_db_wide_options(self, log_entries):
        if self.are_dw_wide_options_set() or \
                not LogFileOptionsParser.is_options_entry(
                    log_entries[self.entry_idx]):
            return False

        support_info_entry_idx = \
            ParsedLog.find_support_info_start_index(log_entries,
                                                    self.entry_idx)

        options_dict =\
            LogFileOptionsParser.parse_db_wide_options(log_entries,
                                                       self.entry_idx,
                                                       support_info_entry_idx)
        if not options_dict:
            raise defs_and_utils.ParsingError(
                f"Empy db-wide options dictionary ({self})."
                f"self.entry_idx:{self.entry_idx}.")

        self.db_options.set_db_wide_options(options_dict)
        self.entry_idx = support_info_entry_idx

        return True

    def try_parse_as_cf_options(self, log_entries):
        entry = log_entries[self.entry_idx]
        result = False
        if LogFileOptionsParser.is_cf_options_start_entry(entry):
            self.parse_cf_options(log_entries,
                                  cf_options_header_available=True)
            result = True
        elif LogFileOptionsParser.is_options_entry(entry):
            assert self.are_dw_wide_options_set()
            self.parse_cf_options(log_entries,
                                  cf_options_header_available=False)
            result = True

        return result

    def try_parse_as_warning_entries(self, log_entries):
        entry = log_entries[self.entry_idx]

        result, cf_name = self.warnings_mngr.try_adding_entry(entry)
        if result:
            if cf_name is not None:
                self.handle_cf_name_found_during_parsing(cf_name)
            self.entry_idx += 1

        return result

    def try_parse_as_event_entries(self, log_entries):
        entry = log_entries[self.entry_idx]

        result, cf_name = self.events_mngr.try_adding_entry(entry)
        if not result:
            return False

        if cf_name is not None:
            self.handle_cf_name_found_during_parsing(cf_name)
        self.entry_idx += 1

        return True

    def try_parse_as_stats_entries(self, log_entries):
        result, self.entry_idx, cfs_names = \
            self.stats_mngr.try_adding_entries(log_entries, self.entry_idx)

        if result:
            for cf_name in cfs_names:
                self.handle_cf_name_found_during_parsing(cf_name)

        return result

    def parse_rest_of_log(self, log_entries):
        # Parse all the entries and process those that are required
        while self.entry_idx < len(log_entries):
            if self.try_parse_as_db_wide_options(log_entries):
                continue

            if self.try_parse_as_cf_options(log_entries):
                continue

            if self.try_parse_as_warning_entries(log_entries):
                continue

            if self.try_parse_as_event_entries(log_entries):
                continue

            if self.try_parse_as_stats_entries(log_entries):
                continue

            self.not_parsed_entries.append(log_entries[self.entry_idx])
            self.entry_idx += 1

    def set_end_time(self, log_entries):
        last_entry = log_entries[-1]
        end_time = last_entry.get_time()
        self.metadata.set_end_time(end_time)

    def get_log_file_path(self):
        return self.log_file_path

    def get_metadata(self):
        return self.metadata

    def get_cf_names(self):
        # Return only the names of cf-s which have been actually present in
        # the log (also implicitly, like "default")
        return [cf_name for cf_name in self.cf_names.keys() if not
                self.cf_names[cf_name]["auto_generated"]]

    def get_auto_generated_cf_names(self):
        return [cf_name for cf_name in self.cf_names.keys() if
                self.cf_names[cf_name]["auto_generated"]]

    def get_num_cfs(self):
        return len(self.get_cf_names())

    def get_database_options(self):
        return self.db_options

    def get_events_mngr(self):
        return self.events_mngr

    def get_stats_mngr(self):
        return self.stats_mngr

    def get_warnings_mngr(self):
        return self.warnings_mngr
