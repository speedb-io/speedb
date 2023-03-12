import re
from log_entry import LogEntry
import regexes
import defs_and_utils


class LogFileOptionsParser:
    @staticmethod
    def try_parsing_as_options_entry(log_entry):
        assert isinstance(log_entry, LogEntry)

        option_parts_match = re.findall(regexes.OPTION_LINE_REGEX,
                                        log_entry.get_msg())
        if len(option_parts_match) != 1 or len(option_parts_match[0]) != 2:
            return None

        option_name = option_parts_match[0][0].strip()
        option_value = option_parts_match[0][1].strip()

        return option_name, option_value

    @staticmethod
    def is_options_entry(line):
        if LogFileOptionsParser.try_parsing_as_options_entry(line):
            return True
        else:
            return False

    @staticmethod
    def try_parsing_as_table_options_entry(log_entry):
        assert isinstance(log_entry, LogEntry)

        options_lines = log_entry.get_msg_lines()
        if len(options_lines) < 1:
            # TODO - Maybe a bug - consider asserting
            return None

        options_dict = dict()
        # first line has the "table_factory options:" prefix
        # example:
        # options:   flush_block_policy_factory: FlushBlockBySizePolicyFactory
        option_parts_match = re.findall(regexes.TABLE_OPTIONS_START_LINE_REGEX,
                                        options_lines[0])
        if len(option_parts_match) != 1 or len(option_parts_match[0]) != 2:
            return None
        options_dict[option_parts_match[0][0].strip()] = \
            option_parts_match[0][1].strip()

        for line in options_lines[1:]:
            option_parts_match = re.findall(
                regexes.TABLE_OPTIONS_CONTINUATION_LINE_REGEX, line)
            if len(option_parts_match) == 1 and \
                    len(option_parts_match[0]) == 2:
                options_dict[option_parts_match[0][0].strip()] = \
                    option_parts_match[0][1].strip()

        return options_dict

    @staticmethod
    def try_parsing_as_cf_options_start_entry(log_entry):
        parts = re.findall(regexes.CF_OPTIONS_START_REGEX, log_entry.get_msg())
        if not parts or len(parts) != 1:
            return None
        # In case of match, we return the column-family name
        return parts[0]

    @staticmethod
    def is_cf_options_start_entry(log_entry):
        result = LogFileOptionsParser.try_parsing_as_cf_options_start_entry(
            log_entry)
        return result is not None

    @staticmethod
    def parse_db_wide_options(log_entries, start_entry_idx, end_entry_idx):
        """
        Parses all of the entries in the specified range of
        [start_entry_idx, end_entry_idx)

        Returns:
            options_dict: The parsed options:
                dict(<option name>: <option value>)
            entry_idx: the index of the entry
        """
        options_dict = {}
        entry_idx = start_entry_idx
        while entry_idx < end_entry_idx:
            entry = log_entries[entry_idx]
            options_kv = \
                LogFileOptionsParser.try_parsing_as_options_entry(entry)
            if options_kv:
                option_name, option_value = options_kv
                options_dict[option_name] = option_value
            else:
                defs_and_utils.g_parsing_warnings.append(
                    f"TODO - ERROR In DB Wide Entry (idx:{entry_idx}),"
                    f" {entry}")

            entry_idx += 1

        return options_dict

    @staticmethod
    def parse_cf_options(log_entries, start_entry_idx, cf_name=None):
        entry_idx = start_entry_idx

        # If cf_name was specified, it means we have received cf options
        # without the CF options header entry
        if cf_name is None:
            cf_name = LogFileOptionsParser. \
                try_parsing_as_cf_options_start_entry(log_entries[entry_idx])
            entry_idx += 1

        # cf_name may be the emtpy string, but not None
        assert cf_name is not None

        options_dict = {}
        table_options_dict = None
        duplicate_option = False
        while entry_idx < len(log_entries):
            entry = log_entries[entry_idx]
            options_kv = \
                LogFileOptionsParser.try_parsing_as_options_entry(entry)
            if options_kv:
                option_name, option_value = options_kv
                if option_name in options_dict:
                    # finding the same option twice implies that the options
                    # for this cf are over.
                    duplicate_option = True
                    break
                options_dict[option_name] = option_value
            else:
                temp_table_options_dict = \
                    LogFileOptionsParser.try_parsing_as_table_options_entry(
                           entry)
                if temp_table_options_dict:
                    assert table_options_dict is None
                    table_options_dict = temp_table_options_dict
                else:
                    # The entry is a different type of entry => done
                    break

            entry_idx += 1

        assert options_dict, "No Options for Column Family"
        assert table_options_dict, "Missing table options in CF options"

        return cf_name, options_dict, table_options_dict, entry_idx, \
            duplicate_option
