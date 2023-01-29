import re
from enum import Enum, auto
import datetime
from dataclasses import dataclass
import json
import configparser
import os
import argparse
import io
import sys
import glob


RAISE_EXCEPTION = 0


def get_default_config():
    return "[General.Options.Display]\n" \
           "    Statistics\n" \
           "[Cf.Options.Display]\n" \
           "    compaction_style\n" \
           "    write_buffer_size\n" \
           "[Cf.Options.TableFactory.Display]\n" \
           "    capacity\n" \
           "    block_cache\n" \
           "    cache_index_and_filter_blocks\n" \
           "    cache_index_and_filter_blocks_with_high_priority\n" \
           "    pin_l0_filter_and_index_blocks_in_cache\n" \
           "    unpartitioned_pinning\n"


TIMESTAMP_REGEX = r'\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6}'
FILE_PATH_REGEX = r"\/?.*?\.[\w:]+"


def parse_date_time(date_time_str):
    try:
        return datetime.datetime.strptime(date_time_str,
                                          '%Y/%m/%d-%H:%M:%S.%f')
    except ValueError:
        return None


def get_value_by_unit(size_str, size_units_str):
    size_units_str = size_units_str.strip()

    multiplier = 1
    if size_units_str == "KB" or size_units_str == "K":
        multiplier = 2 ** 10
    elif size_units_str == "MB" or size_units_str == "M":
        multiplier = 2 ** 20
    elif size_units_str == "GB" or size_units_str == "G":
        multiplier = 2 ** 30
    elif size_units_str == "TB" or size_units_str == "T":
        multiplier = 2 ** 40
    elif size_units_str != '':
        assert False, f"Unexpected size units ({size_units_str}"

    result = float(size_str) * multiplier
    return result


def get_size_in_display_format(size_in_bytes):
    if size_in_bytes < 2**10:
        return str(size_in_bytes) + " B"
    elif size_in_bytes < 2**20:
        size_units_str = "KB"
        divider = 2**10
    elif size_in_bytes < 2 ** 30:
        size_units_str = "MB"
        divider = 2**20
    elif size_in_bytes < 2 ** 40:
        size_units_str = "GB"
        divider = 2**30
    else:
        size_units_str = "TB"
        divider = 2**40

    return f"{float(size_in_bytes)/divider:.1f} {size_units_str}"


class PointerResult(Enum):
    POINTER = auto()
    NULL_POINTER = auto()
    NOT_A_POINTER = auto()


def try_parse_pointer(value_str):
    value_str = value_str.strip()
    if value_str == "(nil)":
        return PointerResult.NULL_POINTER
    else:
        match = re.findall(r'0x[\dA-Fa-f]+', value_str)
        return PointerResult.POINTER if len(match) == 1 else \
            PointerResult.NOT_A_POINTER


def get_options_display(config, options):
    result = dict()

    if config and "Display" in config:
        for option_name in config["Display"].keys():
            if option_name in options:
                result[option_name] = options[option_name]
                pointer_result = try_parse_pointer(result[option_name])
                if pointer_result == PointerResult.POINTER:
                    result[option_name] = ''.join(("Available (", result[
                        option_name], ")"))
                elif pointer_result == PointerResult.NULL_POINTER:
                    result[option_name] = "Not Available"
            else:
                result[option_name] = "<<<<<<< Not Found >>>>>>>"

    return result if result else None


def get_options_diff(ref_options, new_options):
    diff_dict = dict()

    ref_keys_set = set(ref_options.keys())
    new_keys_set = set(new_options.keys())

    missing_options = ref_keys_set - new_keys_set
    if missing_options:
        diff_dict["Missing Options"] = list(missing_options)

    added_options = new_keys_set - ref_keys_set
    if added_options:
        diff_dict["New Options"] = list(added_options)

    common_options_set = ref_keys_set.intersection(new_keys_set)
    for option in common_options_set:
        if ref_options[option] != new_options[option]:
            if "Different Options" not in diff_dict:
                diff_dict["Different Options"] = dict()
            diff_dict["Different Options"][option] =\
                {"ref": ref_options[option],
                 "new": new_options[option]}
    return diff_dict if diff_dict else None


class ParsingError(Exception):
    pass


class FileTypeParsingError(Exception):
    def __init__(self, file_name):
        stream = os.popen(f"file {file_name}")
        self.msg = f"{file_name} [{stream.read().split()[1]}]"


class WarningType(Enum):
    WARN = auto()
    ERROR = auto()
    FATAL = auto()


class LogEntry:
    @staticmethod
    def is_entry_start(log_line):
        token_list = log_line.strip().split()
        if token_list:
            # The assumption is that a new log will start with a date
            return parse_date_time(token_list[0])

    def __init__(self, log_line):
        self.ts = None
        self.thread_id = None
        self.orig_ts = None
        self.warn_msg = None
        self.code_pos = None
        self.msg_lines = None

        self.parse_log_line(log_line)

    def parse_log_line(self, log_line):
        parts = re.findall(LogEntry.START_LINE_WITH_WARN_PARTS_REGEX, log_line)
        if parts:
            num_parts_expected = 6
        else:
            parts = re.findall(LogEntry.START_LINE_PARTS_REGEX, log_line)
            if not parts:
                raise ParsingError(f"Failed parsing Log Entry start line\n"
                                   f"{log_line}")
            num_parts_expected = 5

        assert len(parts) == 1 and len(parts[0]) == num_parts_expected, \
            f"Unepxected parts ({parts})"

        parts = parts[0]
        part_idx = 0

        # Time-Stamp
        self.ts = parse_date_time(parts[part_idx])
        part_idx += 1
        # Thread-Id
        self.thread_id = parts[part_idx]
        part_idx += 1
        # Original Time-Stamp
        self.orig_ts = parts[part_idx]
        part_idx += 1
        # warn msg
        if num_parts_expected == 6:
            self.warn_msg = WarningType[parts[part_idx]]
            part_idx += 1
        # Code Position
        self.code_pos = parts[part_idx]
        if self.code_pos:
            code_pos_value_match = re.findall(r"\[(.*)\]", self.code_pos)
            if code_pos_value_match:
                self.code_pos = code_pos_value_match[0]

        part_idx += 1
        # Rest of entry's text
        self.msg_lines = list()
        if parts[part_idx]:
            self.msg_lines.append(parts[part_idx].strip())

    def add_line(self, log_line):
        self.msg_lines.append(log_line.strip())

    def get_msg_lines(self):
        return self.msg_lines

    def get_msg(self):
        return "\n".join(self.msg_lines)

    def is_warn_msg(self):
        return self.warn_msg

    ORIG_TIME_REGEX = fr"\(Original Log Time ({TIMESTAMP_REGEX})\)"
    CODE_POS_REGEX = fr"\[{FILE_PATH_REGEX}:\d+\]"

    START_LINE_WITH_WARN_PARTS_REGEX =\
        fr"({TIMESTAMP_REGEX}) (\w+)\s*(?:{ORIG_TIME_REGEX})?\s*"\
        fr"\[(WARN|ERROR|FATAL)\]\s*({CODE_POS_REGEX})?(.*)"
    START_LINE_PARTS_REGEX =\
        fr"({TIMESTAMP_REGEX}) (\w+)\s*"\
        fr"(?:{ORIG_TIME_REGEX})?\s*({CODE_POS_REGEX})?(.*)"


class LogMetadata:
    def __init__(self, log_lines, line_num):
        self.start_ts = None
        self.end_ts = None
        self.version = None
        self.git_hash = None
        self.num_lines = 0

        self.parse_log(log_lines, line_num)

    def parse_log(self, log_lines, line_num):
        start_line_num = line_num

        # Version Line
        entry = LogEntry(log_lines[line_num])
        self.version = entry.msg_lines
        self.version = str(self.version).strip()
        self.version = self.version.replace("['", "").replace("']", "")

        line_num += 1

        self.start_ts = entry.ts

        # GIT Hash Line
        entry = LogEntry(log_lines[line_num])
        self.git_hash = entry.msg_lines
        self.git_hash = str(self.git_hash).strip()
        self.git_hash = self.git_hash.replace("['", "").replace("']", "")
        line_num += 1

        # Skip Compile Time Line
        line_num += 1

        self.num_lines = line_num - start_line_num

    def set_end_ts(self, end_ts):
        self.end_ts = end_ts

    def get_num_lines(self):
        return self.num_lines

    def get_log_time_span_seconds(self):
        return (self.end_ts - self.start_ts) / \
               datetime.timedelta(seconds=1)

    def get_info_to_report_dict(self):
        info = dict()

        info["Version"] = self.version
        info["Git Hash"] = self.git_hash
        info["Time Span"] = f"{self.get_log_time_span_seconds():.1f} Seconds"
        info["Start"] = str(self.start_ts)
        info["End"] = str(self.end_ts)

        return info

    COMPILE_DATE_STR = "Compile date:"


@dataclass
class LogLineWithTimestamp:
    date_time: datetime.datetime
    thread_id: str
    msg: str


def parse_log_line_with_timestamp(line):
    parts_delim = " "
    line_parts_list = line.split(parts_delim)
    if len(line_parts_list) < 3:
        return None

    date_time = parse_date_time(line_parts_list[0])
    if not date_time:
        return None

    thread_id = line_parts_list[1]
    msg = parts_delim.join(line_parts_list[2:])

    return LogLineWithTimestamp(date_time, thread_id, msg)


def try_parsing_as_option_line(line):
    log_line_with_ts = parse_log_line_with_timestamp(line)
    if not log_line_with_ts:
        return None

    option_parts_match = re.findall(r"Options\.(.+):\s*(.*)",
                                    log_line_with_ts.msg)
    if len(option_parts_match) != 1 or len(option_parts_match[0]) != 2:
        return None

    return option_parts_match[0]


def is_option_line(line):
    return True if try_parsing_as_option_line(line) \
        else False


class GeneralOptions:
    def __init__(self, config, log_lines, line_num):
        self.config = config
        self.options = dict()

        self.start_line_num = line_num
        while line_num < len(log_lines):
            option_kv = try_parsing_as_option_line(log_lines[line_num])
            if not option_kv:
                break
            self.options[option_kv[0]] = option_kv[1]
            line_num += 1

        self.num_lines = line_num - self.start_line_num

    def get_num_lines(self):
        return self.num_lines

    def get_major_options_to_display(self):
        return get_options_display(self.config, self.options)

    def get_option(self, option_name):
        if option_name in self.options:
            return self.options[option_name]
        else:
            return "Not Found"

    def get_info_to_report_dict(self, ref_general_options):
        info = dict()

        info["DB"] = self.get_major_options_to_display()

        if ref_general_options:
            general_diff = get_options_diff(ref_general_options.options,
                                            self.options)
            if general_diff:
                info["Differences"] = general_diff

        return info


class GeneralInfo:
    def __init__(self):
        self.options = None
        self.events = dict()
        self.db_stats_entries = list()

    def set_options(self, general_options):
        assert type(general_options) is GeneralOptions
        self.options = general_options

    def get_option(self, option_name):
        if self.options:
            return self.options.get_option(option_name)
        else:
            return "NOT FOUND"

    def add_event(self, event_entry):
        assert type(event_entry) is EventEntry
        event_type = event_entry.get_type()
        if event_type not in self.events:
            self.events[event_type] = dict()
        event_ts = event_entry.entry.ts
        self.events[event_type][str(event_ts)] = event_entry

    def add_db_stats_entry(self, ts, db_stats_entry):
        self.db_stats_entries.append(db_stats_entry)

    def calc_flush_started_stats(self):
        total_num_entries = 0
        total_num_deletes = 0
        if "flush_started" in self.events:
            for flush_started_event in self.events["flush_started"].values():
                event_details = flush_started_event.event_details_dict
                total_num_entries += event_details["num_entries"]
                total_num_deletes += event_details["num_deletes"]

        percent_deletes = 0
        if total_num_entries > 0:
            percent_deletes = \
                f'{total_num_deletes / total_num_entries * 100:.1f}%'

        return {"total_num_entries": total_num_entries,
                "total_num_deletes": total_num_deletes,
                "percent_deletes": percent_deletes}

    def get_writes_stats_to_report(self):
        write_stats = dict()
        if self.db_stats_entries:
            last_entry = self.db_stats_entries[-1]
            write_stats["Cumulative Writes"] = \
                last_entry.cumulative_write_entry[1]

            write_stats["Interval Writes"] = dict()
            for entry in self.db_stats_entries:
                write_stats["Interval Writes"][entry.interval_write_entry[
                    0]] = entry.interval_write_entry[1]
        return write_stats

    def get_info_to_report(self, cfs):
        info = dict()

        info["Stats"] = dict()
        info["Stats"]["Size"] = get_size_in_display_format(
            GeneralInfo.get_db_size(cfs))
        info["Stats"]["Writes"] = self.get_writes_stats_to_report()

        info["Events"] = dict()
        for event_type, entries_list in self.events.items():
            key_str = f"{event_type} ({len(entries_list)})"
            for event_ts, event_entry in entries_list.items():
                if key_str not in info["Events"]:
                    info["Events"][key_str] = event_type

        return info

    @staticmethod
    def get_db_size(cfs):
        size_in_bytes = 0
        for cf in cfs.values():
            size_in_bytes += cf.get_cf_size_in_bytes()

        return size_in_bytes


class TableFactoryOptions:
    def __init__(self, config, start_line):
        self.config = config
        self.options = dict()
        self.parse_start_line(start_line)

    @staticmethod
    def is_start_line(line):
        start_match = re.findall(
            TableFactoryOptions.START_LINE_REGEX, line)
        return start_match

    @staticmethod
    def is_option_line(line):
        table_option_match = re.findall(
            TableFactoryOptions.OPTION_LINE_REGEX, line)
        return table_option_match

    def parse_start_line(self, line):
        start_match = re.findall(
            TableFactoryOptions.START_LINE_REGEX, line)
        if len(start_match) != 1 or len(start_match[0]) != 2:
            raise ParsingError(f"Expected table options start line\n{line}")
        self.options[start_match[0][0].strip()] = \
            start_match[0][1].strip()

    def add_line(self, line):
        option_match = re.findall(
            TableFactoryOptions.OPTION_LINE_REGEX, line)
        if len(option_match) != 1 or len(option_match[0]) != 2:
            raise ParsingError(f"Expected table options line\n{line}")
        self.options[option_match[0][0].strip()] = \
            option_match[0][1].strip()

    def get_major_options_to_display(self):
        return get_options_display(self.config, self.options)

    OPTION_LINE_REGEX = r"\s*(\w+)\s*:\s*(.*)"
    START_LINE_REGEX = fr".*table_factory options:{OPTION_LINE_REGEX}"


class CfOptions:
    class ParsingState(Enum):
        INIT = auto()
        IN_OPTIONS = auto()
        IN_TABLE_OPTIONS = auto()
        DONE = auto()

    class LineType(Enum):
        CF_START = auto()
        OPTION = auto()
        TABLE_OPTION_START = auto()
        TABLE_OPTION = auto()
        OTHER = auto()
        CF_END = auto()

    def __init__(self,
                 cf_config,
                 cf_table_factory_config,
                 log_lines,
                 line_num):
        self.cf_config = cf_config
        self.cf_table_factory_config = cf_table_factory_config
        self.name = None
        self.id = None
        self.options = dict()
        self.table_options = None
        self.state = CfOptions.ParsingState.INIT
        self.other_lines = list()
        self.num_lines = 0
        self.implicit_end = False

        start_line_num = line_num

        self.state = CfOptions.ParsingState.INIT

        while line_num < len(log_lines) and \
                self.state != CfOptions.ParsingState.DONE:
            if not self.parse_next_line(log_lines[line_num]):
                line_num += 1

        self.num_lines = line_num - start_line_num

    def get_line_type(self, line):
        if CfOptions.is_start_line(line):
            return CfOptions.LineType.CF_START
        elif self.is_end_line(line):
            return CfOptions.LineType.CF_END
        elif try_parsing_as_option_line(line):
            return CfOptions.LineType.OPTION
        elif TableFactoryOptions.is_start_line(line):
            return CfOptions.LineType.TABLE_OPTION_START
        elif self.state == CfOptions.ParsingState.IN_TABLE_OPTIONS and \
                TableFactoryOptions.is_option_line(line):
            return CfOptions.LineType.TABLE_OPTION
        else:
            return CfOptions.LineType.OTHER

    @staticmethod
    def start_regex():
        return r"--------------- Options for column family \[(.*)\]:.*"

    @staticmethod
    def is_start_line(line):
        match = re.findall(CfOptions.start_regex(), line)
        return len(match) == 1

    def parse_start_line(self, line):
        start_match = re.findall(self.start_regex(), line)
        self.name = start_match[0].strip()

    @staticmethod
    def end_regex(cf_name):
        return fr".*(?:Created)?\s*[cC]olumn family \[{cf_name}\] \(ID ([\
        0-9]+)\)"

    def is_end_line(self, line):
        match = re.findall(self.end_regex(self.name), line)
        return len(match) == 1

    def parse_end_line(self, line):
        match = re.findall(self.end_regex(self.name), line)
        self.set_id(match[0].strip())

    def parse_option_line(self, line):
        option_kv = try_parsing_as_option_line(line)
        if not option_kv:
            raise ParsingError(f"Expected option line:\n{line}")
        self.options[option_kv[0]] = option_kv[1]

    def parse_table_options_start(self, line):
        assert not self.table_options, f"Already has table options\n{line}"
        self.table_options =\
            TableFactoryOptions(self.cf_table_factory_config, line)

    def parse_next_line(self, line):
        implicit_end = False
        line_type = self.get_line_type(line)

        while True:
            if self.state == CfOptions.ParsingState.INIT:
                assert line_type == CfOptions.LineType.CF_START
                self.parse_start_line(line)
                self.state = CfOptions.ParsingState.IN_OPTIONS
                break

            elif self.state == CfOptions.ParsingState.IN_OPTIONS:
                if line_type == CfOptions.LineType.OPTION:
                    self.parse_option_line(line)
                elif line_type == CfOptions.LineType.TABLE_OPTION_START:
                    self.parse_table_options_start(line)
                    self.state = CfOptions.ParsingState.IN_TABLE_OPTIONS
                elif line_type == CfOptions.LineType.OTHER:
                    self.other_lines.append(line)
                elif line_type == CfOptions.LineType.CF_END:
                    self.parse_end_line(line)
                    self.state = CfOptions.ParsingState.DONE
                # When recovering from a manifest, options for a new cf
                # implicitly mean the end of the previous cf
                elif line_type == CfOptions.LineType.CF_START:
                    self.state = CfOptions.ParsingState.DONE
                    implicit_end = True
                else:
                    assert False, f"Invalid line for In-Options state " \
                                  f"(line_type = {line_type}):\n{line}"
                break

            elif self.state == CfOptions.ParsingState.IN_TABLE_OPTIONS:
                if line_type == CfOptions.LineType.TABLE_OPTION:
                    self.table_options.add_line(line)
                    break
                else:
                    self.state = CfOptions.ParsingState.IN_OPTIONS
                    # 2nd pass - Done with the table factory options
                    continue
            else:
                assert False, f"Invalid state ({self.state})"

        return implicit_end

    def set_id(self, id):
        self.id = id

    def get_name(self):
        return self.name

    def get_num_lines(self):
        return self.num_lines

    def get_other_lines(self):
        return self.other_lines

    def get_option(self, option_name):
        if option_name in self.options:
            return self.options[option_name]
        else:
            return "<<<<<<< Not Found >>>>>>>"

    def get_major_options_to_display(self):
        result = get_options_display(self.cf_config, self.options)
        if not result:
            result = dict()

        table_factory_major_options =\
            self.table_options.get_major_options_to_display()
        if table_factory_major_options:
            result["Table"] = table_factory_major_options

        return result if result else None

    def get_info_to_report_dict(self, ref_cf_options):
        info = dict()

        info["DB"] = self.get_major_options_to_display()

        if ref_cf_options:
            cf_options_diff = \
                get_options_diff(ref_cf_options.options.options,
                                 self.options)
            if cf_options_diff:
                info["Differences"] = cf_options_diff

            table_options_diff = \
                get_options_diff(
                    ref_cf_options.options.table_options.options,
                    self.table_options.options)
            if table_options_diff:
                info["Table Options Differences"] = table_options_diff

        return info


class Cf:
    def __init__(self):
        self.options = None
        self.events = dict()
        self.stats_info = list()
        self.cache_stats = dict()

    def set_options(self, cf_options):
        self.options = cf_options

    def get_option(self, option_name):
        if option_name in self.options.options:
            return self.options.options[option_name]
        else:
            return "Not Found"

    def get_table_option(self, option_name):
        if option_name in self.options.table_options.options:
            return self.options.table_options.options[option_name]
        else:
            return "Not Found"

    def add_event(self, event_entry):
        assert type(event_entry) is EventEntry
        event_type = event_entry.get_type()
        if event_type not in self.events:
            self.events[event_type] = dict()
        event_ts = event_entry.entry.ts
        self.events[event_type][str(event_ts)] = event_entry

    def get_info_to_report_dict(self):
        info = dict()

        info["Stats"] = dict()
        info["Stats"]["Size"] = get_size_in_display_format(
            self.get_cf_size_in_bytes())
        info["Stats"]["Block Cache"] = self.cache_stats
        info["Stats"]["Table Stats"] = self.calc_table_creation_stats()
        info["Stats"]["Flushes"] = self.calc_flush_histogram()
        info["Stats"]["Compactions"] = self.calc_compaction_histogram()

        info["Events"] = dict()

        for event_type, entries_list in self.events.items():
            key_str = f"{event_type} ({len(entries_list)})"
            for event_ts, event_entry in entries_list.items():
                if key_str not in info["Events"]:
                    info["Events"][key_str] = event_type

        return info

    def calc_flush_histogram(self):
        if "flush_started" not in self.events:
            return dict()

        histogram = dict()
        for flush_started in list(self.events["flush_started"].values()):
            flush_reason = flush_started.event_details_dict["flush_reason"]
            if flush_reason not in histogram:
                histogram[flush_reason] = 0
            histogram[flush_reason] += 1

        return histogram

    def calc_compaction_histogram(self):
        if "compaction_started" not in self.events:
            return dict()

        histogram = dict()
        for flush_started in list(self.events["compaction_started"].values()):
            compaction_reason = flush_started.event_details_dict[
                "compaction_reason"]
            if compaction_reason not in histogram:
                histogram[compaction_reason] = 0
            histogram[compaction_reason] += 1

        return histogram

    def try_extract_block_cache_stats(self, ts, stats_info):
        if "Compaction Stats" not in stats_info:
            return
        compaction_stats = stats_info["Compaction Stats"]
        ts_str = str(ts)
        for line in compaction_stats:
            if line.startswith("Block cache"):
                cache_id_match = re.findall(Cf.BLOCK_CACHE_ID_REGEX, line)
                if cache_id_match:
                    cache_id = cache_id_match[0][0]
                    cache_capacity = cache_id_match[0][1]
                    if cache_id not in self.cache_stats:
                        self.cache_stats[cache_id] =\
                            {"Capacity": cache_capacity}
                else:
                    cache_stats_match =\
                        re.findall(Cf.BLOCK_CACHE_STATS_REGEX, line)
                    if cache_stats_match:
                        info_part = cache_stats_match[0]
                        types_match = re.findall(r"([A-Za-z]+)\(", info_part)
                        values_match = re.findall(
                            r"\(([0-9]+,[0-9\.]+ [A-Z]+,[0-9\.]+%)+",
                            info_part)

                        if types_match:
                            for i, type in enumerate(types_match):
                                if type not in self.cache_stats[cache_id]:
                                    self.cache_stats[cache_id][type] = dict()
                                self.cache_stats[cache_id][type][ts_str] = \
                                    values_match[i]

    BLOCK_CACHE_ID_REGEX = r"Block cache LRUCache@(0x.+) capacity: " \
                           r"(.*) collections"
    BLOCK_CACHE_STATS_REGEX =\
        r"Block cache entry stats\(count,size,portion\): (.*)"

    def add_stats(self, ts, stats_info):
        self.try_extract_block_cache_stats(ts, stats_info)
        self.stats_info.append((ts, stats_info))

    def get_cf_size_in_bytes(self):
        num_stats_info_entries = len(self.stats_info)
        if num_stats_info_entries == 0:
            return 0

        last_stats_entry = self.stats_info[num_stats_info_entries - 1]
        last_compaction_stats_lines = last_stats_entry[1]['Compaction Stats']
        for line in last_compaction_stats_lines:
            line_parts = line.split()
            if line_parts[0].strip().startswith("Sum"):
                size = line_parts[2].strip()
                size_units = line_parts[3].strip()
                return get_value_by_unit(size, size_units)

    def calc_table_creation_stats(self):
        table_stats = dict(num_tables_created=0,
                           avg_num_table_entries=0,
                           avg_key_size=0,
                           avg_value_size=0)

        total_num_entries = 0
        total_keys_sizes = 0
        total_values_sizes = 0
        if "table_file_creation" in self.events:
            for creation_event in self.events["table_file_creation"].values():
                table_properties =\
                    creation_event.event_details_dict["table_properties"]
                total_num_entries += table_properties["num_entries"]
                total_keys_sizes += table_properties["raw_key_size"]
                total_values_sizes += table_properties["raw_value_size"]

            num_tables_created = len(self.events["table_file_creation"])
            table_stats["num_tables_created"] = num_tables_created
            table_stats["avg_num_table_entries"] =\
                f"{int(total_num_entries / num_tables_created):,}"
            table_stats["avg_key_size"] =\
                int(total_keys_sizes / total_num_entries)
            table_stats["avg_value_size"] =\
                int(total_values_sizes / total_num_entries)

        return table_stats


def find_start_of_general_options(log_lines, line_num):
    start_line_num = line_num
    while line_num < len(log_lines):
        if is_option_line(log_lines[line_num]):
            return line_num
        line_num += 1

    raise ParsingError(f"Failed finding start of General Options section. "
                       f"start_line_num={start_line_num}")


def find_start_of_next_cf_options(log_lines, line_num):
    while line_num < len(log_lines):
        if CfOptions.is_start_line(log_lines[line_num]):
            return line_num
        line_num += 1

    # We will get here when we have passed the last cf options.
    # This is not a parsing error. It's expected
    return None


class DbStatsEntry:
    @staticmethod
    def is_your_entry(log_entry):
        answer = re.findall(r"\s*\*\* DB Stats \*\*", log_entry.get_msg())
        return answer

    def __init__(self, log_entry, cf_names):
        assert DbStatsEntry.is_your_entry(log_entry)
        self.entry = log_entry

        self.general_lines = list()
        self.cumulative_write_entry = None
        self.interval_write_entry = None
        self.cfs = dict()
        section_name = None
        cf_name = None

        lines = log_entry.get_msg().splitlines()
        entry_ts = log_entry.ts

        for line in lines:
            line = line.strip()
            section_match = re.findall(DbStatsEntry.CF_SECTION_REGEX, line)
            if section_match:
                if section_match[0][1] in cf_names:
                    section_name = section_match[0][0]
                    cf_name = section_match[0][1]

                    if cf_name not in self.cfs:
                        self.cfs[cf_name] = dict()
                    if section_name not in self.cfs[cf_name]:
                        self.cfs[cf_name][section_name] = list()

            if cf_name is None:
                if not self.try_add_write_line(entry_ts, line):
                    self.general_lines.append(line)
            else:
                self.cfs[cf_name][section_name].append(line)

    def try_add_write_line(self, entry_ts, line):
        cumulative_str = "Cumulative writes:"
        interval_str = "Interval writes:"
        if line.startswith(cumulative_str) or\
                line.startswith(interval_str):
            ts_str = str(entry_ts)
            if line.startswith("Cumulative writes"):
                line = line[len(cumulative_str):].strip()
                cumulative = True
            else:
                line = line[len(interval_str):].strip()
                cumulative = False

            match = re.findall(self.COMMON_WRITES_REGEX, line)
            assert match and len(match) == 1 and len(match[0]) == 10

            entry = \
                {"writes:":
                 "{get_value_by_unit(match[0][0], match[0][1]):,.0f}",
                 "keys": f"{get_value_by_unit(match[0][2], match[0][3]):,.0f}",
                 "ingest":
                     f"{get_value_by_unit(match[0][7], match[0][8]):,.0f}",
                 "ingest_rate": f"{float(match[0][9])} Mbps"}
            if cumulative:
                self.cumulative_write_entry = (ts_str, entry)
            else:
                self.interval_write_entry = (ts_str, entry)
            return True
        else:
            return False

    COMMON_WRITES_REGEX =\
        r"([\d]+)([KMG]?) writes, ([\d]+)([KMG]?) keys, " \
        r"([\d]+)([KMG]?) commit groups, ([\d+\.]+) " \
        r"writes per commit group, ingest: ([\d+\.]+) " \
        r"(MB|GB|KB), ([\d+\.]+) MB/s"

    def get_general_info(self):
        return self.general_lines

    def get_cfs_info(self):
        return self.cfs

    CF_SECTION_REGEX = r"\*\* (.*) \[(\S*)\] \*\*"


class StatsEntry:
    @staticmethod
    def is_your_entry(log_entry):
        return re.findall(r"\s*STATISTICS:", log_entry.get_msg())

    def __init__(self, log_entry):
        assert StatsEntry.is_your_entry(log_entry)
        self.entry = log_entry


class EventType(Enum):
    COMPACTION = auto()
    FLUSH = auto()
    OTHER = auto()


class EventEntry:
    @staticmethod
    def is_your_entry(log_entry, cf_names):
        # Check if it's an event preamble
        if EventEntry.is_cf_event_preamble(log_entry, cf_names):
            return True
        else:
            return re.findall(r"\s*EVENT_LOG_v1", log_entry.get_msg())

    @staticmethod
    def is_cf_event_preamble(log_entry, cf_names):
        return EventEntry.try_parse_cf_event_preamble(log_entry, cf_names)

    @staticmethod
    def try_parse_cf_event_preamble(log_entry, cf_names):
        cf_preamble_match = re.findall(r"\[(.*?)\] \[JOB ([0-9]+)\]\s*(.*)",
                                       log_entry.get_msg())
        if not cf_preamble_match:
            return None
        cf_name = cf_preamble_match[0][0]
        if cf_name not in cf_names:
            return None

        job_id = cf_preamble_match[0][1]
        rest_of_msg = cf_preamble_match[0][2].strip()
        if rest_of_msg.startswith('Compacting '):
            event_type = EventType.COMPACTION
        elif rest_of_msg.startswith('Flushing memtable'):
            event_type = EventType.FLUSH
        else:
            return None

        return {"cf_name": cf_name,
                "type": event_type,
                "job_id": job_id}

    def __init__(self, log_entry, cf_names):
        assert EventEntry.is_your_entry(log_entry, cf_names)

        self.entry = None
        self.event_details_dict = None
        self.cf_preamble_info = \
            EventEntry.try_parse_cf_event_preamble(log_entry, cf_names)

        if not self.cf_preamble_info:
            self.entry = log_entry
            entry_msg = self.entry.get_msg()
            event_json_str = entry_msg[entry_msg.find("{"):]
            self.event_details_dict = json.loads(event_json_str)

    def get_type(self):
        return self.event_details_dict["event"]

    def is_general_event(self):
        return (not self.cf_preamble_info) and\
               ("cf_name" not in self.event_details_dict)

    def is_cf_preamble_event(self):
        return self.cf_preamble_info

    def is_cf_event(self):
        return self.event_details_dict

    def get_cf_name(self):
        if self.cf_preamble_info:
            return self.cf_preamble_info["cf_name"]
        else:
            return self.event_details_dict["cf_name"]

    def try_adding_preamble_event(self, cf_preamble_event):
        assert cf_preamble_event.is_cf_preamble_event()

        if int(cf_preamble_event.cf_preamble_info["job_id"]) !=\
                self.event_details_dict["job"]:
            return False
        preamble_type = cf_preamble_event.cf_preamble_info["type"]
        if self.event_details_dict["event"] == "flush_started" and\
                preamble_type != EventType.FLUSH:
            return False
        elif self.event_details_dict["event"] == "compaction_started" and\
                preamble_type != EventType.COMPACTION:
            return False
        else:
            # Add the cf_name as if it was part of the event
            self.event_details_dict["cf_name"] = \
                cf_preamble_event.cf_preamble_info["cf_name"]
            return True


def read_config_file(config_file_name):
    if config_file_name and not os.path.isfile(config_file_name):
        print(f"Configuration File ({config_file_name}) Not Found")
        return None

    config_parser = configparser.ConfigParser(allow_no_value=True)

    if config_file_name:
        config_parser.read(config_file_name)
    else:
        buf = io.StringIO(get_default_config())
        config_parser.read_file(buf)

    return config_parser


class WarningsMngr:
    def __init__(self):
        self.cfs = None

        self.global_warnings = dict()
        self.cfs_warnings = dict()

        self.num_warnings = {
            WarningType.WARN.name: 0,
            WarningType.ERROR.name: 0
        }
        self.num_stalls = 0
        self.num_stops = 0

    def set_cfs(self, cfs):
        self.cfs = cfs

    def add_warn_entry(self, msg_entry):
        warning_type_str = msg_entry.warning_type.name
        warning_type_id = msg_entry.code_pos
        warn_msg = msg_entry.get_msg().strip()
        warn_ts_str = str(msg_entry.ts)

        cf_warn_found = False
        if self.cfs:
            for cf_name in self.cfs.keys():
                if warn_msg.startswith(f"[{cf_name}]"):
                    if cf_name not in self.cfs_warnings:
                        self.cfs_warnings[cf_name] = dict()
                    cf_warnings = self.cfs_warnings[cf_name]
                    if warning_type_str not in cf_warnings:
                        cf_warnings[warning_type_str] = dict()
                    cf_warnings_of_type = cf_warnings[warning_type_str]
                    if warning_type_id not in cf_warnings_of_type:
                        cf_warnings_of_type[warning_type_id] = dict()
                    cf_warnings_for_id = cf_warnings_of_type[warning_type_id]
                    warn_msg = warn_msg.replace(f"[{cf_name}]", "", 1)
                    cf_warnings_for_id[warn_ts_str] = warn_msg.strip()
                    cf_warn_found = True
                    break

        if not cf_warn_found:
            if warning_type_str not in self.global_warnings:
                self.global_warnings[warning_type_str] = dict()
            if warning_type_id not in self.global_warnings[warning_type_str]:
                self.global_warnings[warning_type_str][warning_type_id] = \
                    dict()
            warn_msg = warn_msg.strip()
            self.global_warnings[warning_type_str][warning_type_id][
                warn_ts_str] = warn_msg

        self.num_warnings[warning_type_str] += 1
        if self.is_stall_msg(warn_msg):
            self.num_stalls += 1
        elif self.is_stop_msg(warn_msg):
            self.num_stops += 1

    @staticmethod
    def is_stall_msg(warn_msg):
        return warn_msg.strip().startswith("Stalling writes")

    @staticmethod
    def is_stop_msg(warn_msg):
        return warn_msg.strip().startswith("Stopping writes")

    def get_total_num_warns(self):
        return self.num_warnings[WarningType.WARN.name]

    def get_total_num_errors(self):
        return self.num_warnings[WarningType.ERROR.name]

    def get_num_stalls(self):
        return self.num_stalls

    def get_num_stops(self):
        return self.num_stops

    def get_display_info(self):
        cfs_display_info = dict()
        for cf_idx, (cf_name, display_dict) in enumerate(
                self.cfs_warnings.items()):
            cfs_display_info[str(cf_idx)] =\
                dict({"name": cf_name},
                     **display_dict)
        return {"Genearl": self.global_warnings,
                "CF-s": cfs_display_info}


class StatsMngr:
    def __init__(self):
        self.counters = dict()
        self.histograms = dict()

    def add_entry(self, stats_entry):
        for line in stats_entry.msg_lines:
            if self.parse_counter_line(stats_entry, line):
                continue
            else:
                self.parse_histogram_line(stats_entry, line)

    def parse_counter_line(self, stats_entry, line):
        counter_match = re.findall(r"([\w\.]+) COUNT : (\d+)\s*$", line)
        if counter_match:
            assert len(counter_match) == 1 and len(counter_match[0]) == 2,\
                f"Error parsing counter line\n{line}"

            counter_name = counter_match[0][0]
            counter_value = int(counter_match[0][1])
            if counter_value != 0:
                if counter_name not in self.counters:
                    self.counters[counter_name] = dict()
                self.counters[counter_name][str(stats_entry.ts)] = \
                    counter_value
            return True

        return False

    def parse_histogram_line(self, stats_entry, line):
        FR = r"[\d\.]+"
        hist_match =\
            re.findall(fr"([\w\.]+) (P50 : {FR} P95 : {FR} P99 : "
                       fr"{FR} P100 : {FR} COUNT : (\d+) SUM : (\d+))",
                       line)

        if hist_match:
            assert len(hist_match) == 1 and len(hist_match[0]) == 4,\
                f"Error parsing Histogram line\n{line}"

            counter_name = hist_match[0][0]
            count_value = int(hist_match[0][2])
            sum_value = int(hist_match[0][3])

            if count_value != 0:
                if counter_name not in self.histograms:
                    self.histograms[counter_name] = dict()
                average = sum_value / count_value
                self.histograms[counter_name][str(stats_entry.ts)] =\
                    {"Average": f"{average:.1f}",
                     "Entry": hist_match[0][1]}
            return True

        return False

    def get_last_counter_entry(self, counter_name):
        if not self.counters:
            return 0
        if counter_name not in self.counters:
            return 0

        return list(self.counters[counter_name].values())[-1]

    def get_user_opers_stats(self):
        if not self.counters:
            return None

        num_written =\
            self.get_last_counter_entry("rocksdb.number.keys.written")
        num_read = self.get_last_counter_entry("rocksdb.number.keys.read")
        num_seek = self.get_last_counter_entry("rocksdb.number.db.seek")

        num_total = num_written + num_read + num_seek
        percent_written = f'{num_written / num_total * 100:.1f}%'
        percent_read = f'{num_read / num_total * 100:.1f}%'
        percent_seek = f'{num_seek / num_total * 100:.1f}%'

        return {"num_total": num_total,
                "num_written": num_written,
                "percent_written": percent_written,
                "num_read": num_read,
                "percent_read": percent_read,
                "num_seek": num_seek,
                "percent_seek": percent_seek}

    def prepare_counters_for_display(self):
        for counter_name in self.counters.keys():
            if len(self.counters[counter_name]) > 1:
                orig_dict = self.counters[counter_name]
                self.counters[counter_name] = dict()
                last_entry = list(orig_dict.items())[-1]
                self.counters[counter_name]["Last (" + last_entry[0] + ")"] =\
                    last_entry[1]
                self.counters[counter_name]["Entries"] = orig_dict

    def prepare_histograms_for_display(self):
        for counter_name in self.histograms.keys():
            if len(self.histograms[counter_name]) > 1:
                orig_dict = self.histograms[counter_name]
                self.histograms[counter_name] = dict()
                histogram = self.histograms[counter_name]
                last_entry = list(orig_dict.items())[-1]
                histogram["Last (" + last_entry[0] + ")"] = last_entry[1]
                histogram["Entries"] = orig_dict

    def is_empty(self):
        return False if (self.counters or self.histograms) else True

    def get_display_info(self):
        self.prepare_counters_for_display()
        self.prepare_histograms_for_display()

        return {"Counters": self.counters, "Histograms": self.histograms}


class ParsedLog:
    def __init__(self, log_file_name, log_lines, config_file_name):
        self.log_file_name = log_file_name
        self.log_lines = log_lines
        self.line_num = 0
        self.metadata = None
        self.general = None
        self.cfs = dict()
        self.other_log_lines = list()
        self.log_entries = list()
        self.stats_mngr = StatsMngr()
        self.preamble_events = list()
        self.warnings_mngr = WarningsMngr()
        self.other_entries = list()
        self.config_general_options = None
        self.cf_config_options = None
        self.cf_table_config_options = None
        self.last_entry_ts = None

        self.parse_config(config_file_name)

    def parse_config(self, config_file_name):
        config_parser = read_config_file(config_file_name)

        for section in config_parser.sections():
            if section.startswith("General.Options."):
                if not self.config_general_options:
                    self.config_general_options = dict()
                self.config_general_options[
                    section.lstrip("General.Options.")] = \
                    dict(config_parser.items(section))
            elif section.startswith("Cf.Options.TableFactory."):
                if not self.cf_table_config_options:
                    self.cf_table_config_options = dict()
                self.cf_table_config_options[
                    section.lstrip("Cf.Options.TableFactory.")] = \
                    dict(config_parser.items(section))
            elif section.startswith("Cf.Options"):
                if not self.cf_config_options:
                    self.cf_config_options = dict()
                self.cf_config_options[
                    section.lstrip("Cf.Options.")] = \
                    dict(config_parser.items(section))

    def parse_log(self):
        try:
            self.parse_metadata()
            self.parse_general_options()
            self.parse_all_cfs()
            self.parse_rest_of_log()

        except ParsingError as e:
            print(f"Error parsing log\n{e}")

    def parse_metadata(self):
        self.metadata = LogMetadata(self.log_lines, self.line_num)
        self.line_num += self.metadata.num_lines

    def parse_general_options(self):
        self.line_num = find_start_of_general_options(self.log_lines,
                                                      self.line_num)
        self.general = GeneralInfo()
        general_options = GeneralOptions(self.config_general_options,
                                         self.log_lines,
                                         self.line_num)
        self.general.set_options(general_options)
        self.line_num += general_options.get_num_lines()

    def parse_cf(self, start_line_num):
        self.line_num = start_line_num
        cf_options = CfOptions(self.cf_config_options,
                               self.cf_table_config_options,
                               self.log_lines,
                               self.line_num)
        cf_name = cf_options.get_name()
        if cf_name in self.cfs:
            raise ParsingError(f"CF Already Parsed ({cf_name})")

        cf = Cf()
        cf.set_options(cf_options)
        self.cfs[cf_name] = cf
        self.line_num += cf_options.get_num_lines()
        self.other_log_lines += cf_options.get_other_lines()

    def parse_all_cfs(self):
        while self.line_num < len(self.log_lines):
            next_cf_options_line_num = \
                find_start_of_next_cf_options(self.log_lines,
                                              self.line_num)
            if next_cf_options_line_num:
                self.parse_cf(next_cf_options_line_num)
            else:
                # Done processing CF-s
                self.other_log_lines += self.log_lines[self.line_num:]
                break

    def parse_rest_of_log_to_log_entries(self):
        curr_log_entry = None
        for line in self.other_log_lines:
            if LogEntry.is_entry_start(line):  # if starts with date
                # The start of a new entry is the end of the previous one
                if curr_log_entry:
                    self.log_entries.append(curr_log_entry)

                curr_log_entry = LogEntry(line)
            else:
                # To account for logs split into multiple lines
                curr_log_entry.add_line(line)

        # End of log => finalize the last entry
        if curr_log_entry:
            self.log_entries.append(curr_log_entry)

        if self.log_entries:
            last_entry = self.log_entries[-1:][0]
            self.metadata.set_end_ts(last_entry.ts)

    def process_db_stats(self, log_entry):
        ts = log_entry.ts
        db_stats_entry = DbStatsEntry(log_entry, self.cfs.keys())
        self.general.add_db_stats_entry(ts, db_stats_entry)

        cfs_stats = db_stats_entry.get_cfs_info()
        for cf_name, cf_stats in cfs_stats.items():
            self.cfs[cf_name].add_stats(ts, cf_stats)

    def process_event(self, log_entry):
        event_entry = EventEntry(log_entry, self.get_cf_names())
        if event_entry.is_cf_preamble_event():
            self.preamble_events.append(event_entry)
        else:
            for preamble_event in self.preamble_events:
                if event_entry.try_adding_preamble_event(preamble_event):
                    self.preamble_events.remove(preamble_event)
                    break

            if event_entry.is_general_event():
                self.general.add_event(event_entry)
            else:
                assert event_entry.is_cf_event()
                self.cfs[event_entry.get_cf_name()].add_event(event_entry)

    def process_log_entries(self):
        for i, entry in enumerate(self.log_entries):
            if entry.is_warn_msg():
                self.warnings_mngr.add_warn_entry(entry)
            if DbStatsEntry.is_your_entry(entry):
                self.process_db_stats(entry)
            elif StatsEntry.is_your_entry(entry):
                self.stats_mngr.add_entry(entry)
            elif EventEntry.is_your_entry(entry, self.get_cf_names()):
                self.process_event(entry)
            else:
                self.other_entries.append(entry)

    def parse_rest_of_log(self):
        self.warnings_mngr.set_cfs(self.cfs)
        self.parse_rest_of_log_to_log_entries()
        self.process_log_entries()

    def get_db_size(self):
        return GeneralInfo.get_db_size(self.cfs)

    def get_cf_names(self):
        return list(self.cfs.keys())

    def get_general_info_to_report_dict(self):
        metadata_json = self.metadata.get_info_to_report_dict()
        general_json = self.general.get_info_to_report(self.cfs)
        return dict(**metadata_json, **general_json)


def parse_log(log_file_name, config_file_name):
    if not os.path.isfile(log_file_name):
        raise FileTypeParsingError(log_file_name)

    with open(log_file_name) as log:
        log_lines = log.readlines()
        parsed_log = ParsedLog(log_file_name, log_lines, config_file_name)
        parsed_log.parse_log()

        return parsed_log


def get_json(baseline_parsed_log, new_parsed_log):
    j = dict()

    j["name"] = new_parsed_log.log_file_name

    # General
    info = new_parsed_log.get_general_info_to_report_dict()

    baseline = dict()
    if baseline_parsed_log:
        baseline = \
            {"Baseline": {"version": str(baseline_parsed_log.metadata.version),
                          "git_hash": baseline_parsed_log.metadata.git_hash}}

    general_info = new_parsed_log.get_general_info_to_report_dict()
    j["General"] = dict(info, **baseline, **general_info)

    # Options
    j["Options"] =\
        new_parsed_log.general.options.get_info_to_report_dict(
            baseline_parsed_log.general.options
            if baseline_parsed_log else None)

    j["Options"]["CF-s"] = dict()
    for cf_idx, (cf_name, cf) in enumerate(new_parsed_log.cf_names.items()):
        j["Options"]["CF-s"][cf_idx] = \
            dict({"name": cf_name},
                 **new_parsed_log.cf_names[cf_name].options.
                 get_info_to_report_dict(
                     baseline_parsed_log.cf_names["default"] if
                     baseline_parsed_log else None))

    # Per CF Info
    j["CF-s"] = dict()
    for cf_idx, (cf_name, cf) in enumerate(new_parsed_log.cf_names.items()):
        j["CF-s"][cf_idx] = dict({"name": cf_name},
                                 **cf.get_info_to_report_dict())

    j["Warnings"] = new_parsed_log.warnings_mngr.get_display_info()

    j["Statistics"] = new_parsed_log.stats_mngr.get_display_info()

    return j


def write_json(json_file_name, json_content):
    with open(json_file_name, 'w') as json_file:
        json.dump(json_content, json_file)


def get_cf_console_printout(parsed_log,
                            compaction_styles,
                            compressions,
                            filter_policies,
                            width):
    f = io.StringIO()

    num_cfs = len(parsed_log.cf_names)
    all_cfs_stats = dict()
    for cf_name, cf in parsed_log.cf_names.items():
        all_cfs_stats[cf_name] = cf.calc_cf_table_creation_stats()

    indent = 2
    print(f"{'Num CF-s'.ljust(width)}: {num_cfs}", file=f)
    cf_width = max([len(cf_name) for cf_name in list(all_cfs_stats.keys())])
    for cf_name, cf_stats in all_cfs_stats.items():
        cf_title = ' '*indent + (cf_name).ljust(cf_width)
        key_size = get_size_in_display_format(cf_stats['avg_key_size'])
        value_size = get_size_in_display_format(cf_stats['avg_value_size'])

        cf_line = f"{cf_title}: Key:{key_size}  Value:{value_size}  "
        if compaction_styles:
            cf_line += f"Compaction:{compaction_styles[cf_name]}  "
        if compressions:
            cf_line += f"Compression:{compressions[cf_name]}  "
        if filter_policies:
            cf_line += f"Filter-Policy:{filter_policies[cf_name]}  "

        print(cf_line, file=f)

    return f.getvalue()


def get_console_output_title(parsed_log):
    metadata = parsed_log.metadata
    log_time_span = metadata.get_log_time_span_seconds()
    title = f"{parsed_log.log_file_name} " \
            f"({log_time_span:.1f} Seconds) " \
            f"[{str(metadata.start_ts)} - " \
            f"{str(metadata.end_ts)}]"
    return title


def get_console_printout(parsed_log):
    f = io.StringIO()

    title = get_console_output_title(parsed_log)
    print(f"{title}", file=f)
    print(len(title) * "=", file=f)

    version = parsed_log.metadata.version
    git_hash = parsed_log.metadata.git_hash
    num_warns = parsed_log.warnings_mngr.get_total_num_warns()
    num_errors = parsed_log.warnings_mngr.get_total_num_errors()
    num_stalls = parsed_log.warnings_mngr.get_num_stalls()
    num_stops = parsed_log.warnings_mngr.get_num_stops()
    db_size = parsed_log.get_db_size()
    num_trivial_moves = 0
    if "trivial_move" in parsed_log.general.events:
        num_trivial_moves = len(parsed_log.general.events["trivial_move"])
    compaction_styles = dict()
    compressions = dict()
    filter_policies = dict()

    flush_started_stats = parsed_log.general.calc_flush_started_stats()

    for cf_name, cf in parsed_log.cf_names.items():
        compaction_styles[cf_name] = cf.get_option("compaction_style")
        compressions[cf_name] = cf.get_option("compression")
        filter_policies[cf_name] = cf.get_table_option("filter_policy")

    if len(set(compaction_styles.values())) == 1:
        compaction_style = list(compaction_styles.values())[0]
        compaction_styles = None
    else:
        compaction_style = None

    if len(set(compressions.values())) == 1:
        compression = list(compressions.values())[0]
        compressions = None
    else:
        compression = None

    if len(set(filter_policies.values())) == 1:
        filter_policy = list(filter_policies.values())[0]
        filter_policies = None
    else:
        filter_policy = None

    wal_dir = parsed_log.general.get_option("wal_dir")
    has_statistics = not parsed_log.stats_mngr.is_empty()

    width = 25
    print(f"{'Version'.ljust(width)}: {version} [{git_hash}]", file=f)
    print(f"{'Num Warnings'.ljust(width)}: {num_warns}", file=f)
    print(f"{'Num Errors'.ljust(width)}: {num_errors}", file=f)
    print(f"{'Num Stalls'.ljust(width)}: {num_stalls}", file=f)
    print(f"{'Num Stops'.ljust(width)}: {num_stops}", file=f)
    print(f"{'DB Size'.ljust(width)}: {get_size_in_display_format(db_size)}",
          file=f)
    print(f"{'Num Trivial Moves'.ljust(width)}: {num_trivial_moves}", file=f)

    if compaction_style:
        print(f"{'Compaction Style'.ljust(width)}:"
              f" {compaction_style}", file=f)

    if compression:
        print(f"{'Compression'.ljust(width)}: {compression}", file=f)

    if filter_policy:
        print(f"{'Filter-Policy'.ljust(width)}: {filter_policy}", file=f)

    print(f"{'WAL'.ljust(width)}: {wal_dir}", file=f)

    if has_statistics:
        opers_stats = parsed_log.stats_mngr.get_user_operations_stats()
        if opers_stats:
            num_total = opers_stats["num_total"]
            print(f"{'Written'.ljust(width)}: {opers_stats['percent_written']}"
                  f" ({opers_stats['num_written']}/{num_total})", file=f)
            print(f"{'Read'.ljust(width)}: {opers_stats['percent_read']} ("
                  f"{opers_stats['num_read']}/{num_total})", file=f)
            print(f"{'Seek'.ljust(width)}: {opers_stats['percent_seek']} ("
                  f"{opers_stats['num_seek']}/{num_total})", file=f)
    else:
        print(f"{'Statistics'.ljust(width)}: Not Available", file=f)

    print(f"{'Deletes (In Flushes)'.ljust(width)}:"
          f" {flush_started_stats['percent_deletes']} "
          f"({flush_started_stats['total_num_deletes']}/"
          f"{flush_started_stats['total_num_entries']})", file=f)

    f.write(get_cf_console_printout(parsed_log,
                                    compaction_styles,
                                    compressions,
                                    filter_policies,
                                    width))

    return f.getvalue()


def print_results_to_console(parsed_log):
    print(get_console_printout(parsed_log))


def get_dir_files(folder_full_name, recursive):
    return glob.glob(folder_full_name, recursive=recursive)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("log_file_name",
                        metavar="[log-file/folder-name]",
                        help="log file to parse or folder containing log "
                             "files")
    parser.add_argument("-j", "--json-file-name",
                        metavar="[json file name]",
                        help="Optional Output json file name. If not "
                             "specified, a minimal output will be displayed "
                             "on the console")
    parser.add_argument("-c", "--config-file_name",
                        help="Configurtaion file name (ini file)",
                        type=str)
    parser.add_argument("-b", "--baseline-file-name",
                        help="Baseline log file name to compare options",
                        type=str)
    parser.add_argument("-r", "--recursive",
                        help="When parsing a folder, whether to "
                             "look for log files recursively or not",
                        action="store_true",
                        default=False)

    cmdline_args = parser.parse_args()

    try:
        baseline_parsed_log = None
        if cmdline_args.baseline_file_name:
            baseline_parsed_log = parse_log(cmdline_args.baseline_file_name,
                                            cmdline_args.config_file_name)
        if os.path.isfile(cmdline_args.log_file_name):
            log_files_list = [cmdline_args.log_file_name]
        else:
            log_files_list = get_dir_files(
                cmdline_args.log_file_name, cmdline_args.recursive)

        f = io.StringIO()
        json_content = dict()

        parsing_failed_files = list()
        non_files_msgs = list()
        for i, full_log_file_name in enumerate(log_files_list):
            try:
                new_parsed_log = parse_log(full_log_file_name,
                                           cmdline_args.config_file_name)
                if cmdline_args.json_file_name:
                    print(f"Parsing {full_log_file_name}")
                    j = get_json(baseline_parsed_log,
                                 new_parsed_log)
                    # json_content[full_log_file_name] = j
                    json_content[str(i)] = j
                else:
                    f.write(get_console_printout(new_parsed_log))
                    print(f"{f.getvalue()}\n")
            except FileTypeParsingError as e:
                if RAISE_EXCEPTION:
                    raise
                non_files_msgs.append(e.msg)

            except Exception:
                if RAISE_EXCEPTION:
                    raise
                parsing_failed_files.append(full_log_file_name)

        if json_content:
            if len(json_content) == 1:
                json_content = list(json_content.values())[0]
            write_json(cmdline_args.json_file_name, json_content)

        if parsing_failed_files:
            failed_files = '\n'.join(parsing_failed_files)
            print(f"\nFAILED PARSING THE FOLLOWING FILES:\n{failed_files}",
                  file=sys.stderr)
        if non_files_msgs:
            non_files = '\n'.join(non_files_msgs)
            print(f"\nNON-FILES:\n{non_files}", file=sys.stderr)

    except FileNotFoundError:
        print(f"Error: {cmdline_args.log_file_name} not found")
        exit(1)
