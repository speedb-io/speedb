from enum import Enum, auto
import re
import defs_and_utils
import regexes
from datetime import timedelta


def is_empty_line(line):
    return re.findall(regexes.EMPTY_LINE_REGEX, line)


def parse_uptime_line(line, allow_mismatch=False):
    line_parts = re.findall(regexes.UPTIME_STATS_LINE_REGEX, line)

    if not line_parts:
        if allow_mismatch:
            return None
        assert line_parts

    total_sec, interval_sec = line_parts[0]
    return float(total_sec), float(interval_sec)


def parse_line_with_cf(line, regex_str, allow_mismatch=False):
    line_parts = re.findall(regex_str, line)

    if not line_parts:
        if allow_mismatch:
            return None
        assert line_parts

    cf_name = line_parts[0]
    return cf_name


class DbWideStatsMngr:
    @staticmethod
    def is_start_line(line):
        return re.findall(regexes.DB_STATS_REGEX, line) != []

    def __init__(self):
        self.stalls = {}

    def add_lines(self, time, db_stats_lines):
        assert len(db_stats_lines) > 0

        self.stalls[time] = {}

        for line in db_stats_lines[1:]:
            if self.try_parse_as_interval_stall_line(time, line):
                continue
            elif self.try_parse_as_cumulative_stall_line(time, line):
                continue

        if not self.stalls[time]:
            del self.stalls[time]

    @staticmethod
    def try_parse_as_stalls_line(regex, line):
        line_parts = re.findall(regex, line)
        if not line_parts:
            return None

        assert len(line_parts) == 1 and len(line_parts[0]) == 5

        hours, minutes, seconds, ms, stall_percent = line_parts[0]
        stall_duration = timedelta(hours=int(hours),
                                   minutes=int(minutes),
                                   seconds=int(seconds),
                                   milliseconds=int(ms))
        return stall_duration, stall_percent

    def try_parse_as_interval_stall_line(self, time, line):
        stall_info = DbWideStatsMngr.try_parse_as_stalls_line(
            regexes.DB_WIDE_INTERVAL_STALL_REGEX, line)
        if stall_info is None:
            return None

        stall_duration, stall_percent = stall_info
        self.stalls[time].update({"interval_duration": stall_duration,
                                  "interval_percent": float(stall_percent)})

    def try_parse_as_cumulative_stall_line(self, time, line):
        stall_info = DbWideStatsMngr.try_parse_as_stalls_line(
            regexes.DB_WIDE_CUMULATIVE_STALL_REGEX, line)
        if stall_info is None:
            return None

        stall_duration, stall_percent = stall_info
        self.stalls[time].update({"cumulative_duration": stall_duration,
                                  "cumulative_percent": float(stall_percent)})

    def get_stalls_entries(self):
        return self.stalls


class CompactionStatsMngr:
    @staticmethod
    def parse_start_line(line, allow_mismatch=False):
        return parse_line_with_cf(line, regexes.COMPACTION_STATS_REGEX,
                                  allow_mismatch)

    @staticmethod
    def is_start_line(line):
        return CompactionStatsMngr.parse_start_line(line, allow_mismatch=True)\
               is not None

    def __init__(self):
        self.level_entries = dict()
        self.priority_entries = dict()

    def add_lines(self, time, cf_name, db_stats_lines):
        db_stats_lines = [line.strip() for line in db_stats_lines]
        assert cf_name ==\
               CompactionStatsMngr.parse_start_line(db_stats_lines[0])

        if db_stats_lines[1].startswith('Level'):
            self.parse_level_lines(time, cf_name, db_stats_lines)
        elif db_stats_lines[1].startswith('Priority'):
            self.parse_priority_lines(time, cf_name, db_stats_lines)
        else:
            assert 0

    def parse_level_lines(self, time, cf_name, db_stats_lines):
        header_line_parts = db_stats_lines[1].split()
        # TODO - The code should adapt to the actual number of columns
        ##### assert len(header_line_parts) == 21
        assert header_line_parts[0] == 'Level' and header_line_parts[1] == \
               "Files" and header_line_parts[2] == "Size"

        found_sum_line = False
        for line in db_stats_lines:
            if line.strip().startswith("Sum"):
                if cf_name not in self.level_entries:
                    self.level_entries[cf_name] = list()
                sum_line_parts = line.split()
                # One more since Size has both a value and a unit
                # TODO - The code should adapt to the actual number of columns
                ########## assert len(sum_line_parts) == 22
                self.level_entries[cf_name].append(
                    {"time": time,
                     "files": sum_line_parts[1],
                     "size":
                         defs_and_utils.get_value_by_unit(sum_line_parts[2],
                                                          sum_line_parts[3])})
                found_sum_line = True
                break

        assert found_sum_line

    def parse_priority_lines(self, time, cf_name, db_stats_lines):
        # TODO - Consider issuing an info message as Redis (e.g.) don not
        #  have any content here
        if len(db_stats_lines) < 4:
            return

        # TODO: Parse when doing something with the data
        pass

    def get_cf_size_bytes(self, cf_name):
        size_bytes = 0

        if cf_name in self.level_entries:
            last_entry = self.level_entries[cf_name][-1]
            size_bytes = last_entry["size"]

        return size_bytes


class BlobStatsMngr:
    @staticmethod
    def parse_blob_stats_line(line, allow_mismatch=False):
        line_parts = re.findall(regexes.BLOB_STATS_LINE_REGEX, line)
        if not line_parts:
            if allow_mismatch:
                return None
            assert line_parts

        file_count, total_size_gb, garbage_size_gb, space_amp = line_parts[0]
        return \
            int(file_count), float(total_size_gb), float(garbage_size_gb), \
            float(space_amp)

    @staticmethod
    def is_start_line(line):
        return \
            BlobStatsMngr.parse_blob_stats_line(line, allow_mismatch=True) \
            is not None

    def __init__(self):
        self.entries = dict()

    def add_lines(self, time, cf_name, db_stats_lines):
        assert len(db_stats_lines) > 0
        line = db_stats_lines[0]

        line_parts = re.findall(regexes.BLOB_STATS_LINE_REGEX, line)
        assert line_parts and len(line_parts) == 1 and len(line_parts[0]) == 4

        components = line_parts[0]
        file_count = int(components[0])
        total_size_bytes =\
            defs_and_utils.get_value_by_unit(components[1], "GB")
        garbage_size_bytes = \
            defs_and_utils.get_value_by_unit(components[2], "GB")
        space_amp = float(components[3])

        if cf_name not in self.entries:
            self.entries[cf_name] = dict()
        self.entries[cf_name][time] = {
            "File Count": file_count,
            "Total Size": total_size_bytes,
            "Garbage Size": garbage_size_bytes,
            "Space Amp": space_amp
        }

    def get_cf_stats(self, cf_name):
        if cf_name not in self.entries:
            return []
        return self.entries[cf_name]


class CfNoFileStatsMngr:
    @staticmethod
    def is_start_line(line):
        return parse_uptime_line(line, allow_mismatch=True)

    def __init__(self):
        self.stall_counts = {}

    def try_parse_as_stalls_count_line(self, time, cf_name, line):
        if not line.startswith(regexes.CF_STALLS_LINE_START):
            return None

        if cf_name not in self.stall_counts:
            self.stall_counts[cf_name] = {}
        # TODO - Redis have compaction stats for the same cf twice - WHY?
        #######assert time not in self.stall_counts[cf_name]
        self.stall_counts[cf_name][time] = {}

        stall_count_and_reason_matches =\
            re.compile(regexes.CF_STALLS_COUNT_AND_REASON_REGEX)
        for match in stall_count_and_reason_matches.finditer(line):
            self.stall_counts[cf_name][time][match[2]] = int(match[1])
        assert self.stall_counts[cf_name][time]

        total_count_match = re.findall(
            regexes.CF_STALLS_INTERVAL_COUNT_REGEX, line)

        # TODO - Last line of Redis's log was cropped in the middle
        ###### assert total_count_match and len(total_count_match) == 1
        if not total_count_match or len(total_count_match) != 1:
            return None

        self.stall_counts[cf_name][time]["interval_total_count"] = \
            int(total_count_match[0])

    def add_lines(self, time, cf_name, stats_lines):
        for line in stats_lines:
            line = line.strip()
            if self.try_parse_as_stalls_count_line(time, cf_name, line):
                continue

    def get_stall_counts(self):
        return self.stall_counts


class CfFileHistogramStatsMngr:
    @staticmethod
    def parse_start_line(line, allow_mismatch=False):
        return parse_line_with_cf(line,
                                  regexes.FILE_READ_LATENCY_STATS_REGEX,
                                  allow_mismatch)

    @staticmethod
    def is_start_line(line):
        return CfFileHistogramStatsMngr.parse_start_line(line,
                                                         allow_mismatch=True)\
               is not None

    def add_lines(self, time, cf_name, db_stats_lines):
        pass


class BlockCacheStatsMngr:
    @staticmethod
    def is_start_line(line):
        return re.findall(regexes.BLOCK_CACHE_STATS_START_REGEX, line)

    def __init__(self):
        self.caches = dict()

    def add_lines(self, time, cf_name, db_stats_lines):
        assert len(db_stats_lines) >= 2
        cache_id = self.parse_cache_id_line(db_stats_lines[0])
        self.parse_entry_stats_line(time, cf_name, cache_id, db_stats_lines[1])

    def parse_cache_id_line(self, line):
        line_parts = re.findall(regexes.BLOCK_CACHE_STATS_START_REGEX, line)
        assert line_parts and len(line_parts) == 1 and len(line_parts[0]) == 3
        cache_id, cache_capacity, capacity_units = line_parts[0]
        capacity_bytes = defs_and_utils.get_value_by_unit(cache_capacity,
                                                          capacity_units)

        if cache_id not in self.caches:
            self.caches[cache_id] = {"Capacity": capacity_bytes}

        return cache_id

    def parse_entry_stats_line(self, time, cf_name, cache_id, line):
        line_parts = re.findall(regexes.BLOCK_CACHE_ENTRY_STATS_REGEX, line)
        assert line_parts and len(line_parts) == 1

        roles_info = line_parts[0]
        roles = re.findall(regexes.BLOCK_CACHE_ENTRY_ROLES_NAMES_REGEX,
                           roles_info)
        roles_stats = re.findall(regexes.BLOCK_CACHE_ENTRY_ROLES_STATS,
                                 roles_info)
        assert len(roles) == len(roles_stats)

        for i, role in enumerate(roles):
            if role not in self.caches[cache_id]:
                self.caches[cache_id][role] = dict()
            role_stats_parts = \
                re.findall(regexes.BLOCK_CACHE_ROLE_STATS_COMPONENTS,
                           roles_stats[i])
            assert role_stats_parts and len(role_stats_parts) == 1 and \
                   len(role_stats_parts[0]) == 4
            count, size, size_unit, portion = role_stats_parts[0]
            size_bytes = defs_and_utils.get_value_by_unit(size, size_unit)

            self.caches[cache_id][role][time] = \
                {"Count": count, "Size": size_bytes, "Portion": portion}

    def get_cache_entries(self, cache_id):
        if cache_id not in self.caches:
            return {}
        return self.caches[cache_id]


class StatsCountersAndHistogramsMngr:
    @staticmethod
    def is_start_line(line):
        return re.findall(regexes.STATS_COUNTERS_AND_HISTOGRAMS_REGEX, line)

    @staticmethod
    def is_your_entry(entry):
        entry_lines = entry.get_msg_lines()
        return StatsCountersAndHistogramsMngr.is_start_line(entry_lines[0])

    def __init__(self):
        # list of counters names in the order of their appearance
        # in the log file (retaining this order assuming it is
        # convenient for the user)
        self.counters_names = []
        self.counters = dict()
        self.histograms = dict()

    def add_entry(self, entry):
        time = entry.get_time()
        lines = entry.get_msg_lines()
        assert StatsCountersAndHistogramsMngr.is_start_line(lines[0])

        for line in lines[1:]:
            is_counter_line = self.try_parse_counter_line(time, line)
            if not is_counter_line:
                assert self.try_parse_histogram_line(time, line)

    def try_parse_counter_line(self, time, line):
        line_parts = re.findall(regexes.STATS_COUNTER_REGEX, line)
        if not line_parts:
            return False
        assert len(line_parts) == 1 and len(line_parts[0]) == 2

        counter_value = int(line_parts[0][1])
        counter_name = line_parts[0][0]
        if counter_name not in self.counters:
            self.counters_names.append(counter_name)
            self.counters[counter_name] = list()
        self.counters[counter_name].append({
            "time": time,
            "value": counter_value})

        return True

    def try_parse_histogram_line(self, time, line):
        line_parts = re.findall(regexes.STATS_HISTOGRAM_REGEX, line)
        if not line_parts:
            return False
        assert len(line_parts) == 1 and len(line_parts[0]) == 7

        components = line_parts[0]

        count = int(components[5])
        total = int(components[6])
        # For some reason there are cases where the count is > 0 but the
        # total is 0 (e.g., 'rocksdb.prefetched.bytes.discarded')
        if count > 0 and total > 0:
            counter_name = components[0]
            if counter_name not in self.histograms:
                self.histograms[counter_name] = list()

            average = float(f"{(count / total):.2f}")

            self.histograms[counter_name].append(
                {"time": time,
                 "values": {"P50": float(components[1]),
                            "P95": float(components[2]),
                            "P99": float(components[3]),
                            "P100": float(components[4]),
                            "Count": count,
                            "Sum": total,
                            "Average": average}})

        return True

    def get_counters_names(self):
        return self.counters_names

    def get_counters_times(self):
        all_entries = self.get_all_counters_entries()
        times = list(
            {counter_entry["time"]
             for counter_entries in all_entries.values()
             for counter_entry in counter_entries})
        times.sort()
        return times

    def get_all_counters_entries(self):
        return self.counters

    def get_counter_entries(self, counter_name):
        if counter_name not in self.counters:
            return {}
        return self.counters[counter_name]

    def get_last_counter_entry(self, counter_name):
        entries = self.get_counter_entries(counter_name)
        if not entries:
            return {}
        return entries[-1]

    def get_last_counter_value(self, counter_name):
        last_entry = self.get_last_counter_entry(counter_name)

        if not last_entry:
            return 0

        return last_entry["value"]

    def get_histogram_entries(self, counter_name):
        if counter_name not in self.histograms:
            return {}
        return self.histograms[counter_name]


class StatsMngr:
    class StatsType(Enum):
        DB_WIDE = auto()
        COMPACTION = auto()
        BLOB = auto()
        BLOCK_CACHE = auto()
        CF_NO_FILE = auto()
        CF_FILE_HISTOGRAM = auto()
        COUNTERS = auto()

    def __init__(self):
        self.db_wide_stats_mngr = DbWideStatsMngr()
        self.compaction_stats_mngr = CompactionStatsMngr()
        self.blob_stats_mngr = BlobStatsMngr()
        self.block_cache_stats_mngr = BlockCacheStatsMngr()
        self.cf_no_file_stats_mngr = CfNoFileStatsMngr()
        self.cf_file_histogram_stats_mngr = CfFileHistogramStatsMngr()
        self.counter_and_histograms_mngr = StatsCountersAndHistogramsMngr()

    @staticmethod
    def is_dump_stats_start(entry):
        return entry.get_msg().startswith(regexes.DUMP_STATS_REGEX)

    @staticmethod
    def find_next_start_line_in_db_stats(db_stats_lines,
                                         start_line_idx,
                                         curr_stats_type):
        line_idx = start_line_idx + 1
        next_stats_type = None
        cf_name = None
        # DB Wide Stats must be the first and were verified above
        while line_idx < len(db_stats_lines) and next_stats_type is None:
            line = db_stats_lines[line_idx]

            if CompactionStatsMngr.is_start_line(line):
                next_stats_type = StatsMngr.StatsType.COMPACTION
                cf_name = CompactionStatsMngr.parse_start_line(line)
            elif BlobStatsMngr.is_start_line(line):
                next_stats_type = StatsMngr.StatsType.BLOB
            elif BlockCacheStatsMngr.is_start_line(line):
                next_stats_type = StatsMngr.StatsType.BLOCK_CACHE
            elif CfFileHistogramStatsMngr.is_start_line(line):
                next_stats_type = StatsMngr.StatsType.CF_FILE_HISTOGRAM
                cf_name = CfFileHistogramStatsMngr.parse_start_line(line)
            elif CfNoFileStatsMngr.is_start_line(line) and \
                    curr_stats_type != StatsMngr.StatsType.DB_WIDE:
                next_stats_type = StatsMngr.StatsType.CF_NO_FILE
            else:
                line_idx += 1

        return line_idx, next_stats_type, cf_name

    def parse_next_db_stats_entry_lines(self, time, cf_name, stats_type,
                                        db_stats_lines, start_line_idx,
                                        end_line_idx):
        assert end_line_idx <= len(db_stats_lines)
        stats_lines_to_parse = db_stats_lines[start_line_idx:end_line_idx]
        stats_lines_to_parse = [line.strip() for line in stats_lines_to_parse]

        if stats_type == StatsMngr.StatsType.DB_WIDE:
            self.db_wide_stats_mngr.add_lines(time, stats_lines_to_parse)
        elif stats_type == StatsMngr.StatsType.COMPACTION:
            self.compaction_stats_mngr.add_lines(time, cf_name,
                                                 stats_lines_to_parse)
        elif stats_type == StatsMngr.StatsType.BLOB:
            self.blob_stats_mngr.add_lines(time, cf_name, stats_lines_to_parse)
        elif stats_type == StatsMngr.StatsType.BLOCK_CACHE:
            self.block_cache_stats_mngr.add_lines(time, cf_name,
                                                  stats_lines_to_parse)
        elif stats_type == StatsMngr.StatsType.CF_NO_FILE:
            self.cf_no_file_stats_mngr.add_lines(time, cf_name,
                                                 stats_lines_to_parse)
        elif stats_type == StatsMngr.StatsType.CF_FILE_HISTOGRAM:
            self.cf_file_histogram_stats_mngr.add_lines(time, cf_name,
                                                        stats_lines_to_parse)
        else:
            assert False, f"Unexpected stats type ({stats_type})"

    def try_adding_entries(self, log_entries, start_entry_idx):
        entry_idx = start_entry_idx

        # Our entries starts with the "------- DUMPING STATS -------" entry
        if not StatsMngr.is_dump_stats_start(log_entries[entry_idx]):
            return False, entry_idx
        entry_idx += 1

        db_stats_entry = log_entries[entry_idx]
        db_stats_lines = db_stats_entry.get_msg_lines()
        db_stats_time = db_stats_entry.get_time()
        # "** DB Stats **" must be next
        assert len(db_stats_lines) > 0
        assert DbWideStatsMngr.is_start_line(db_stats_lines[0])
        entry_idx += 1

        line_num = 0
        stats_type = StatsMngr.StatsType.DB_WIDE
        curr_cf_name = defs_and_utils.NO_COL_FAMILY
        while line_num < len(db_stats_lines):
            next_line_num, next_stats_type, next_cf_name = \
                StatsMngr.find_next_start_line_in_db_stats(db_stats_lines,
                                                           line_num,
                                                           stats_type)
            # parsing must progress
            assert next_line_num > line_num

            if next_cf_name is not None:
                curr_cf_name = next_cf_name
            self.parse_next_db_stats_entry_lines(db_stats_time,
                                                 curr_cf_name,
                                                 stats_type,
                                                 db_stats_lines,
                                                 line_num,
                                                 next_line_num)

            line_num = next_line_num
            stats_type = next_stats_type

        # counters / histograms may or may not be present
        # If they are present, they are contained in a single entry
        # starting with "STATISTICS:"
        if entry_idx < len(log_entries):
            entry = log_entries[entry_idx]
            if StatsCountersAndHistogramsMngr.is_your_entry(entry):
                self.counter_and_histograms_mngr.add_entry(entry)
                entry_idx += 1

        return True, entry_idx

    def get_db_wide_stats_mngr(self):
        return self.db_wide_stats_mngr

    def get_compaction_stats_mngr(self):
        return self.compaction_stats_mngr

    def get_blob_stats_mngr(self):
        return self.blob_stats_mngr

    def get_block_cache_stats_mngr(self):
        return self.block_cache_stats_mngr

    def get_cf_no_file_stats_mngr(self):
        return self.cf_no_file_stats_mngr

    def get_cf_file_histogram_stats_mngr(self):
        return self.cf_file_histogram_stats_mngr

    def get_counter_and_histograms_mngr(self):
        return self.counter_and_histograms_mngr
