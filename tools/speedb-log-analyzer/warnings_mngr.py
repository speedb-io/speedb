from dataclasses import dataclass
from log_entry import LogEntry
import defs_and_utils
import regexes


@dataclass
class WarningElementInfo:
    warning_time: str
    cf_name: str
    warning_msg: str

    def __str__(self):
        return f"{self.warning_time} [{self.cf_name}] {self.warning_msg}"


class WarningsMngr:
    def __init__(self, cf_names=[]):
        self.cf_names = cf_names
        self.warnings = {warning_type: {} for warning_type
                         in defs_and_utils.WarningType}
        self.num_stalls = 0
        self.num_stops = 0

    def try_adding_entry(self, entry):
        assert isinstance(entry, LogEntry)

        returned_cf_name = None
        if not entry.is_warn_msg():
            return False, returned_cf_name

        warning_type = entry.get_warning_type()
        warning_type_id = entry.get_code_pos()
        warning_msg = entry.get_msg()
        warning_time = entry.get_time()

        cf_name = defs_and_utils.NO_COL_FAMILY
        for checked_cf_name in self.cf_names:
            cf_prefix = f"[{checked_cf_name}]"
            if warning_msg.startswith(cf_prefix):
                cf_name = checked_cf_name
                assert warning_msg.startswith(cf_prefix)
                warning_msg = warning_msg[len(cf_prefix):].strip()
                break

        if warning_type_id not in self.warnings[warning_type]:
            self.warnings[warning_type][warning_type_id] = list()

        warning_info = WarningElementInfo(warning_time, cf_name, warning_msg)
        self.warnings[warning_type][warning_type_id].append(warning_info)

        if self.is_stall_msg(warning_msg):
            self.num_stalls += 1
        elif self.is_stop_msg(warning_msg):
            self.num_stops += 1

        if cf_name and cf_name != defs_and_utils.NO_COL_FAMILY:
            returned_cf_name = cf_name

        return True, returned_cf_name

    @staticmethod
    def is_stall_msg(warn_msg):
        return warn_msg.strip().startswith(regexes.STALLS_WARN_MSG_PREFIX)

    @staticmethod
    def is_stop_msg(warn_msg):
        return warn_msg.strip().startswith(regexes.STOPS_WARN_MSG_PREFIX)

    def add_cf_name(self, cf_name):
        if cf_name not in self.cf_names:
            self.cf_names.append(cf_name)

    def get_total_num_warns(self):
        return len(self.warnings[defs_and_utils.WarningType.WARN])

    def get_total_num_errors(self):
        return len(self.warnings[defs_and_utils.WarningType.ERROR])

    def get_num_stalls(self):
        return self.num_stalls

    def get_num_stops(self):
        return self.num_stops

    def get_num_stalls_and_stops(self):
        return self.get_num_stalls() + self.get_num_stops()

    def get_all_warnings(self):
        return self.warnings

    def get_warn_warnings(self):
        return self.warnings[defs_and_utils.WarningType.WARN]

    def get_error_warnings(self):
        return self.warnings[defs_and_utils.WarningType.ERROR]
