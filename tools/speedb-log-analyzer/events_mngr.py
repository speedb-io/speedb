import re
import json
from enum import Enum
from dataclasses import dataclass

import defs_and_utils
import regexes
from log_entry import LogEntry


class EventType(str, Enum):
    FLUSH_STARTED = "flush_started"
    FLUSH_FINISHED = "flush_finished"
    COMPACTION_STARTED = "compaction_started"
    COMPACTION_FINISHED = "compaction_finished"
    TABLE_FILE_CREATION = 'table_file_creation'
    TRIVIAL_MOVE = "trivial_move"
    UNKNOWN = "UNKNOWN"

    @staticmethod
    def get_type(event_type_str):
        try:
            return EventType(event_type_str)
        except ValueError:
            return EventType.UNKNOWN


class Event:
    @dataclass
    class EventPreambleInfo:
        cf_name: str
        type: str
        job_id: str

    @staticmethod
    def is_an_event_entry(log_entry, cf_names):
        assert isinstance(log_entry, LogEntry)
        return re.findall(regexes.EVENT_REGEX, log_entry.get_msg()) != []

    @staticmethod
    def try_parse_event_preamble(log_entry, cf_names):
        cf_preamble_match = re.findall(regexes.PREAMBLE_EVENT_REGEX,
                                       log_entry.get_msg())
        if not cf_preamble_match:
            return None
        cf_name = cf_preamble_match[0][0]
        if cf_name not in cf_names:
            return None

        job_id = int(cf_preamble_match[0][1])
        rest_of_msg = cf_preamble_match[0][2].strip()
        if rest_of_msg.startswith('Compacting '):
            event_type = EventType.COMPACTION_STARTED
        elif rest_of_msg.startswith('Flushing memtable with next log file'):
            event_type = EventType.FLUSH_STARTED
        else:
            return None

        return Event.EventPreambleInfo(cf_name, event_type, job_id)

    def __init__(self, log_entry, cf_names):
        assert Event.is_an_event_entry(log_entry, cf_names)

        entry_msg = log_entry.get_msg()
        event_json_str = entry_msg[entry_msg.find("{"):]
        self.event_details_dict = json.loads(event_json_str)

        if "cf_name" not in self.event_details_dict:
            self.event_details_dict["cf_name"] = defs_and_utils.NO_COL_FAMILY

        self.event_type = EventType.get_type(self.get_type_str())

    # By default, sort events based on their time
    def __lt__(self, other):
        return self.get_time() < other.get_time()

    def __eq__(self, other):
        return self.get_time() == other.get_time() and \
               self.get_type() == other.get_type() and\
               self.get_cf_name() == other.get_cf_name()

    def get_type_str(self):
        return self.get_event_data_by_key("event")

    def get_type(self):
        return self.event_type

    def get_job_id(self):
        return self.get_event_data_by_key("job")

    def get_time(self):
        return self.get_event_data_by_key("time_micros")

    def get_event_data_by_key(self, key):
        assert key in self.event_details_dict
        return self.event_details_dict[key]

    def is_db_wide_event(self):
        return self.get_cf_name() == defs_and_utils.NO_COL_FAMILY

    def is_cf_event(self):
        return not self.is_db_wide_event()

    def get_cf_name(self):
        return self.event_details_dict["cf_name"]

    def try_adding_preamble_event(self, event_preamble_type, cf_name):
        if self.event_type != event_preamble_type:
            return False

        # Add the cf_name as if it was part of the event
        self.event_details_dict["cf_name"] = cf_name
        return True


class EventsMngr:
    """
    The events manager contains all of the events.

    It stores them in a dictionary of the following format:
    <cf-name>: Dictionary of cf events
    (The db-wide events are stored under the "cf name" No_COL_NAME)

    Dictionary of cf events is itself a dictionary of the following format:
    <event-type>: List of Event-s, ordered by their time
    """
    def __init__(self, cf_names):
        self.cf_names = cf_names
        self.preambles = dict()
        self.events = dict()

    def try_parsing_as_preamble(self, entry):
        preamble_info = \
            Event.try_parse_event_preamble(entry, self.cf_names)
        if not preamble_info:
            return False

        (cf_name, event_type, job_id) = (preamble_info.cf_name,
                                         preamble_info.type,
                                         preamble_info.job_id)

        # Assuming no more than one preamble for the same job
        assert job_id not in self.preambles
        self.preambles[job_id] = (event_type, cf_name)

        return True

    def try_adding_entry(self, entry):
        # A preamble event is an entry that will be pending for its
        # associated event entry to provide the event with its cf name
        if self.try_parsing_as_preamble(entry):
            return True

        if not Event.is_an_event_entry(entry, self.cf_names):
            return False

        event = Event(entry, self.cf_names)

        # Combine associated event preamble, if any exists
        event_job_id = event.get_job_id()
        if event_job_id in self.preambles:
            preamble_info = self.preambles[event_job_id]
            if event.try_adding_preamble_event(preamble_info[0],
                                               preamble_info[1]):
                del(self.preambles[event_job_id])

        event_cf_name = event.get_cf_name()
        event_type = event.get_type()

        if event_cf_name not in self.events:
            self.events[event_cf_name] = dict()
        if event_type not in self.events[event_cf_name]:
            self.events[event_cf_name][event_type] = []
        self.events[event_cf_name][event_type].append(event)

        return True

    def get_cf_events(self, cf_name):
        if cf_name not in self.events:
            return []

        all_cf_events = []
        for cf_events in self.events[cf_name].values():
            all_cf_events.extend(cf_events)

        # Return the events sorted by their time
        all_cf_events.sort()
        return all_cf_events

    def get_cf_events_by_type(self, cf_name, event_type):
        assert isinstance(event_type, EventType)

        if cf_name not in self.events:
            return []
        if event_type not in self.events[cf_name]:
            return []

        # The list may not be ordered due to the original time issue
        # or having event preambles matched to their events somehow
        # out of order. Sorting will insure correctness even if the list
        # is already sorted
        events = self.events[cf_name][event_type]
        events.sort()
        return events
