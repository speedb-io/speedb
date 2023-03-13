import pytest
import defs_and_utils
from log_entry import LogEntry
from events_mngr import Event, EventsMngr, EventType


def create_event_entry(time_str, event_type, cf_name, job_id=100):
    event_line = time_str + " "
    event_line += '7f4a8b5bb700 EVENT_LOG_v1 {"time_micros": '

    time_micros_part = time_str.split(".")[1]
    event_line += str(defs_and_utils.get_gmt_timestamp(time_str)) + \
        time_micros_part + ", "

    event_line += '"event": "' + event_type.value + '", '
    event_line += f'"cf_name": "{cf_name}", '
    event_line += f'"job": {job_id}'
    event_line += '}'

    event_entry = LogEntry(0, event_line, True)
    assert Event.is_an_event_entry(event_entry, [cf_name])
    return event_entry


def verify_expected_events(events_mngr, expected_events_dict):
    # Expecting a dictionary of:
    # {<cf_name>: [<events entries for this cf>]}
    cf_names = list(expected_events_dict.keys())

    # prepare the expected events per (cf, event type)
    expected_cf_events_per_type = dict()
    for name in cf_names:
        expected_cf_events_per_type[name] = {event_type: [] for event_type
                                             in EventType}

    for cf_name, cf_events_entries in expected_events_dict.items():
        expected_cf_events = [Event(event_entry, cf_names) for event_entry
                              in cf_events_entries]
        assert events_mngr.get_cf_events(cf_name) == expected_cf_events

        for event in expected_cf_events:
            expected_cf_events_per_type[cf_name][event.get_type()].append(
                event)

        for event_type in EventType:
            assert events_mngr.get_cf_events_by_type(cf_name, event_type) ==\
                   expected_cf_events_per_type[cf_name][event_type]


@pytest.mark.parametrize("cf_name", ["default", ""])
def test_event(cf_name):
    cf_names = [cf_name]
    event_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                     EventType.FLUSH_FINISHED, cf_name,
                                     35)

    assert Event.is_an_event_entry(event_entry, cf_names)
    assert not Event.try_parse_event_preamble(event_entry, cf_names)

    event = Event(event_entry, cf_names)
    assert event.get_type_str() == "flush_finished"
    assert event.get_type() == EventType.FLUSH_FINISHED
    assert event.get_job_id() == 35
    assert event.get_cf_name() == cf_name
    assert not event.is_db_wide_event()
    assert event.is_cf_event()


@pytest.mark.parametrize("cf_name", ["default", ""])
def test_event_preamble(cf_name):
    preamble_line = f"""2022/04/17-14:42:11.398681 7f4a8b5bb700 
    [/flush_job.cc:333] [{cf_name}] [JOB 8] 
    Flushing memtable with next log file: 5
    """ # noqa

    preamble_line = " ".join(preamble_line.splitlines())

    cf_names = [cf_name, "dummy_cf"]

    preamble_entry = LogEntry(0, preamble_line, True)
    event_entry = create_event_entry("2022/11/24-15:58:17.683316",
                                     EventType.FLUSH_STARTED,
                                     defs_and_utils.NO_COL_FAMILY,
                                     8)

    assert not Event.try_parse_event_preamble(event_entry, ["dummy_cf"])

    preamble_info = Event.try_parse_event_preamble(preamble_entry, cf_names)
    assert preamble_info
    assert preamble_info.job_id == 8
    assert preamble_info.type == EventType.FLUSH_STARTED
    assert preamble_info.cf_name == cf_name

    assert Event.is_an_event_entry(event_entry, cf_names)
    assert not Event.try_parse_event_preamble(event_entry, cf_names)

    event = Event(event_entry, cf_names)
    assert event.get_type_str() == "flush_started"
    assert event.get_type() == EventType.FLUSH_STARTED
    assert event.get_job_id() == 8
    assert event.get_cf_name() == defs_and_utils.NO_COL_FAMILY
    assert event.is_db_wide_event()
    assert not event.is_cf_event()

    assert event.try_adding_preamble_event(preamble_info.type,
                                           preamble_info.cf_name)
    assert event.get_cf_name() == cf_name
    assert not event.is_db_wide_event()
    assert event.is_cf_event()


def test_adding_events_to_events_mngr():
    cf1 = "cf1"
    cf2 = "cf2"
    cf_names = [cf1, cf2]
    events_mngr = EventsMngr(cf_names)

    assert not events_mngr.get_cf_events(cf1)
    assert not events_mngr.get_cf_events_by_type(cf2,
                                                 EventType.FLUSH_FINISHED)

    expected_events_entries = {cf1: [], cf2: []}

    event1_entry = create_event_entry("2022/04/17-14:42:19.220573",
                                      EventType.FLUSH_FINISHED, cf1)
    assert events_mngr.try_adding_entry(event1_entry) == (True, cf1)
    expected_events_entries[cf1] = [event1_entry]
    verify_expected_events(events_mngr, expected_events_entries)

    event2_entry = create_event_entry("2022/04/18-14:42:19.220573",
                                      EventType.FLUSH_STARTED, cf2)
    assert events_mngr.try_adding_entry(event2_entry) == (True, cf2)
    expected_events_entries[cf2] = [event2_entry]
    verify_expected_events(events_mngr, expected_events_entries)

    # Create another cf1 event, but set its time to EARLIER than event1
    event3_entry = create_event_entry("2022/03/17-14:42:19.220573",
                                      EventType.FLUSH_FINISHED, cf1)
    assert events_mngr.try_adding_entry(event3_entry) == (True, cf1)
    # Expecting event3 to be before event1
    expected_events_entries[cf1] = [event3_entry, event1_entry]
    verify_expected_events(events_mngr, expected_events_entries)

    # Create some more cf21 event, later in time
    event4_entry = create_event_entry("2022/05/17-14:42:19.220573",
                                      EventType.COMPACTION_STARTED, cf2)
    event5_entry = create_event_entry("2022/05/17-15:42:19.220573",
                                      EventType.COMPACTION_STARTED, cf2)
    event6_entry = create_event_entry("2022/05/17-16:42:19.220573",
                                      EventType.COMPACTION_FINISHED, cf2)
    assert events_mngr.try_adding_entry(event4_entry) == (True, cf2)
    assert events_mngr.try_adding_entry(event5_entry) == (True, cf2)
    assert events_mngr.try_adding_entry(event6_entry) == (True, cf2)
    expected_events_entries[cf2] = [event2_entry, event4_entry,
                                    event5_entry, event6_entry]
    verify_expected_events(events_mngr, expected_events_entries)
