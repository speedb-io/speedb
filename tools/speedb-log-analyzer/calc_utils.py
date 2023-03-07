from events_mngr import EventType
from log_file import ParsedLog


def get_cf_size_bytes(parsed_log: ParsedLog, cf_name):
    stats_mngr = parsed_log.get_stats_mngr()
    compaction_stats_mngr = stats_mngr.get_compaction_stats_mngr()

    return compaction_stats_mngr.get_cf_size_bytes(cf_name)


def get_db_size_bytes(parsed_log: ParsedLog):
    cf_names = parsed_log.get_cf_names()
    stats_mngr = parsed_log.get_stats_mngr()
    compaction_stats_mngr = stats_mngr.get_compaction_stats_mngr()

    db_size_bytes = 0
    for cf_name in cf_names:
        db_size_bytes += compaction_stats_mngr.get_cf_size_bytes(cf_name)

    return db_size_bytes


def calc_cf_table_creation_stats(cf_name, events_mngr):
    creation_events = \
        events_mngr.get_cf_events_by_type(cf_name,
                                          EventType.TABLE_FILE_CREATION)

    total_num_entries = 0
    total_keys_sizes = 0
    total_values_sizes = 0
    for event in creation_events:
        table_properties = event.event_details_dict["table_properties"]
        total_num_entries += table_properties["num_entries"]
        total_keys_sizes += table_properties["raw_key_size"]
        total_values_sizes += table_properties["raw_value_size"]

    num_tables_created = len(creation_events)
    avg_num_table_entries = 0
    avg_key_size = 0
    avg_value_size = 0

    if num_tables_created > 0:
        avg_num_table_entries = int(total_num_entries / num_tables_created)
        avg_key_size = int(total_keys_sizes / total_num_entries)
        avg_value_size = int(total_values_sizes / total_num_entries)

    return {"num_tables_created": num_tables_created,
            "total_num_entries": total_num_entries,
            "total_keys_sizes": total_keys_sizes,
            "total_values_sizes": total_values_sizes,
            "avg_num_table_entries": avg_num_table_entries,
            "avg_key_size": avg_key_size,
            "avg_value_size": avg_value_size}


def calc_flush_started_stats(cf_name, events_mngr):
    flush_started_events = \
        events_mngr.get_cf_events_by_type(cf_name,
                                          EventType.FLUSH_STARTED)

    total_num_entries = 0
    total_num_deletes = 0
    for event in flush_started_events:
        event_details = event.event_details_dict
        total_num_entries += event_details["num_entries"]
        total_num_deletes += event_details["num_deletes"]

    # TODO - Consider setting as -1 / illegal value to indicate no value
    percent_deletes = 0
    if total_num_entries > 0:
        percent_deletes = \
            f'{total_num_deletes / total_num_entries * 100:.1f}%'

    return {"total_num_entries": total_num_entries,
            "total_num_deletes": total_num_deletes,
            "percent_deletes": percent_deletes}


def get_user_operations_stats(counters_and_histograms_mngr):
    # TODO - Handle the case where stats aren't availabe
    mngr = counters_and_histograms_mngr
    num_written = mngr.get_last_counter_value("rocksdb.number.keys.written")
    num_read = mngr.get_last_counter_value("rocksdb.number.keys.read")
    num_seek = mngr.get_last_counter_value("rocksdb.number.db.seek")
    total_num_user_opers = num_written + num_read + num_seek

    percent_written = 0
    percent_read = 0
    percent_seek = 0
    if total_num_user_opers > 0:
        percent_written = f'{num_written / total_num_user_opers * 100:.1f}%'
        percent_read = f'{num_read / total_num_user_opers * 100:.1f}%'
        percent_seek = f'{num_seek / total_num_user_opers * 100:.1f}%'

    return {
        "num_written": num_written,
        "num_read": num_read,
        "num_seek": num_seek,
        "total_num_user_opers": total_num_user_opers,
        "percent_written": percent_written,
        "percent_read": percent_read,
        "percent_seek":  percent_seek
    }


def get_db_wide_info(parsed_log: ParsedLog):
    metadata = parsed_log.get_metadata()
    warns_mngr = parsed_log.get_warnings_mngr()
    user_operations_stats = get_user_operations_stats(
        parsed_log.get_stats_mngr().get_counter_and_histograms_mngr())

    total_num_table_created_entries = 0
    total_num_flushed_entries = 0
    total_num_deletes = 0
    total_keys_sizes = 0
    total_values_size = 0

    events_mngr = parsed_log.get_events_mngr()
    for cf_name in parsed_log.get_cf_names():
        table_creation_stats = calc_cf_table_creation_stats(cf_name,
                                                            events_mngr)
        total_num_table_created_entries += \
            table_creation_stats["total_num_entries"]
        total_keys_sizes += table_creation_stats["total_keys_sizes"]
        total_values_size += table_creation_stats["total_values_sizes"]

        flush_started_stats = calc_flush_started_stats(cf_name, events_mngr)
        total_num_flushed_entries += flush_started_stats['total_num_entries']
        total_num_deletes += flush_started_stats['total_num_deletes']

    total_percent_deletes = 0
    if total_num_flushed_entries > 0:
        total_percent_deletes = \
            f'{total_num_deletes / total_num_flushed_entries * 100:.1f}%'

    # TODO - Add unit test when total_num_table_created_entries == 0
    # TODO - Consider whether this means data is not available
    avg_key_size_bytes = 0
    avg_value_size_bytes = 0
    if total_num_table_created_entries > 0:
        avg_key_size_bytes =\
            int(total_keys_sizes / total_num_table_created_entries)
        avg_value_size_bytes = \
            int(total_values_size / total_num_table_created_entries),

    info = {
        "version": metadata.get_version(),
        "git_hash": metadata.get_git_hash(),
        "db_size_bytes": get_db_size_bytes(parsed_log),
        "num_cfs": len(parsed_log.get_cf_names()),
        "avg_key_size_bytes": avg_key_size_bytes,
        "avg_value_size_bytes": avg_value_size_bytes,
        "num_warns": warns_mngr.get_total_num_warns(),
        "num_errors": warns_mngr.get_total_num_errors(),
        "num_stalls": warns_mngr.get_num_stalls_and_stops(),
        "total_num_table_created_entries": total_num_table_created_entries,
        "total_num_flushed_entries": total_num_flushed_entries,
        "total_num_deletes": total_num_deletes,
        "total_percent_deletes": total_percent_deletes
    }
    info.update(user_operations_stats)

    return info


def calc_event_histogram(cf_name, events_mngr, event_type, group_by_field):
    events = events_mngr.get_cf_events_by_type(cf_name, event_type)

    histogram = dict()
    for event in events:
        event_grouping = event.event_details_dict[group_by_field]
        if event_grouping not in histogram:
            histogram[event_grouping] = 0
        histogram[event_grouping] += 1

    return histogram


def calc_flushes_histogram(cf_name, events_mngr):
    return calc_event_histogram(cf_name,
                                events_mngr,
                                EventType.FLUSH_STARTED,
                                "flush_reason")


def calc_compactions_histogram(cf_name, events_mngr):
    return calc_event_histogram(cf_name,
                                events_mngr,
                                EventType.COMPACTION_STARTED,
                                "compaction_reason")


def calc_all_events_histogram(cf_names, events_mngr):
    # Returns a dictionary of:
    # {<cf_name>: {<event_type>: [events]}}   # noqa
    histogram = {}

    for cf_name in cf_names:
        for event_type in EventType:
            cf_events_of_type = events_mngr.get_cf_events_by_type(cf_name,
                                                                  event_type)
            if cf_events_of_type:
                histogram[cf_name] = cf_events_of_type
    return histogram


def is_cf_compression_by_level(parsed_log, cf_name):
    db_options = parsed_log.get_database_options()
    return db_options.get_cf_option(cf_name, "compression[0]") is not None


def get_applicable_cf_options(parsed_log: ParsedLog):
    cf_names = parsed_log.get_cf_names()
    db_options = parsed_log.get_database_options()
    cfs_options = {"compaction_style": {},
                   "compression": {},
                   "filter_policy": {}}

    for cf_name in cf_names:
        cfs_options["compaction_style"][cf_name] = \
            db_options.get_cf_option(cf_name, "compaction_style")
        cfs_options["compression"][cf_name] = \
            db_options.get_cf_option(cf_name, "compression")
        cfs_options["filter_policy"][cf_name] = \
            db_options.get_cf_table_option(cf_name, "filter_policy")

    compaction_styles = list(set(cfs_options["compaction_style"].values()))
    if len(compaction_styles) == 1 and compaction_styles[0] is not None:
        common_compaction_style = compaction_styles[0]
    else:
        common_compaction_style = "Per Column Family"
    cfs_options["compaction_style"]["common"] = common_compaction_style

    compressions = list(set(cfs_options["compression"].values()))
    if len(compressions) == 1 and compressions[0] is not None:
        common_compression = compressions[0]
    else:
        common_compression = "Per Column Family"
    cfs_options["compression"]["common"] = common_compression

    filter_policies = list(set(cfs_options["filter_policy"].values()))
    if len(filter_policies) == 1 and filter_policies[0] is not None:
        common_filter_policy = filter_policies[0]
    else:
        common_filter_policy = "Per Column Family"
    cfs_options["filter_policy"]["common"] = common_filter_policy

    return cfs_options
