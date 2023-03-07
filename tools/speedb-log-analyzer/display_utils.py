import defs_and_utils
import calc_utils
import options_files_utils
from database_options import DatabaseOptions


def prepare_db_wide_user_opers_stats_for_display(db_wide_info):
    display_info = {}

    total_num_user_opers = db_wide_info["total_num_user_opers"]
    total_num_flushed_entries = db_wide_info['total_num_flushed_entries']

    if total_num_user_opers > 0:
        display_info['Writes'] = \
            f"{db_wide_info['percent_written']}" \
            f"({db_wide_info['num_written']}/{total_num_user_opers})"
        display_info['Reads'] = \
            f"{db_wide_info['percent_read']}" \
            f"({db_wide_info['num_read']}/{total_num_user_opers})"
        display_info['Seeks'] = \
            f"{db_wide_info['percent_seek']}" \
            f"({db_wide_info['num_seek']}/{total_num_user_opers})"
    else:
        display_info['Writes'] = "Not Available"
        display_info['Reads'] = "Not Available"
        display_info['Seeks'] = "Not Available"

    if total_num_flushed_entries > 0:
        display_info['Deletes'] = \
            f"{db_wide_info['total_percent_deletes']}" \
            f"({db_wide_info['total_num_deletes']}/" \
            f"{total_num_flushed_entries})"

    return display_info


def prepare_db_wide_info_for_display(parsed_log):
    display_info = {}

    db_wide_info = calc_utils.get_db_wide_info(parsed_log)
    cfs_options = calc_utils.get_applicable_cf_options(parsed_log)

    size_for_display = defs_and_utils.get_size_for_display

    display_info["Name"] = parsed_log.get_log_file_path()
    display_info["Version"] = f"{db_wide_info['version']} " \
                              f"[{db_wide_info['git_hash']}]"
    display_info["DB Size"] = size_for_display(db_wide_info['db_size_bytes'])
    display_info["Num Keys"] = f"{db_wide_info['num_written']:,}"
    display_info["Average Key Size"] = \
        size_for_display(db_wide_info['avg_key_size_bytes'])
    display_info["Average Value Size"] = \
        size_for_display(db_wide_info['avg_value_size_bytes'])
    display_info["Num Warnings"] = db_wide_info['num_warns']
    display_info["Num Errors"] = db_wide_info['num_errors']
    display_info["Num Stalls"] = db_wide_info['num_stalls']
    display_info["Compaction Style"] = \
        cfs_options['compaction_style']['common']
    display_info["Compression"] = cfs_options['compression']['common']
    display_info["Filter-Policy"] = cfs_options['filter_policy']['common']

    display_info.update(prepare_db_wide_user_opers_stats_for_display(
        db_wide_info))
    display_info['Num CF-s'] = db_wide_info['num_cfs']

    return display_info


def prepare_general_cf_info_for_display(parsed_log):
    display_info = {}

    cf_names = parsed_log.get_cf_names()
    events_mngr = parsed_log.get_events_mngr()

    size_for_display = defs_and_utils.get_size_for_display

    for i, cf_name in enumerate(cf_names):
        table_creation_stats = \
            calc_utils.calc_cf_table_creation_stats(cf_name, events_mngr)
        cf_options = calc_utils.get_applicable_cf_options(parsed_log)
        cf_size_bytes = calc_utils.get_cf_size_bytes(parsed_log, cf_name)

        display_info[cf_name] = {}
        cf_display_info = display_info[cf_name]

        cf_display_info["CF Size"] = size_for_display(cf_size_bytes)
        cf_display_info["Avg Key Size"] = \
            size_for_display(table_creation_stats['avg_key_size'])
        cf_display_info["Avg Value Size"] = \
            size_for_display(table_creation_stats['avg_value_size'])

        if cf_options['compaction_style']['common'] == 'Per Column Family':
            if cf_options['Compaction Style'][cf_name] is not None:
                cf_display_info["Compaction Style"] = \
                    cf_options['compaction_style'][cf_name]
            else:
                cf_display_info["Compaction Style"] = "UNKNOWN"

        if cf_options['compression']['common'] == 'Per Column Family':
            if cf_options['compression'][cf_name] is not None:
                cf_display_info["Compression"] = \
                    cf_options['compression'][cf_name]
            elif calc_utils.is_cf_compression_by_level(parsed_log, cf_name):
                cf_display_info["Compression"] = "Per-Level"
            else:
                cf_display_info["Compression"] = "UNKNOWN"

        if cf_options['filter_policy']['common'] == 'Per Column Family':
            if cf_options['filter_policy'][cf_name] is not None:
                cf_display_info["Filter-Policy"] = \
                    cf_options['filter_policy'][cf_name]
            else:
                cf_display_info["Filter-Policy"] = "UNKNOWN"

    return display_info


def prepare_warnings_for_display(parsed_log):
    warnings = parsed_log.get_warnings_mngr().get_all_warnings()
    display_warnings = {}
    for key in warnings.keys():
        if len(warnings[key]) == 0:
            continue

        display_warnings[key.value] = {}
        for warning_id in warnings[key]:
            warning_id_warnings = warnings[key][warning_id]
            num_warnings_of_id = len(warning_id_warnings)
            warning_id_key = f"{warning_id} ({num_warnings_of_id})"
            display_warnings[key.value][warning_id_key] = {}
            for warn_info in warning_id_warnings:
                temp = display_warnings[key.value][warning_id_key]
                temp[warn_info.warning_time] = \
                    f"[{warn_info.cf_name}] {warn_info.warning_msg}"
    return display_warnings if display_warnings else "No Warnings"


def get_all_options_for_display(parsed_log):
    options_per_cf = {}
    cf_names = parsed_log.get_cf_names()
    db_options = parsed_log.get_database_options()

    options_per_cf["DB-Wide"] = db_options.get_db_wide_options_for_display()
    for cf_name in cf_names:
        cf_options, cf_table_options = \
            db_options.get_cf_options_for_display(cf_name)
        options_per_cf[cf_name] = cf_options
        options_per_cf[cf_name]["Table-Options"] = cf_table_options

    return options_per_cf


def get_options_baseline_diff_for_display(parsed_log):
    metadata = parsed_log.get_metadata()

    options_diff, baseline_version = \
        options_files_utils.find_options_diff(
            defs_and_utils.OPTIONS_FILE_FOLDER,
            metadata.get_product_name(),
            metadata.get_version(),
            parsed_log.get_database_options())

    display_diff = {
        "Baseline": str(baseline_version),
        "DB-Wide": DatabaseOptions.get_db_wide_options_diff(options_diff)}

    for cf_name in parsed_log.get_cf_names():
        cf_options_diff = \
            DatabaseOptions.get_cf_options_diff(options_diff, cf_name)
        if cf_options_diff:
            if "CF-s" not in display_diff:
                display_diff["CF-s"] = {}
            display_diff["CF-s"][cf_name] = cf_options_diff

    return display_diff


def prepare_flushes_histogram_for_display(parsed_log):
    flushes_for_display = {}

    cf_names = parsed_log.get_cf_names()
    events_mngr = parsed_log.get_events_mngr()
    for cf_name in cf_names:
        cf_flushes_histogram = calc_utils.calc_flushes_histogram(cf_name,
                                                                 events_mngr)
        if cf_flushes_histogram:
            flushes_for_display[cf_name] = cf_flushes_histogram

    return flushes_for_display


def prepare_db_wide_stalls_entries_for_display(parsed_log):
    stalls_display_entries = {}

    mngr = parsed_log.get_stats_mngr().get_db_wide_stats_mngr()
    stalls_entries = mngr.get_stalls_entries()

    for entry_time, entry_stats in stalls_entries.items():
        stalls_display_entries[entry_time] = \
            {"Interval-Duration": str(entry_stats["interval_duration"]),
             "Interval-Percent": entry_stats["interval_percent"],
             "Cumulative-Duration": str(entry_stats["cumulative_duration"]),
             "Cumulative-Percent": entry_stats["cumulative_percent"]}

    return stalls_display_entries if stalls_display_entries \
        else "No (Non-Zero) Entries"


def prepare_cf_stalls_entries_for_display(parsed_log):
    mngr = parsed_log.get_stats_mngr().get_cf_no_file_stats_mngr()
    stall_counts = mngr.get_stall_counts()

    display_stall_counts = {}
    for cf_name in stall_counts.keys():
        if stall_counts[cf_name]:
            display_stall_counts[cf_name] = stall_counts[cf_name]

    return display_stall_counts if display_stall_counts \
        else "No (Non-Zero) Entries"
