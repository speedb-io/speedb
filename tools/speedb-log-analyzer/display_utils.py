import defs_and_utils
import calc_utils


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
            if cf_options['Compression'][cf_name] is not None:
                cf_display_info["Compression"] = \
                    cf_options['compression'][cf_name]
            else:
                cf_display_info["Compression"] = "UNKNOWN"

        if cf_options['filter_policy']['common'] == 'Per Column Family':
            if cf_options['filter_policy'][cf_name] is not None:
                cf_display_info["Filter-Policy"] = \
                    cf_options['filter_policy'][cf_name]
            else:
                cf_display_info["Filter-Policy"] = "UNKNOWN"

    return display_info
