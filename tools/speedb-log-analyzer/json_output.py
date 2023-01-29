import json
import defs_and_utils
import calc_utils


def get_options_per_cf(parsed_log):
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


def prepare_db_wide_info_for_display(log_file_path, parsed_log):
    display_info = {}

    db_wide_info = calc_utils.get_db_wide_info(parsed_log)
    cfs_options = calc_utils.get_applicable_cf_options(parsed_log)

    size_for_display = defs_and_utils.get_size_for_display

    db_wide_info["name"] = log_file_path
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
    return display_info


def get_json(log_file_path, parsed_log):
    j = dict()

    cf_names = parsed_log.get_cf_names()
    events_mngr = parsed_log.get_events_mngr()

    j["General"] = prepare_db_wide_info_for_display(log_file_path, parsed_log)

    j["Options"] = get_options_per_cf(parsed_log)
    j["Warnings"] = parsed_log.get_warnings_mngr().get_all_warnings()
    j["Events"] = calc_utils.calc_all_events_histogram(cf_names, events_mngr)

    return j


def write_json(json_file_name, json_content):
    with open(json_file_name, 'w') as json_file:
        json.dump(json_content, json_file)
