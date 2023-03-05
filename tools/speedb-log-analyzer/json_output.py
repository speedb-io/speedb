import json
import calc_utils
import display_utils
import defs_and_utils


def print_cf_console_printout(f, parsed_log):
    cf_names = parsed_log.get_cf_names()
    events_mngr = parsed_log.get_events_mngr()
    cf_width = max([len(cf_name) for cf_name in cf_names])

    for i, cf_name in enumerate(cf_names):
        table_creation_stats = \
            calc_utils.calc_cf_table_creation_stats(cf_name, events_mngr)
        cf_options = calc_utils.get_applicable_cf_options(parsed_log)

        cf_title = f'#{i+1}' + ' ' + cf_name.ljust(cf_width)
        cf_size_bytes = calc_utils.get_cf_size_bytes(parsed_log, cf_name)
        key_size = \
            defs_and_utils.get_size_for_display(
                table_creation_stats['avg_key_size'])
        value_size = \
            defs_and_utils.get_size_for_display(
                table_creation_stats['avg_value_size'])

        cf_line = f"{cf_title}: "
        cf_line += f"{defs_and_utils.get_size_for_display(cf_size_bytes)}  "
        cf_line += f"Key:{key_size}  Value:{value_size}  "
        if cf_options['compaction_style']['common'] == 'Per Column Family':
            cf_line += \
                f"Compaction:{cf_options['compaction_style'][cf_name]}  "
        if cf_options['compression']['common'] == 'Per Column Family':
            cf_line += f"Compression:{cf_options['compression'][cf_name]}  "
        if cf_options['filter_policy']['common'] == 'Per Column Family':
            cf_line += \
                f"Filter-Policy:{cf_options['filter_policy'][cf_name]}  "

        print(cf_line, file=f)


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


def prepare_warnings_for_json(parsed_log):
    warnings = parsed_log.get_warnings_mngr().get_all_warnings()
    json_warnings = {}
    for key in warnings.keys():
        if len(warnings[key]) == 0:
            continue

        json_warnings[key.value] = {}
        for warning_id in warnings[key]:
            warning_id_warnings = warnings[key][warning_id]
            json_warnings[key.value][warning_id] = {}
            for warn_info in warning_id_warnings:
                temp = json_warnings[key.value][warning_id]
                temp[warn_info.warning_time] = \
                    f"[{warn_info.cf_name}] {warn_info.warning_msg}"
    return json_warnings


def get_json(parsed_log):
    j = dict()

    cf_names = parsed_log.get_cf_names()
    events_mngr = parsed_log.get_events_mngr()

    j["General"] = display_utils.prepare_db_wide_info_for_display(parsed_log)
    j["General"]["CF-s"] = \
        display_utils.prepare_general_cf_info_for_display(parsed_log)

    j["Options"] = get_options_per_cf(parsed_log)
    j["Warnings"] = prepare_warnings_for_json(parsed_log)
    j["Events"] = calc_utils.calc_all_events_histogram(cf_names, events_mngr)

    return j


def write_json(json_file_name, json_content):
    with open(json_file_name, 'w') as json_file:
        json.dump(json_content, json_file)
