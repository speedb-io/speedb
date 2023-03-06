import json
import calc_utils
import display_utils


def get_json(parsed_log):
    j = dict()

    cf_names = parsed_log.get_cf_names()
    events_mngr = parsed_log.get_events_mngr()

    j["General"] = display_utils.prepare_db_wide_info_for_display(parsed_log)
    j["General"]["CF-s"] = \
        display_utils.prepare_general_cf_info_for_display(parsed_log)

    j["Options"] = {
        "Diff":
            display_utils.get_options_baseline_diff_for_display(parsed_log),
        "All Options": display_utils.get_all_options_for_display(parsed_log)
    }

    j["Warnings"] = display_utils.prepare_warnings_for_display(parsed_log)
    j["Events"] = calc_utils.calc_all_events_histogram(cf_names, events_mngr)
    j["Flushes"] = \
        display_utils.prepare_flushes_histogram_for_display(parsed_log)

    return j


def write_json(json_file_name, json_content):
    with open(json_file_name, 'w') as json_file:
        json.dump(json_content, json_file)
