import io
import defs_and_utils
from log_file import ParsedLog
import display_utils


def get_title(log_file_path, parsed_log):
    metadata = parsed_log.get_metadata()
    log_time_span = metadata.get_log_time_span_seconds()
    title = f"{log_file_path} " \
            f"({log_time_span:.1f} Seconds) " \
            f"[{str(metadata.get_start_time())} - " \
            f"{str(metadata.get_end_time())}]"
    return title


def print_title(f, log_file_path, parsed_log):
    title = get_title(log_file_path, parsed_log)
    print(f"{title}", file=f)
    print(len(title) * "=", file=f)


def print_db_wide_user_opers_stats(db_wide_info, f, width):
    total_num_user_opers = db_wide_info["total_num_user_opers"]
    total_num_flushed_entries = db_wide_info['total_num_flushed_entries']
    total_num_deletes = db_wide_info['total_num_deletes']

    if total_num_user_opers > 0:
        print(f"{'Writes'.ljust(width)}: {db_wide_info['percent_written']}"
              f" ({db_wide_info['num_written']}/{total_num_user_opers})",
              file=f)
        print(f"{'Reads'.ljust(width)}: {db_wide_info['percent_read']} ("
              f"{db_wide_info['num_read']}/{total_num_user_opers})", file=f)
        print(f"{'Seeks'.ljust(width)}: {db_wide_info['percent_seek']} ("
              f"{db_wide_info['num_seek']}/{total_num_user_opers})", file=f)
    else:
        print(f"{'Writes'.ljust(width)}: Not Available", file=f)
        print(f"{'Reads'.ljust(width)}: Not Available", file=f)
        print(f"{'Seeks'.ljust(width)}: Not Available", file=f)

    if total_num_flushed_entries > 0:
        print(f"{'Deletes'.ljust(width)}: "
              f"{db_wide_info['total_percent_deletes']} "
              f"({total_num_deletes}/{total_num_flushed_entries})", file=f)
    else:
        print(f"{'Deletes'.ljust(width)}: Not Available", file=f)


def print_cf_console_printout(f, parsed_log):
    cf_names = parsed_log.get_cf_names()
    cf_width = max([len(cf_name) for cf_name in cf_names])

    cfs_info_for_display = \
        display_utils.prepare_general_cf_info_for_display(parsed_log)

    for i, cf_name in enumerate(cf_names):
        cf_info_for_display = cfs_info_for_display[cf_name]

        cf_title = f'#{i+1}' + ' ' + cf_name.ljust(cf_width)
        cf_line = f"{cf_title}: "
        cf_line += cf_info_for_display["CF Size"] + " "
        cf_line += "Key:" + cf_info_for_display["Avg Key Size"] + " "
        cf_line += "Value:" + cf_info_for_display["Avg Value Size"] + " "
        if "Compaction Style" in cf_info_for_display:
            cf_line += \
                "Compaction:" + cf_info_for_display["Compaction Style"] + " "
        if "Compression" in cf_info_for_display:
            cf_line += \
                "Compression:" + cf_info_for_display["Compression"] + " "
        if "Filter-Policy" in cf_info_for_display:
            cf_line += \
                "Filter-Policy:" + cf_info_for_display["Filter-Policy"] + " "

        print(cf_line, file=f)


def print_general_info(f, parsed_log: ParsedLog):
    display_general_info_dict = \
        display_utils.prepare_db_wide_info_for_display(parsed_log)

    width = 25
    for field_name, value in display_general_info_dict.items():
        print(f"{field_name.ljust(width)}: {value}", file=f)
    print_cf_console_printout(f, parsed_log)


def get_console_output(log_file_path, parsed_log, output_type):
    f = io.StringIO()

    if output_type == defs_and_utils.ConsoleOutputType.SHORT:
        print_title(f, log_file_path, parsed_log)
        print_general_info(f, parsed_log)

    return f.getvalue()
