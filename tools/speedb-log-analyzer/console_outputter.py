import io
import defs_and_utils
import calc_utils
from log_file import ParsedLog


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


def print_db_wide_info(f, parsed_log: ParsedLog):
    db_wide_info = calc_utils.get_db_wide_info(parsed_log)
    cfs_options = calc_utils.get_applicable_cf_options(parsed_log)

    width = 25
    print(f"{'Version'.ljust(width)}: "
          f"{db_wide_info['version']} [{db_wide_info['git_hash']}]", file=f)
    db_size_bytes = db_wide_info['db_size_bytes']
    print(f"{'DB Size'.ljust(width)}: "
          f"{defs_and_utils.get_size_for_display(db_size_bytes)}", file=f)
    print(f"{'Num Keys'.ljust(width)}: {db_wide_info['num_written']}", file=f)
    print(f"{'Average Key Size'.ljust(width)}: "
          f"{db_wide_info['avg_key_size_bytes']}",
          file=f)
    print(f"{'Average Value Size'.ljust(width)}:"
          f" {db_wide_info['avg_value_size_bytes']}", file=f)
    print(f"{'Num Warnings'.ljust(width)}: "
          f"{db_wide_info['num_warns']}", file=f)
    print(f"{'Num Errors'.ljust(width)}: {db_wide_info['num_errors']}", file=f)
    print(f"{'Num Stalls'.ljust(width)}: {db_wide_info['num_stalls']}", file=f)
    print(f"{'Compaction Style'.ljust(width)}: "
          f"{cfs_options['compaction_style']['common']}", file=f)
    print(f"{'Compression'.ljust(width)}: "
          f"{cfs_options['compression']['common']}", file=f)
    print(f"{'Filter-Policy'.ljust(width)}: "
          f"{cfs_options['filter_policy']['common']}", file=f)

    print_db_wide_user_opers_stats(db_wide_info, f, width)
    print(f"{'Num CF-s'.ljust(width)}: {db_wide_info['num_cfs']}", file=f)
    print_cf_console_printout(f, parsed_log)


def get_console_output(log_file_path, parsed_log, output_type):
    f = io.StringIO()

    if output_type == defs_and_utils.ConsoleOutputType.SHORT:
        print_title(f, log_file_path, parsed_log)
        print_db_wide_info(f, parsed_log)

    return f.getvalue()
