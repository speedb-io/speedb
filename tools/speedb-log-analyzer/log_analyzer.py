import os
import io
import sys
import argparse
import textwrap
import logging
import defs_and_utils
import json_output
import console_outputter
from log_file import ParsedLog


RAISE_EXCEPTION = 0


def parse_log(log_file_path):
    if not os.path.isfile(log_file_path):
        raise defs_and_utils.FileTypeParsingError(log_file_path)

    with open(log_file_path) as log_file:
        log_lines = log_file.readlines()
        log_lines = [line.strip() for line in log_lines]
        return ParsedLog(log_lines)


def setup_cmd_line_parser():
    epilog = textwrap.dedent('''\
    Notes:
    - The default it to print to the console in a short format
    - It is possible to specify both json and console outputs. Both will be 
       generated.
    ''') # noqa

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog)
    parser.add_argument("log",
                        metavar="log file path",
                        help="A path to a log file to parse")
    parser.add_argument("-c", "--console",
                        choices=["short", "full"],
                        help="Print to console a summary (short) or a "
                             "full output ")
    parser.add_argument("-j", "--json-file-name",
                        metavar="[json file name]",
                        help="Optional output json file name.")

    return parser


def validate_and_sanitize_cmd_line_args(cmdline_args):
    if not cmdline_args.console and not cmdline_args.json_file_name:
        cmdline_args.console = defs_and_utils.ConsoleOutputType.SHORT


def report_error(e):
    print(f"\nERROR:{e}\n", file=sys.stderr)


def handle_exception(e):
    if RAISE_EXCEPTION:
        raise e
    else:
        logging.critical(f"\n{e}", exc_info=True)
        exit(1)


def print_warnings_if_applicable():
    if len(defs_and_utils.g_parsing_warnings) > 0:
        print("Warnings:", file=sys.stderr)
        for i, warn_msg in enumerate(defs_and_utils.g_parsing_warnings):
            print(f"#{i}:\n{warn_msg}\n", file=sys.stderr)


def setup_logger():
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


if __name__ == '__main__':
    setup_logger()

    parser = setup_cmd_line_parser()
    cmdline_args = parser.parse_args()
    validate_and_sanitize_cmd_line_args(cmdline_args)

    e = None

    try:
        f = io.StringIO()
        log_file_path = cmdline_args.log

        parsed_log = parse_log(log_file_path)

        if cmdline_args.console:
            f.write(
                console_outputter.get_console_output(
                    log_file_path,
                    parsed_log,
                    cmdline_args.console))
            print(f"{f.getvalue()}\n")

        if cmdline_args.json_file_name:
            json_content = json_output.get_json(log_file_path, parsed_log)
            json_output.write_json(cmdline_args.json_file_name,
                                   json_content)
            print(f"JSON Output is in {cmdline_args.json_file_name}")

        print_warnings_if_applicable()

    except defs_and_utils.FileTypeParsingError as e:
        report_error(e)
    except FileNotFoundError as e:
        report_error(e)
    except ValueError as e:
        handle_exception(e)
    except Exception as e:  # noqa
        handle_exception(e)
