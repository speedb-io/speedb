import re
import regexes
import defs_and_utils
import pathlib
import bisect
from options_file_parser import OptionsFileParser
from database_options import DatabaseOptions


def find_all_baseline_options_files(options_folder, product_name):
    if product_name == defs_and_utils.ProductName.ROCKSDB:
        options_regex = regexes.ROCKSDB_OPTIONS_FILE_REGEX
    elif product_name == defs_and_utils.ProductName.SPEEDB:
        options_regex = regexes.SPEEDB_OPTIONS_FILE_REGEX
    else:
        assert False

    options_folder = pathlib.Path(options_folder)

    files = []
    for file_name in options_folder.iterdir():
        file_match = re.findall(options_regex, file_name.name)
        if file_match:
            assert len(file_match) == 1
            files.append(defs_and_utils.OptionsFileInfo(
                file_name, defs_and_utils.Version(file_match[0])))

    files.sort()
    return files


def find_closest_version_idx(baseline_versions, version):
    if isinstance(version, str):
        version = defs_and_utils.Version(version)

    if baseline_versions[0] == version:
        return 0

    baseline_versions.sort()
    closest_version_idx = bisect.bisect_right(baseline_versions, version)

    if closest_version_idx:
        return closest_version_idx-1
    else:
        return None


def find_closest_baseline_options_file(options_folder, product_name, version):
    baseline_files = find_all_baseline_options_files(options_folder,
                                                     product_name)
    baseline_versions = [file.version for file in baseline_files]
    closest_version_idx = find_closest_version_idx(baseline_versions, version)
    if closest_version_idx is not None:
        return baseline_files[closest_version_idx].file_name,\
               baseline_files[closest_version_idx].version
    else:
        return None


def get_baseline_database_options(options_folder, product_name, version):
    closest_options_file, closest_version =\
        find_closest_baseline_options_file(options_folder,
                                           product_name,
                                           version)
    return OptionsFileParser.load_options_file(closest_options_file), \
        closest_version


def find_options_diff_relative_to_baseline(options_folder, product_name,
                                           version, database_options):
    baseline_database_options, closest_version =\
        get_baseline_database_options(options_folder, product_name, version)

    return \
        DatabaseOptions.get_options_diff(
            baseline_database_options.get_all_options(),
            database_options.get_all_options()), \
        closest_version
