# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import defs_and_utils
from ini_parser import IniParser
from database_options import DatabaseOptions


class OptionsSpecParser(IniParser):
    @staticmethod
    def is_new_option(line):
        return '=' in line

    @staticmethod
    def get_section_type(line):
        """
        Example section header: [TableOptions/BlockBasedTable "default"]
        Here ConfigurationOptimizer returned would be
        'TableOptions.BlockBasedTable'
        """
        section_path = line.strip()[1:-1].split()[0]
        section_type = '.'.join(section_path.split('/'))
        return section_type

    @staticmethod
    def get_section_name(line):
        # example: get_section_name('[CFOptions "default"]')
        token_list = line.strip()[1:-1].split('"')
        # token_list = ['CFOptions', 'default', '']
        if len(token_list) < 3:
            return None
        return token_list[1]  # return 'default'

    @staticmethod
    def get_section_str(section_type, section_name):
        # Example:
        # Case 1: get_section_str('DBOptions', defs_and_utils.NO_COL_FAMILY)
        # Case 2: get_section_str('TableOptions.BlockBasedTable', 'default')
        section_type = '/'.join(section_type.strip().split('.'))
        # Case 1: section_type = 'DBOptions'
        # Case 2: section_type = 'TableOptions/BlockBasedTable'
        section_str = '[' + section_type
        if section_name == defs_and_utils.NO_COL_FAMILY:
            # Case 1: '[DBOptions]'
            return section_str + ']'
        else:
            # Case 2: '[TableOptions/BlockBasedTable "default"]'
            return section_str + ' "' + section_name + '"]'


class OptionsFileParser:
    @staticmethod
    def load_options_file(options_path):
        options_dict = {}
        with open(options_path, 'r') as db_options:
            for line in db_options:
                line = OptionsSpecParser.remove_trailing_comment(line)
                if not line:
                    continue
                if OptionsSpecParser.is_section_header(line):
                    curr_sec_type = (
                        OptionsSpecParser.get_section_type(line)
                    )
                    curr_sec_name = OptionsSpecParser.get_section_name(line)
                    if curr_sec_type not in options_dict:
                        options_dict[curr_sec_type] = {}
                    if not curr_sec_name:
                        curr_sec_name = defs_and_utils.NO_COL_FAMILY
                    options_dict[curr_sec_type][curr_sec_name] = {}
                elif OptionsSpecParser.is_new_option(line):
                    key, value = OptionsSpecParser.get_key_value_pair(line)
                    options_dict[curr_sec_type][curr_sec_name][key] = (
                        value
                    )
                else:
                    error = 'Not able to parse line in Options file.'
                    OptionsSpecParser.exit_with_parse_error(line, error)

        return DatabaseOptions(options_dict)
