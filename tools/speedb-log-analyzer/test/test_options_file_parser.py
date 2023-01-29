# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import os
import pytest
import defs_and_utils
from database_options import DatabaseOptions
from options_file_parser import OptionsFileParser


@pytest.fixture(autouse=True)
def setup():
    this_path = os.path.abspath(os.path.dirname(__file__))
    options_path = os.path.join(this_path, 'input_files/OPTIONS-000005')
    misc_options = ['bloom_bits = 4', 'rate_limiter_bytes_per_sec = 1024000']

    # Load the options from the file and generate an options dict
    db_options = OptionsFileParser.load_options_file(options_path)
    db_options.setup_misc_options(misc_options)

    # perform clean-up before running tests
    generated_options = os.path.join(
        this_path, '../temp/OPTIONS_testing.tmp'
    )
    if os.path.isfile(generated_options):
        os.remove(generated_options)

    return db_options


def test_is_misc_option():
    assert DatabaseOptions.is_misc_option('bloom_bits')
    assert not DatabaseOptions.is_misc_option('DBOptions.stats_dump_freq_sec')


def test_set_up(setup):
    options = setup.get_all_options()
    assert 22 == len(options.keys())
    expected_misc_options = {
        'bloom_bits': '4', 'rate_limiter_bytes_per_sec': '1024000'
    }
    assert expected_misc_options == setup.get_misc_options()
    assert ['default', 'col_fam_A'], setup.get_column_families()


def test_get_options(setup):
    opt_to_get = [
        'DBOptions.manual_wal_flush', 'DBOptions.db_write_buffer_size',
        'bloom_bits', 'CFOptions.compaction_filter_factory',
        'CFOptions.num_levels', 'rate_limiter_bytes_per_sec',
        'TableOptions.BlockBasedTable.block_align', 'random_option'
    ]
    options = setup.get_options(opt_to_get)
    expected_options = {
        'DBOptions.manual_wal_flush': {defs_and_utils.NO_COL_FAMILY: 'false'},
        'DBOptions.db_write_buffer_size': {defs_and_utils.NO_COL_FAMILY: '0'},
        'bloom_bits': {defs_and_utils.NO_COL_FAMILY: '4'},
        'CFOptions.compaction_filter_factory': {
            'default': 'nullptr', 'col_fam_A': 'nullptr'
        },
        'CFOptions.num_levels': {'default': '7', 'col_fam_A': '5'},
        'rate_limiter_bytes_per_sec':
            {defs_and_utils.NO_COL_FAMILY: '1024000'},
        'TableOptions.BlockBasedTable.block_align': {
            'default': 'false', 'col_fam_A': 'true'
        }
    }
    assert expected_options == options


def test_get_column_families(setup):
    column_families = setup.get_column_families()
    column_families.sort()

    expected_column_families = ["default", "col_fam_A"]
    expected_column_families.sort()\

    assert column_families == expected_column_families
