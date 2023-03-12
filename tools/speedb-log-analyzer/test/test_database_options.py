import pytest
import defs_and_utils
from database_options import DatabaseOptions

default = 'default'
cf1 = 'cf1'
cf2 = 'cf2'
cf3 = 'cf3'


def test_empty_db_options():
    db_options = DatabaseOptions()
    assert db_options.get_all_options() == {}
    assert db_options.get_db_wide_options() == {}
    assert not db_options.are_db_wide_options_set()
    assert db_options.get_column_families() == []
    assert db_options.get_db_wide_options_for_display() == {}
    assert db_options.get_db_wide_option("manual_wal_flush") is None

    with pytest.raises(AssertionError):
        db_options.set_db_wide_option("manual_wal_flush", "1",
                                      allow_new_option=False)
    db_options.set_db_wide_option("manual_wal_flush", "1",
                                  allow_new_option=True)
    assert db_options.get_db_wide_option("manual_wal_flush") == "1"
    assert db_options.get_db_wide_option("Dummy-Options") is None
    assert db_options.get_options({"DBOptions.manual_wal_flush"}) == \
           {'DBOptions.manual_wal_flush': {"DB_WIDE": "1"}}
    assert db_options.get_options({"DBOptions.Dummy-Option"}) == {}

    assert db_options.get_cf_options(default) == {}
    assert db_options.get_cf_options_for_display(default) == ({}, {})
    assert db_options.get_cf_option(default, "write_buffer_size") is None
    with pytest.raises(AssertionError):
        db_options.set_cf_option(default, "write_buffer_size", "100",
                                 allow_new_option=False)
    db_options.set_cf_option(default, "write_buffer_size", "100",
                             allow_new_option=True)
    assert db_options.get_cf_option(default, "write_buffer_size") == "100"
    assert db_options.get_cf_option(default, "Dummmy-Options") is None
    assert db_options.get_cf_option("Dummy-CF", "write_buffer_size") is None
    assert db_options.get_options({"CFOptions.write_buffer_size"}) == \
           {'CFOptions.write_buffer_size': {default: '100'}}
    assert db_options.get_options({"CFOptions.write_buffer_size"}, default) \
           == {'CFOptions.write_buffer_size': {default: '100'}}
    assert db_options.get_options({"CFOptions.write_buffer_size"}, "Dummy-CF")\
           == {}
    assert db_options.get_options({"CFOptions.Dummy-Options"}, default) == {}

    assert db_options.get_cf_table_option(default, "write_buffer_size") is \
           None

    assert db_options.get_options("TableOptions.BlockBasedTable.block_align",
                                  cf1) == {}

    with pytest.raises(AssertionError):
        db_options.set_cf_table_option(cf1, "index_type", "3",
                                       allow_new_option=False)
    db_options.set_cf_table_option(cf1, "index_type", "3",
                                   allow_new_option=True)
    assert db_options.get_cf_table_option(cf1, "index_type") == "3"
    assert db_options.get_cf_table_option(cf1, "Dummy-Options") is None
    assert db_options.get_cf_table_option(default, "index_type") is None

    assert db_options.get_options({"TableOptions.BlockBasedTable.index_type"})\
           == {'TableOptions.BlockBasedTable.index_type': {cf1: '3'}}
    assert db_options.get_options({"TableOptions.BlockBasedTable.index_type"},
                                  cf1) == \
           {'TableOptions.BlockBasedTable.index_type': {cf1: '3'}}
    assert db_options.get_options({
        "TableOptions.BlockBasedTable.index_type"}, "Dummy-CF") == {}
    assert db_options.get_options({
        "TableOptions.BlockBasedTable.Dummy-Option"}) == {}


def test_set_db_wide_options():
    input_dict = {"manual_wal_flush": "false",
                  "db_write_buffer_size": "0"}
    expected_options = {
        'DBOptions.manual_wal_flush': {defs_and_utils.NO_COL_FAMILY: 'false'},
        'DBOptions.db_write_buffer_size': {defs_and_utils.NO_COL_FAMILY: '0'},
    }

    db_options = DatabaseOptions()
    db_options.set_db_wide_options(input_dict)

    assert db_options.get_all_options() == expected_options
    assert db_options.get_db_wide_options() == expected_options
    assert db_options.get_db_wide_option("manual_wal_flush") == 'false'
    assert db_options.get_db_wide_option("manual_wal_flush-X") is None
    assert db_options.get_column_families() == []

    db_options.set_db_wide_option('manual_wal_flush', 'true',
                                  allow_new_option=False)
    assert db_options.get_db_wide_option("manual_wal_flush") == 'true'

    db_options.set_db_wide_option('manual_wal_flush', 'false',
                                  allow_new_option=False)
    assert db_options.get_db_wide_option("manual_wal_flush") == 'false'

    assert db_options.get_db_wide_option("DUMMY") is None
    with pytest.raises(AssertionError):
        db_options.set_db_wide_option('DUMMY', 'XXX', allow_new_option=False)

    db_options.set_db_wide_option('DUMMY', 'XXX', True)
    assert db_options.get_db_wide_option("DUMMY") == "XXX"

    expected_db_wide_display_options = {'manual_wal_flush': 'false',
                                        'db_write_buffer_size': '0',
                                        'DUMMY': 'XXX'}
    assert db_options.get_db_wide_options_for_display() == \
           expected_db_wide_display_options


def test_set_cf_options():
    default_input_dict = {"write_buffer_size": "1000"}
    default_table_input_dict = {"index_type": "1"}

    cf1_input_dict = {"write_buffer_size": "2000"}
    cf1_table_input_dict = {"index_type": "2"}

    cf2_input_dict = {"write_buffer_size": "3000"}
    cf2_table_input_dict = {"index_type": "3"}

    db_options = DatabaseOptions()

    expected_options = dict()
    expected_options['CFOptions.write_buffer_size'] = {default: '1000'}
    expected_options['TableOptions.BlockBasedTable.index_type'] = \
        {default: '1'}
    db_options.set_cf_options(default, default_input_dict,
                              default_table_input_dict)
    assert db_options.get_all_options() == expected_options
    assert db_options.get_column_families() == [default]

    expected_options['CFOptions.write_buffer_size'][cf1] = "2000"
    expected_options['TableOptions.BlockBasedTable.index_type'][cf1] = "2"
    db_options.set_cf_options(cf1, cf1_input_dict, cf1_table_input_dict)
    assert db_options.get_all_options() == expected_options
    assert db_options.get_column_families() == [default, cf1]

    expected_options['CFOptions.write_buffer_size'][cf2] = "3000"
    expected_options['TableOptions.BlockBasedTable.index_type'][cf2] = "3"
    db_options.set_cf_options(cf2, cf2_input_dict, cf2_table_input_dict)
    assert db_options.get_all_options() == expected_options
    assert db_options.get_column_families() == [default, cf1, cf2]

    expected_dict_default = \
        {'CFOptions.write_buffer_size': {default: '1000'},
         'TableOptions.BlockBasedTable.index_type': {default: '1'}}

    expected_dict_cf1 = \
        {'CFOptions.write_buffer_size': {cf1: '2000'},
         'TableOptions.BlockBasedTable.index_type': {cf1: '2'}}

    expected_dict_cf2 = \
        {'CFOptions.write_buffer_size': {cf2: '3000'},
         'TableOptions.BlockBasedTable.index_type': {cf2: '3'}}

    assert db_options.get_db_wide_options() == {}
    assert expected_dict_default == db_options.get_cf_options(default)
    assert expected_dict_cf1 == db_options.get_cf_options(cf1)
    assert expected_dict_cf2 == db_options.get_cf_options(cf2)
    assert db_options.get_cf_option(default, "write_buffer_size") == '1000'
    assert db_options.get_cf_table_option(default, "index_type", ) == '1'
    assert db_options.get_cf_option(cf1, "write_buffer_size") == '2000'
    assert db_options.get_cf_table_option(cf1, "index_type", ) == '2'
    db_options.set_cf_table_option(cf1, "index_type", '100')
    assert db_options.get_cf_table_option(cf1, "index_type", ) == '100'
    assert db_options.get_cf_option(cf2, "write_buffer_size") == '3000'
    assert db_options.get_cf_table_option(cf2, "index_type") == '3'
    db_options.set_cf_table_option(cf2, "index_type", 'XXXX')
    assert db_options.get_cf_table_option(cf2, "index_type") == 'XXXX'

    assert db_options.get_cf_option(default, "index_type") is None
    db_options.set_cf_option(default, "index_type", "200", True)
    assert db_options.get_cf_option(default, "index_type") == "200"
    assert db_options.get_cf_table_option(cf1, "write_buffer_size") is None

    expected_default_display_options = {'write_buffer_size': '1000',
                                        'index_type': '200'}
    expected_default_display_table_options = {'index_type': '1'}

    expected_cf1_display_options = {'write_buffer_size': '2000'}
    expected_cf1_display_table_options = {'index_type': '100'}

    expected_cf2_display_options = {'write_buffer_size': '3000'}
    expected_cf2_display_table_options = {'index_type': 'XXXX'}

    assert db_options.get_cf_options_for_display(default) == \
           (expected_default_display_options,
            expected_default_display_table_options)

    assert db_options.get_cf_options_for_display(cf1) == \
           (expected_cf1_display_options,
            expected_cf1_display_table_options)

    assert db_options.get_cf_options_for_display(cf2) == \
           (expected_cf2_display_options,
            expected_cf2_display_table_options)


def test_get_options_diff():
    old_opt = {
        'DBOptions.stats_dump_freq_sec': {defs_and_utils.NO_COL_FAMILY: '20'},
        'CFOptions.write_buffer_size': {
            default: '1024000',
            cf1: '128000',
            cf2: '128000000'
        },

        'TableOptions.BlockBasedTable.index_type': {cf2: '1'},
        'TableOptions.BlockBasedTable.format_version': {cf1: '4', cf2: '5'},
        'TableOptions.BlockBasedTable.block_size': {cf1: '4096', cf2: '4096'},

        'DBOptions.use_fsync': {defs_and_utils.NO_COL_FAMILY: 'true'},
        'DBOptions.max_log_file_size':
            {defs_and_utils.NO_COL_FAMILY: '128000000'}
    }
    new_opt = {
        'bloom_bits': {defs_and_utils.NO_COL_FAMILY: '4'},
        'CFOptions.write_buffer_size': {
            default: '128000000',
            cf1: '128000',
            cf3: '128000000'
        },
        'TableOptions.BlockBasedTable.checksum': {cf1: 'true'},
        'TableOptions.BlockBasedTable.format_version': {cf1: '5', cf2: '5'},
        'TableOptions.BlockBasedTable.block_size': {cf1: '4096', cf2: '4096'},
        'DBOptions.use_fsync': {defs_and_utils.NO_COL_FAMILY: 'true'},
        'DBOptions.max_log_file_size': {defs_and_utils.NO_COL_FAMILY: '0'}
    }
    expected_diff = {
        'DBOptions.stats_dump_freq_sec':
            {defs_and_utils.NO_COL_FAMILY: ('20', None)},
        'bloom_bits': {defs_and_utils.NO_COL_FAMILY: (None, '4')},
        'CFOptions.write_buffer_size': {
            default: ('1024000', '128000000'),
            cf2: ('128000000', None),
            cf3: (None, '128000000')},
        'TableOptions.BlockBasedTable.index_type': {'cf2': ('1', None)},
        'TableOptions.BlockBasedTable.checksum': {cf1: (None, 'true')},
        'TableOptions.BlockBasedTable.format_version': {'cf1': ('4', '5')},
        'DBOptions.max_log_file_size':
            {defs_and_utils.NO_COL_FAMILY: ('128000000', '0')}
    }

    diff = DatabaseOptions.get_options_diff(old_opt, new_opt)

    assert diff == expected_diff


def test_get_cfs_options_diff():
    old_opt = {
        'DBOptions.stats_dump_freq_sec': {defs_and_utils.NO_COL_FAMILY: '20'},
        'CFOptions.write_buffer_size': {
            default: '1024000',
            cf1: '128000',
            cf2: '128000000'
        },
        'TableOptions.BlockBasedTable.index_type': {cf2: '1'},
        'TableOptions.BlockBasedTable.format_version': {cf1: '4', cf2: '5'},
        'TableOptions.BlockBasedTable.block_size': {cf1: '4096', cf2: '4096'},
        'DBOptions.use_fsync': {defs_and_utils.NO_COL_FAMILY: 'true'},
        'DBOptions.max_log_file_size':
            {defs_and_utils.NO_COL_FAMILY: '128000000'}
    }
    new_opt = {
        'bloom_bits': {defs_and_utils.NO_COL_FAMILY: '4'},
        'CFOptions.write_buffer_size': {
            default: '128000000',
            cf1: '128000',
            cf3: '128000000'
        },
        'TableOptions.BlockBasedTable.checksum': {cf1: 'true'},
        'TableOptions.BlockBasedTable.format_version': {cf1: '5', cf2: '5'},
        'TableOptions.BlockBasedTable.block_size': {cf1: '4096', cf2: '4096'},
        'DBOptions.use_fsync': {defs_and_utils.NO_COL_FAMILY: 'true'},
        'DBOptions.max_log_file_size': {defs_and_utils.NO_COL_FAMILY: '0'}
    }

    assert not DatabaseOptions.get_cfs_options_diff(
        old_opt, defs_and_utils.NO_COL_FAMILY,
        old_opt, defs_and_utils.NO_COL_FAMILY)
    assert not DatabaseOptions.get_cfs_options_diff(old_opt, default,
                                                    old_opt, default)
    assert not DatabaseOptions.get_cfs_options_diff(old_opt, cf1,
                                                    old_opt, cf1)
    assert not DatabaseOptions.get_cfs_options_diff(old_opt, cf2,
                                                    old_opt, cf2)
    assert not DatabaseOptions.get_cfs_options_diff(old_opt, cf3,
                                                    old_opt, cf3)

    assert not DatabaseOptions.get_cfs_options_diff(
        new_opt, defs_and_utils.NO_COL_FAMILY,
        new_opt, defs_and_utils.NO_COL_FAMILY)
    assert not DatabaseOptions.get_cfs_options_diff(new_opt, default,
                                                    new_opt, default)
    assert not DatabaseOptions.get_cfs_options_diff(new_opt, cf1,
                                                    new_opt, cf1)
    assert not DatabaseOptions.get_cfs_options_diff(new_opt, cf2,
                                                    new_opt, cf2)
    assert not DatabaseOptions.get_cfs_options_diff(new_opt, cf3,
                                                    new_opt, cf3)

    expected_diff = {
        'cf names': (default, default),
        'CFOptions.write_buffer_size': ('1024000', '128000000'),
    }

    assert DatabaseOptions.get_cfs_options_diff(old_opt, default,
                                                new_opt, default) == \
           expected_diff

    expected_diff = {
        'cf names': (default, cf1),
        'CFOptions.write_buffer_size': ('1024000', '128000'),
        'TableOptions.BlockBasedTable.checksum': (None, 'true'),
        'TableOptions.BlockBasedTable.format_version': (None, '5'),
        'TableOptions.BlockBasedTable.block_size': (None, '4096')
    }
    assert DatabaseOptions.get_cfs_options_diff(old_opt, default,
                                                new_opt, cf1) == \
           expected_diff

    expected_diff = {
        'cf names': (cf1, default),
        'CFOptions.write_buffer_size': ('128000', '1024000'),
        'TableOptions.BlockBasedTable.checksum': ('true', None),
        'TableOptions.BlockBasedTable.format_version': ('5', None),
        'TableOptions.BlockBasedTable.block_size': ('4096', None)
    }
    assert DatabaseOptions.get_cfs_options_diff(new_opt, cf1,
                                                old_opt, default) == \
           expected_diff

    expected_diff = {
        'cf names': (cf2, cf2),
        'CFOptions.write_buffer_size': ('128000000', None),
        'TableOptions.BlockBasedTable.index_type': ('1', None),

    }
    assert DatabaseOptions.get_cfs_options_diff(old_opt, cf2,
                                                new_opt, cf2) == \
           expected_diff

    expected_diff = {
        'cf names': (cf3, cf3),
        'CFOptions.write_buffer_size': (None, '128000000')}
    assert DatabaseOptions.get_cfs_options_diff(old_opt, cf3,
                                                new_opt, cf3) == \
           expected_diff


def test_extract_db_wide_diff_from_options_diff():
    assert DatabaseOptions.extract_db_wide_diff_from_options_diff({}) == {}

    option_diff_without_db_wide = {
        'bloom_bits': {defs_and_utils.NO_COL_FAMILY: '4'},
        'CFOptions.write_buffer_size': {
            default: '128000000',
            cf1: '128000',
            cf3: '128000000'
        },
        'TableOptions.BlockBasedTable.index_type': {cf1: '2'},
    }
    assert DatabaseOptions.extract_db_wide_diff_from_options_diff(
        option_diff_without_db_wide) == {}

    option_diff_with_db_wide = {
        'bloom_bits': {defs_and_utils.NO_COL_FAMILY: '4'},
        'CFOptions.write_buffer_size': {
            default: '128000000',
            cf1: '128000',
            cf3: '128000000'
        },
        'TableOptions.BlockBasedTable.index_type': {cf1: '2'},
        'DBOptions.max_log_file_size':
            {defs_and_utils.NO_COL_FAMILY: ('128000000', '0')},
    }
    expected_db_wide_diff = {'max_log_file_size': {'Base': '128000000',
                                                   'Log': '0'}}
    assert DatabaseOptions.extract_db_wide_diff_from_options_diff(
        option_diff_with_db_wide) == expected_db_wide_diff


def test_extract_cfs_diff_from_options_diff():
    option_diff_no_cfs = {
        'cf names': (cf1, cf2),
    }
    assert DatabaseOptions.extract_cfs_diff_from_options_diff(
        option_diff_no_cfs, cf1, cf2) == ({}, {})

    option_diff_with_cfs_no_table_diff = {
        'cf names': (default, default),
        'CFOptions.write_buffer_size': ('128000000', '10000')
    }

    assert DatabaseOptions.extract_cfs_diff_from_options_diff(
        option_diff_with_cfs_no_table_diff, default, default) == (
               {'write_buffer_size': ('128000000', '10000')}, {})

    option_diff_no_cfs_with_table_diff = {
        'cf names': (cf2, default),
        'TableOptions.BlockBasedTable.index_type': ('1', None),
        'TableOptions.BlockBasedTable.checksum': (None, 'true'),
        'TableOptions.BlockBasedTable.format_version': ('4', '5'),
    }

    assert DatabaseOptions.extract_cfs_diff_from_options_diff(
        option_diff_no_cfs_with_table_diff, cf2, default) ==\
        ({}, {'index_type': ('1', None),
              'checksum': (None, 'true'),
              'format_version': ('4', '5')})

    option_diff_with_cfs_and_with_table_diff = {
        'cf names': (cf2, default),
        'TableOptions.BlockBasedTable.index_type': ('1', None),
        'TableOptions.BlockBasedTable.checksum': (None, 'true'),
        'TableOptions.BlockBasedTable.format_version': ('4', '5'),
        'CFOptions.write_buffer_size': ('128000000', '10000')
    }

    assert DatabaseOptions.extract_cfs_diff_from_options_diff(
        option_diff_with_cfs_and_with_table_diff, cf2, default) ==\
        ({'write_buffer_size': ('128000000', '10000')},
         {'index_type': ('1', None),
          'checksum': (None, 'true'),
          'format_version': ('4', '5')})
