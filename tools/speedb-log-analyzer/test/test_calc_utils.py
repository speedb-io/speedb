import calc_utils
import test.test_utils as test_utils
import defs_and_utils


def test_get_db_size_bytes():
    parsed_log = test_utils.create_parsed_log()

    actual_size_bytes = calc_utils.get_db_size_bytes(parsed_log)
    assert actual_size_bytes == defs_and_utils.get_value_by_unit("82.43", "GB")
