import pytest
from ini_parser import IniParser


def test_trailing_comment():
    assert IniParser.remove_trailing_comment('') == ''
    assert IniParser.remove_trailing_comment('no comment') == 'no comment'
    assert IniParser.remove_trailing_comment('option1=100') == 'option1=100'
    assert IniParser.remove_trailing_comment('#option1=100') == ''
    assert IniParser.remove_trailing_comment('TEXT #comment TEXT1') == 'TEXT '
    assert IniParser.remove_trailing_comment('A B #T1 #T2 A') == 'A B '


def test_is_section_header():
    assert IniParser.is_section_header("[]")
    assert IniParser.is_section_header("[Version]")
    assert IniParser.is_section_header('[CFOptions "default"]')
    assert not IniParser.is_section_header("[abc")
    assert not IniParser.is_section_header("abc]")
    assert not IniParser.is_section_header("abc")


def test_get_section_name():
    assert IniParser.get_section_name('[CFOptions "default"]') == 'default'

    with pytest.raises(ValueError):
        IniParser.get_section_name('[Version]')

    with pytest.raises(ValueError):
        IniParser.get_section_name('max_open_files = -1')


def test_get_element():
    assert IniParser.get_element("") == IniParser.Element.comment
    assert IniParser.get_element("#") == IniParser.Element.comment
    assert IniParser.get_element("# abc def") == IniParser.Element.comment
    assert IniParser.get_element("#") == IniParser.Element.comment

    assert IniParser.get_element("a=1") == IniParser.Element.key_val
    assert IniParser.get_element("a=b") == IniParser.Element.key_val
    assert IniParser.get_element("a = 1") == IniParser.Element.key_val
    assert IniParser.get_element("a = b") == IniParser.Element.key_val
    assert IniParser.get_element("a=1:2:x") == IniParser.Element.key_val
    assert IniParser.get_element("abc = 100 = 200 # Comment") == \
           IniParser.Element.key_val

    with pytest.raises(ValueError):
        IniParser.get_element("abc #")

    with pytest.raises(ValueError):
        IniParser.get_element("abc + def #")


def test_get_key_value_pair():
    assert IniParser.get_key_value_pair("a") == ('a', None)
    assert IniParser.get_key_value_pair("a=1") == ('a', '1')
    assert IniParser.get_key_value_pair("a=X") == ('a', 'X')
    assert IniParser.get_key_value_pair("a=X Y") == ('a', 'X Y')
    assert IniParser.get_key_value_pair("") == ('', None)
    assert IniParser.get_key_value_pair("a=1:2:3") == ('a', ['1', '2', '3'])
    assert IniParser.get_key_value_pair("a=X2:20:R1") ==\
           ('a', ['X2', '20', 'R1'])


def test_get_list_from_value():
    assert IniParser.get_list_from_value('') == ['']
    assert IniParser.get_list_from_value('a') == ['a']
    assert IniParser.get_list_from_value('1:2') == ['1', '2']
    assert IniParser.get_list_from_value('a:bb:ccc') == ['a', 'bb', 'ccc']
