import pytest

from runtools.runcore.util.text import parse_size_to_bytes


def test_plain_bytes():
    assert parse_size_to_bytes('1048576') == 1048576
    assert parse_size_to_bytes('0') == 0


def test_kilobytes():
    assert parse_size_to_bytes('512KB') == 512 * 1024
    assert parse_size_to_bytes('512kb') == 512 * 1024


def test_megabytes():
    assert parse_size_to_bytes('2MB') == 2 * 1024 * 1024
    assert parse_size_to_bytes('2mb') == 2 * 1024 * 1024


def test_gigabytes():
    assert parse_size_to_bytes('1GB') == 1024 ** 3


def test_fractional():
    assert parse_size_to_bytes('1.5MB') == int(1.5 * 1024 * 1024)


def test_whitespace_stripped():
    assert parse_size_to_bytes('  2MB  ') == 2 * 1024 * 1024


def test_invalid_format():
    with pytest.raises(ValueError):
        parse_size_to_bytes('abc')


def test_unknown_unit():
    with pytest.raises(ValueError):
        parse_size_to_bytes('2TB')


def test_empty_string():
    with pytest.raises(ValueError):
        parse_size_to_bytes('')
