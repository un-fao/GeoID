import pytest
from dynastore.modules.edr.temporal import parse_datetime_param


def test_parse_none_returns_none_pair():
    assert parse_datetime_param(None) == (None, None)
    assert parse_datetime_param("") == (None, None)


def test_parse_instant_returns_same_start_end():
    start, end = parse_datetime_param("2024-01-15T00:00:00Z")
    assert start == "2024-01-15T00:00:00Z"
    assert end == "2024-01-15T00:00:00Z"


def test_parse_closed_interval():
    start, end = parse_datetime_param("2024-01-01/2024-12-31")
    assert start == "2024-01-01"
    assert end == "2024-12-31"


def test_parse_open_start():
    start, end = parse_datetime_param("../2024-12-31")
    assert start is None
    assert end == "2024-12-31"


def test_parse_open_end():
    start, end = parse_datetime_param("2024-01-01/..")
    assert start == "2024-01-01"
    assert end is None
